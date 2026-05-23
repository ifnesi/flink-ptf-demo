"""Kafka producer + two background consumers exposed as pub/sub queues for SSE."""

from __future__ import annotations

import os
import time
import queue
import logging
import threading

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
    StringSerializer,
)

log = logging.getLogger(__name__)


# --- Avro schemas (single source of truth in terraform/schemas/) ------------

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "terraform" / "schemas"
CLICK_VALUE_SCHEMA = (SCHEMAS_DIR / "user-clicks-value.avsc").read_text()
WATERMARK_USER = "_wma"


@dataclass
class Settings:
    bootstrap: str
    kafka_key: str
    kafka_secret: str
    sr_url: str
    sr_key: str
    sr_secret: str
    clicks_topic: str
    summaries_topic: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            bootstrap=os.environ["BOOTSTRAP_SERVERS"],
            kafka_key=os.environ["KAFKA_API_KEY"],
            kafka_secret=os.environ["KAFKA_API_SECRET"],
            sr_url=os.environ["SR_URL"],
            sr_key=os.environ["SR_API_KEY"],
            sr_secret=os.environ["SR_API_SECRET"],
            clicks_topic=os.environ.get("CLICKS_TOPIC", "user-clicks"),
            summaries_topic=os.environ.get("SUMMARIES_TOPIC", "user-clicks-summary"),
        )


# --- Pub/sub registry: one queue per SSE subscriber -------------------------


class PubSub:
    """Fan-out registry. Each subscriber gets its own bounded queue."""

    def __init__(
        self,
        maxsize: int = 100,
    ) -> None:
        self._subscribers: list[queue.Queue] = []
        self._lock = threading.Lock()
        self._maxsize = maxsize

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=self._maxsize)
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(
        self,
        q: queue.Queue,
    ) -> None:
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def publish(
        self,
        item: Any,
    ) -> None:
        with self._lock:
            subs = list(self._subscribers)
        for q in subs:
            try:
                q.put_nowait(item)
            except queue.Full:
                # Drop for slow subscribers — the UI is best-effort.
                pass


# --- KafkaIO: producer + two consumer threads -------------------------------


class KafkaIO:
    def __init__(
        self,
        settings: Settings,
    ) -> None:
        self.s = settings
        self.clicks_pubsub = PubSub()
        self.summaries_pubsub = PubSub()

        self._sr = SchemaRegistryClient(
            {
                "url": settings.sr_url,
                "basic.auth.user.info": f"{settings.sr_key}:{settings.sr_secret}",
            }
        )

        self._key_str_ser = StringSerializer("utf_8")
        self._key_str_de = StringDeserializer("utf_8")
        self._click_value_ser = AvroSerializer(self._sr, CLICK_VALUE_SCHEMA)
        # Summaries come from Flink — pull the schema by subject for the deserializer.
        self._click_value_de = AvroDeserializer(self._sr)
        self._summary_value_de = AvroDeserializer(self._sr)

        self._producer = Producer(self._kafka_conf())
        self._stopping = threading.Event()
        self._threads: list[threading.Thread] = []

    # -- producer side --

    def _kafka_conf(self) -> dict:
        return {
            "bootstrap.servers": self.s.bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.s.kafka_key,
            "sasl.password": self.s.kafka_secret,
            "client.id": "flink-ptf-demo-backend",
        }

    def produce_click(
        self,
        user: str,
        product_id: str,
        product_name: str,
        click_ts_ms: int,
    ) -> None:
        value = {
            "user_id": user,
            "product_id": product_id,
            "product_name": product_name,
            "click_ts": click_ts_ms,
        }
        topic = self.s.clicks_topic
        self._producer.produce(
            topic=topic,
            key=self._key_str_ser(
                user,
                SerializationContext(
                    topic,
                    MessageField.KEY,
                ),
            ),
            value=self._click_value_ser(
                value,
                SerializationContext(
                    topic,
                    MessageField.VALUE,
                ),
            ),
            on_delivery=_log_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 5.0) -> int:
        return self._producer.flush(timeout)

    # -- consumer side --

    def start_consumers(self) -> None:
        self._spawn_consumer(
            topic=self.s.clicks_topic,
            group_id=f"demo-clicks-ui-{os.getpid()}",
            value_de=self._click_value_de,
            pubsub=self.clicks_pubsub,
            decoder=self._decode_click,
        )
        self._spawn_consumer(
            topic=self.s.summaries_topic,
            group_id=f"demo-summaries-ui-{os.getpid()}",
            value_de=self._summary_value_de,
            pubsub=self.summaries_pubsub,
            decoder=self._decode_summary,
        )
        self._spawn_watermark_thread()

    def stop(self) -> None:
        self._stopping.set()
        for t in self._threads:
            t.join(timeout=5)

    def _spawn_consumer(
        self,
        topic: str,
        group_id: str,
        value_de: AvroDeserializer,
        pubsub: PubSub,
        decoder: Callable[[Any, Any], dict],
    ) -> None:
        def _run() -> None:
            conf = self._kafka_conf() | {
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
                "isolation.level": "read_uncommitted",
            }
            consumer = Consumer(conf)
            consumer.subscribe([topic])
            log.info("Consumer started: topic=%s group=%s", topic, group_id)
            try:
                while not self._stopping.is_set():
                    msg = consumer.poll(0.5)
                    if msg is None:
                        continue
                    if msg.error():
                        log.warning("Consumer error on %s: %s", topic, msg.error())
                        continue
                    try:
                        key = self._key_str_de(
                            msg.key(),
                            SerializationContext(
                                topic,
                                MessageField.KEY,
                            ),
                        )
                        value = value_de(
                            msg.value(),
                            SerializationContext(
                                topic,
                                MessageField.VALUE,
                            ),
                        )
                        pubsub.publish(decoder(key, value))
                    except Exception as exc:
                        log.exception(
                            "Failed to decode message from %s: %s", topic, exc
                        )
            finally:
                consumer.close()
                log.info("Consumer stopped: topic=%s", topic)

        t = threading.Thread(target=_run, daemon=True, name=f"consumer-{topic}")
        t.start()
        self._threads.append(t)

    def _spawn_watermark_thread(self) -> None:
        """Spawn a background thread that publishes watermark advancement messages every second."""

        def _run() -> None:
            log.info("Watermark advancement thread started")
            try:
                while not self._stopping.is_set():
                    value = {
                        "user_id": WATERMARK_USER,
                        "product_id": "0",
                        "product_name": "watermark",
                        "click_ts": int(time.time() * 1000),
                    }
                    topic = self.s.clicks_topic
                    try:
                        self._producer.produce(
                            topic=topic,
                            key=self._key_str_ser(
                                WATERMARK_USER,
                                SerializationContext(
                                    topic,
                                    MessageField.KEY,
                                ),
                            ),
                            value=self._click_value_ser(
                                value,
                                SerializationContext(
                                    topic,
                                    MessageField.VALUE,
                                ),
                            ),
                            on_delivery=_log_delivery,
                        )
                        self._producer.poll(0)
                    except Exception as exc:
                        log.warning("Failed to produce watermark message: %s", exc)

                    # Wait second or until stopping
                    self._stopping.wait(1.0)
            finally:
                log.info("Watermark advancement thread stopped")

        t = threading.Thread(
            target=_run,
            daemon=True,
            name="watermark-advancement",
        )
        t.start()
        self._threads.append(t)

    # -- decoders normalize records for the UI --

    @staticmethod
    def _decode_click(key: str | None, value: dict) -> dict:
        return {
            "type": "click",
            "user": key,
            "user_id": value.get("user_id"),
            "product_id": value.get("product_id"),
            "product_name": value.get("product_name"),
            "click_ts": value.get("click_ts"),
        }

    @staticmethod
    def _decode_summary(key: str | None, value: dict) -> dict:
        # The Flink sink writes the user_id as raw UTF-8 bytes into the key.
        # AvroDeserializer falls through for raw bytes; StringDeserializer handles it.
        return {
            "type": "summary",
            "user": key,
            "detected_at": value.get("detected_at"),
            "clicks_summary": value.get("clicks_summary", ""),
        }


def _log_delivery(err, msg) -> None:
    if err is not None:
        log.error("Produce failed: %s", err)
    else:
        log.debug(
            "Produced to %s [%d] @ offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )
