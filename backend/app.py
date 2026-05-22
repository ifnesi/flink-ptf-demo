"""Flask app for the Flink PTF demo.

Endpoints:
  POST /login                 — accept a username (no auth, demo only)
  POST /click                 — produce a click event to user-clicks
  GET  /stream/clicks         — SSE; live messages from user-clicks
  GET  /stream/summaries      — SSE; live messages from user-clicks-summary
  GET  /health                — basic health check
"""

from __future__ import annotations

import json
import time
import queue
import logging

from pathlib import Path
from typing import Any, Iterator
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, request, send_from_directory, stream_with_context
from dotenv import load_dotenv

from kafka_io import KafkaIO, PubSub, Settings

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger("flink-ptf-demo")

settings = Settings.from_env()
kio = KafkaIO(settings)
kio.start_consumers()

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

app = Flask(
    __name__,
    static_folder=str(FRONTEND_DIR),
    static_url_path="",
)


@app.get("/")
def index() -> Response:
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.get("/health")
def health() -> Response:
    return jsonify({"ok": True})


@app.post("/login")
def login() -> Response:
    body = request.get_json(force=True, silent=True) or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    return jsonify({"user": name})


@app.post("/click")
def click() -> Response:
    body = request.get_json(force=True, silent=True) or {}
    user = (body.get("user") or "").strip()
    product_id = (body.get("product_id") or "").strip()
    product_name = (body.get("product_name") or "").strip()
    if not (user and product_id and product_name):
        return jsonify({"error": "user, product_id, product_name are required"}), 400

    ts_ms = int(time.time() * 1000)
    kio.produce_click(user, product_id, product_name, ts_ms)
    return jsonify({"ok": True, "click_ts": ts_ms})


def _json_default(o: Any) -> Any:
    # Avro `timestamp-millis` decodes to a tz-aware datetime; the UI wants epoch ms.
    if isinstance(o, datetime):
        if o.tzinfo is None:
            o = o.replace(tzinfo=timezone.utc)
        return int(o.timestamp() * 1000)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def _sse_stream(pubsub: PubSub) -> Iterator[bytes]:
    q = pubsub.subscribe()
    try:
        # Send a comment line immediately so the client transitions to OPEN.
        yield b": connected\n\n"
        while True:
            try:
                event = q.get(timeout=15)
            except queue.Empty:
                # Heartbeat keeps proxies and EventSource alive.
                yield b": keepalive\n\n"
                continue
            payload = json.dumps(event, separators=(",", ":"), default=_json_default)
            yield f"data: {payload}\n\n".encode()
    finally:
        pubsub.unsubscribe(q)


def _sse_response(pubsub: PubSub) -> Response:
    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",  # disable proxy buffering
        "Connection": "keep-alive",
    }
    return Response(
        stream_with_context(_sse_stream(pubsub)),
        mimetype="text/event-stream",
        headers=headers,
    )


@app.get("/stream/clicks")
def stream_clicks() -> Response:
    return _sse_response(kio.clicks_pubsub)


@app.get("/stream/summaries")
def stream_summaries() -> Response:
    return _sse_response(kio.summaries_pubsub)


if __name__ == "__main__":
    # `flask --app app run -p 5000` is preferred; this is only for direct python execution.
    app.run(host="127.0.0.1", port=5001, threaded=True)
