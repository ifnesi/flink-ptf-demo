# Flink PTF Demo — Click Inactivity Detection on Confluent Cloud

A self-contained web demo of **Confluent Cloud for Apache Flink Process Table Functions (PTFs)**.

A user logs in, clicks product tiles, and watches in real time:
1. Click events being produced to Kafka (`user-clicks` topic, Avro).
2. The PTF-generated **inactivity summary** (`user-clicks-summary` topic, Avro), emitted **10 seconds after the user's last click**, with an aggregated per-product click count.

All Confluent Cloud resources (environment, Kafka Basic cluster in AWS `us-east-1`, Flink compute pool, service account, RBAC, API keys, topics, schemas, the PTF artifact upload, and `CREATE FUNCTION` + `INSERT INTO` Flink statements) are provisioned via **Terraform**.

## Architecture

```
React UI  ─click→  Flask API  ─Avro produce→  Kafka: user-clicks
   ▲                  │                              │
   │                  ▼                              ▼
   │           SSE consumer            Flink PTF (per-user state,
   │           (2 topics)              10s event-time inactivity timer)
   │                  ▲                              │
   └──── SSE stream ──┘   ←─Avro consume─  Kafka: user-clicks-summary
```

## Prerequisites

- A [Confluent Cloud](https://confluent.cloud/signup) account with a [**Cloud API key/secret**](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/manage-api-keys.html#add-an-api-key) (organization-level, not Kafka-level)
- [Terraform](https://developer.hashicorp.com/terraform/install) `>= 1.5`
- [Java](https://adoptium.net/) `11+` and [Maven](https://maven.apache.org/install.html) `3.8+` (for building the PTF JAR)
- [Python](https://www.python.org/downloads/) `3.10+` (for the Flask backend; also serves the static React UI — no Node/npm required)

> ⚠️ **PTFs are an Early Access feature on Confluent Cloud for Apache Flink.**
> Before running `terraform apply`, the **Process Table Functions** Early Access program
> must be enabled for your Confluent Cloud organization (and, depending on the sub-feature,
> the **PTF timer service** may need to be enabled separately — required for this demo's
> inactivity-detection PTF).
> If it isn't, `terraform apply` will fail on the `CREATE FUNCTION` statement.
> See [Process Table Functions — Confluent Cloud docs](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html)
> and contact Confluent support or your account team to request enablement.

## Quickstart

```bash
# 1) Build the PTF JAR — Terraform reads it from flink-ptf/target/
cd flink-ptf && mvn -q clean package && cd ..

# 2) Provision Confluent Cloud (creates env, cluster, SR, topics, schemas,
#    compute pool, uploads JAR, registers PTF, starts INSERT INTO statement)
export CONFLUENT_CLOUD_API_KEY=...
export CONFLUENT_CLOUD_API_SECRET=...
cd terraform
terraform init
terraform apply              # ~5 minutes; writes ../backend/.env on success
cd ..

# 3) Start the backend (it serves the UI too, at http://localhost:5001)
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
flask --app app run -p 5001  # Press [CTRL]-C to stop it


# 4) Tear it down when done
cd ..
cd terraform && terraform destroy
```

## How the PTF works

The PTF (`flink-ptf/src/main/java/io/confluent/demo/ptf/ClickInactivitySummary.java`)
extends `ProcessTableFunction<Row>` and uses:

- `@StateHint` for per-user managed state (a `Map<product_id, ProductCount>`).
- `@ArgumentHint({SET_SEMANTIC_TABLE, REQUIRE_ON_TIME})` so Flink partitions input by `user_id` and provides event-time semantics.
- A **named** event-time timer `"inactivity"` re-registered on every click — re-using the same name resets it.
- `ctx.clearAllState()` after emitting, so each inactivity burst is independent.

It's registered with `CREATE FUNCTION inactivity_summary AS '…' USING JAR 'confluent-artifact://…'` and invoked from SQL:

```sql
INSERT INTO `user-clicks-summary`
SELECT CAST(user_id AS BYTES) AS key, detected_at, click_counts
FROM inactivity_summary(
  input        => TABLE `user-clicks` PARTITION BY user_id,
  timeout_secs => 10,
  on_time      => DESCRIPTOR(`$rowtime`),
  uid          => 'inactivity-summary-v1'
);
```

## Verification

1. Open `http://localhost:5001`, log in as `clarice`, click 🍕 ×3 and 🍔 ×1 — the "Clicks" panel updates within ~1 s each.
2. Stop clicking. After ~10 s of inactivity (plus a few seconds for watermark advance), the "Summaries" panel shows `clarice: Pizza×3, Burger×1`.
3. Click 🍣 once, wait — next summary contains only `Sushi×1`, proving `clearAllState()` ran.
4. Open a second browser as `bob` — bob's summaries are isolated from clarice's.
5. In Confluent Cloud UI → Flink → Statements, the `insert_into_sink` statement is `RUNNING`.

## Layout

```
flink-ptf-demo/
├── terraform/        # All Confluent Cloud provisioning
├── flink-ptf/        # The PTF Java/Maven project
├── backend/          # Flask + confluent-kafka-python (Avro); also serves the UI
└── frontend/         # Single index.html — React + Babel from CDN, no build step
```

## References

- [Confluent Flink PTF docs](https://docs.confluent.io/cloud/current/flink/concepts/process-table-functions.html)
- [Example_11_ProcessTableFunction.java](https://github.com/confluentinc/flink-table-api-java-examples/blob/master/src/main/java/io/confluent/flink/examples/table/Example_11_ProcessTableFunction.java)
- [`confluent_flink_artifact` Terraform example](https://github.com/confluentinc/terraform-provider-confluent/tree/master/examples/configurations/flink_artifact)
