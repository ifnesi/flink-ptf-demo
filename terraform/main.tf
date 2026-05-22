resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  name_suffix = random_id.suffix.hex
}

# ---------------------------------------------------------------------------
# Environment + Kafka cluster + Schema Registry
# ---------------------------------------------------------------------------

resource "confluent_environment" "demo" {
  display_name = "${var.demo_prefix}-${local.name_suffix}"

  stream_governance {
    package = var.stream_governance
  }
}

resource "confluent_kafka_cluster" "demo" {
  display_name = "${var.demo_prefix}-cluster-${local.name_suffix}"
  availability = var.cc_availability
  cloud        = var.cc_cloud_provider
  region       = var.cloud_region

  basic {}

  environment {
    id = confluent_environment.demo.id
  }
}

# Schema Registry is auto-provisioned with the environment (ESSENTIALS package).
data "confluent_schema_registry_cluster" "demo" {
  environment {
    id = confluent_environment.demo.id
  }

  depends_on = [confluent_kafka_cluster.demo]
}

# ---------------------------------------------------------------------------
# Service account + RBAC + API keys
# ---------------------------------------------------------------------------

resource "confluent_service_account" "app_demo" {
  display_name = "${var.demo_prefix}-sa-${local.name_suffix}"
  description  = "Service account for the Flink PTF demo (Kafka, SR, Flink access)."
}

# EnvironmentAdmin keeps the demo simple. Tighten in production.
resource "confluent_role_binding" "app_demo_env_admin" {
  principal   = "User:${confluent_service_account.app_demo.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.demo.resource_name
}

resource "confluent_api_key" "kafka_key" {
  display_name = "${var.demo_prefix}-kafka-key-${local.name_suffix}"
  description  = "Kafka API key for the demo backend."

  owner {
    id          = confluent_service_account.app_demo.id
    api_version = confluent_service_account.app_demo.api_version
    kind        = confluent_service_account.app_demo.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.demo.id
    api_version = confluent_kafka_cluster.demo.api_version
    kind        = confluent_kafka_cluster.demo.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  depends_on = [confluent_role_binding.app_demo_env_admin]
}

resource "confluent_api_key" "sr_key" {
  display_name = "${var.demo_prefix}-sr-key-${local.name_suffix}"
  description  = "Schema Registry API key for the demo backend."

  owner {
    id          = confluent_service_account.app_demo.id
    api_version = confluent_service_account.app_demo.api_version
    kind        = confluent_service_account.app_demo.kind
  }

  managed_resource {
    id          = data.confluent_schema_registry_cluster.demo.id
    api_version = data.confluent_schema_registry_cluster.demo.api_version
    kind        = data.confluent_schema_registry_cluster.demo.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  depends_on = [confluent_role_binding.app_demo_env_admin]
}

data "confluent_flink_region" "demo" {
  cloud  = var.cc_cloud_provider
  region = var.cloud_region
}

resource "confluent_api_key" "flink_key" {
  display_name = "${var.demo_prefix}-flink-key-${local.name_suffix}"
  description  = "Flink API key used by Terraform to submit statements."

  owner {
    id          = confluent_service_account.app_demo.id
    api_version = confluent_service_account.app_demo.api_version
    kind        = confluent_service_account.app_demo.kind
  }

  managed_resource {
    id          = data.confluent_flink_region.demo.id
    api_version = data.confluent_flink_region.demo.api_version
    kind        = data.confluent_flink_region.demo.kind

    environment {
      id = confluent_environment.demo.id
    }
  }

  depends_on = [confluent_role_binding.app_demo_env_admin]
}

# ---------------------------------------------------------------------------
# Topics + Avro value schemas
# ---------------------------------------------------------------------------

resource "confluent_kafka_topic" "topics" {
  for_each = var.topics

  kafka_cluster {
    id = confluent_kafka_cluster.demo.id
  }

  topic_name       = each.key
  rest_endpoint    = confluent_kafka_cluster.demo.rest_endpoint
  partitions_count = each.value.partitions_count

  config = {
    "cleanup.policy" = each.value.cleanup_policy
    "retention.ms"   = each.value.retention_ms
  }

  credentials {
    key    = confluent_api_key.kafka_key.id
    secret = confluent_api_key.kafka_key.secret
  }
}

resource "confluent_schema" "user_clicks_value" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.demo.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.demo.rest_endpoint

  subject_name = "user-clicks-value"
  format       = "AVRO"
  schema       = file("${path.module}/schemas/user-clicks-value.avsc")

  credentials {
    key    = confluent_api_key.sr_key.id
    secret = confluent_api_key.sr_key.secret
  }

  depends_on = [confluent_kafka_topic.topics]
}

resource "confluent_schema" "user_clicks_summary_value" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.demo.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.demo.rest_endpoint

  subject_name = "user-clicks-summary-value"
  format       = "AVRO"
  schema       = file("${path.module}/schemas/user-clicks-summary-value.avsc")

  credentials {
    key    = confluent_api_key.sr_key.id
    secret = confluent_api_key.sr_key.secret
  }

  depends_on = [confluent_kafka_topic.topics]
}

# ---------------------------------------------------------------------------
# Render backend/.env from outputs
# ---------------------------------------------------------------------------

resource "local_file" "backend_env" {
  filename        = "${path.module}/../backend/.env"
  file_permission = "0600"
  content = templatefile("${path.module}/templates/env.tftpl", {
    bootstrap_servers = confluent_kafka_cluster.demo.bootstrap_endpoint
    kafka_api_key     = confluent_api_key.kafka_key.id
    kafka_api_secret  = confluent_api_key.kafka_key.secret
    sr_url            = data.confluent_schema_registry_cluster.demo.rest_endpoint
    sr_api_key        = confluent_api_key.sr_key.id
    sr_api_secret     = confluent_api_key.sr_key.secret
    clicks_topic      = "user-clicks"
    summaries_topic   = "user-clicks-summary"
  })
}
