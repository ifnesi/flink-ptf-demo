# ---------------------------------------------------------------------------
# Flink compute pool, artifact (PTF JAR), and statements
# ---------------------------------------------------------------------------

data "confluent_organization" "demo" {}

resource "confluent_flink_compute_pool" "demo" {
  display_name = "${var.demo_prefix}-pool-${local.name_suffix}"
  cloud        = var.cc_cloud_provider
  region       = var.cloud_region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.demo.id
  }
}

# Upload the PTF JAR produced by `mvn package` in ../flink-ptf/.
resource "confluent_flink_artifact" "click_inactivity" {
  cloud          = var.cc_cloud_provider
  region         = var.cloud_region
  display_name   = "click-inactivity-summary-${local.name_suffix}"
  content_format = "JAR"
  artifact_file  = "${path.module}/../flink-ptf/target/flink-ptf-1.0.0.jar"

  environment {
    id = confluent_environment.demo.id
  }
}

locals {
  flink_statement_properties = {
    "sql.current-catalog"  = confluent_environment.demo.display_name
    "sql.current-database" = confluent_kafka_cluster.demo.display_name
  }
}

# Register the PTF.
resource "confluent_flink_statement" "register_function" {
  organization {
    id = data.confluent_organization.demo.id
  }
  environment {
    id = confluent_environment.demo.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.demo.id
  }
  principal {
    id = confluent_service_account.app_demo.id
  }

  statement = "CREATE FUNCTION inactivity_summary AS 'io.confluent.demo.ptf.ClickInactivitySummary' USING JAR 'confluent-artifact://${confluent_flink_artifact.click_inactivity.id}/${confluent_flink_artifact.click_inactivity.versions[0].version}';"

  properties = local.flink_statement_properties

  rest_endpoint = data.confluent_flink_region.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.flink_key.id
    secret = confluent_api_key.flink_key.secret
  }

  depends_on = [
    confluent_schema.user_clicks_value,
    confluent_schema.user_clicks_summary_value,
  ]
}

# Run the INSERT INTO statement that invokes the PTF. This is the running Flink job.
resource "confluent_flink_statement" "insert_into_sink" {
  organization {
    id = data.confluent_organization.demo.id
  }
  environment {
    id = confluent_environment.demo.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.demo.id
  }
  principal {
    id = confluent_service_account.app_demo.id
  }

  statement  = file("${path.module}/sql/01_insert_into_sink.sql")
  properties = local.flink_statement_properties

  rest_endpoint = data.confluent_flink_region.demo.rest_endpoint

  credentials {
    key    = confluent_api_key.flink_key.id
    secret = confluent_api_key.flink_key.secret
  }

  depends_on = [confluent_flink_statement.register_function]
}
