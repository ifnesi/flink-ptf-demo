output "environment_id" {
  description = "Confluent Cloud environment ID."
  value       = confluent_environment.demo.id
}

output "kafka_cluster_id" {
  description = "Kafka cluster ID."
  value       = confluent_kafka_cluster.demo.id
}

output "kafka_bootstrap" {
  description = "Kafka bootstrap endpoint (SASL_SSL)."
  value       = confluent_kafka_cluster.demo.bootstrap_endpoint
}

output "schema_registry_url" {
  description = "Schema Registry REST endpoint."
  value       = data.confluent_schema_registry_cluster.demo.rest_endpoint
}

output "flink_compute_pool_id" {
  description = "Flink compute pool ID."
  value       = confluent_flink_compute_pool.demo.id
}

output "flink_artifact_id" {
  description = "ID of the uploaded Flink PTF artifact."
  value       = confluent_flink_artifact.click_inactivity.id
}

output "backend_env_path" {
  description = "Path to the generated backend .env file."
  value       = local_file.backend_env.filename
}
