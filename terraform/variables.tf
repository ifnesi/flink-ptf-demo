variable "demo_prefix" {
  description = "Prefix used in display names for created resources."
  type        = string
  default     = "flink-ptf-demo"
}

variable "cc_cloud_provider" {
  description = "Cloud provider for the Confluent Cloud Kafka cluster and Flink compute pool."
  type        = string
  default     = "AWS"
}

variable "cloud_region" {
  description = "Cloud region for the cluster, Flink compute pool, and Flink artifact."
  type        = string
  default     = "us-east-1"
}

variable "cc_availability" {
  description = "Cluster availability: SINGLE_ZONE or MULTI_ZONE."
  type        = string
  default     = "SINGLE_ZONE"
}

variable "stream_governance" {
  description = "Stream Governance package for the environment (ESSENTIALS or ADVANCED)."
  type        = string
  default     = "ESSENTIALS"
}

variable "flink_max_cfu" {
  description = "Maximum CFUs for the Flink compute pool."
  type        = number
  default     = 10
}

variable "inactivity_timeout_seconds" {
  description = "Number of seconds of inactivity before the PTF emits a summary."
  type        = number
  default     = 10
}

variable "topics" {
  description = "Kafka topics to create on the cluster."
  type = map(object({
    partitions_count = number
    cleanup_policy   = string
    retention_ms     = string
  }))
  default = {
    "user-clicks" = {
      partitions_count = 1
      cleanup_policy   = "delete"
      retention_ms     = "604800000" # 7 days
    }
  }
}
