variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "europe-west1"
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "EU"
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
}

variable "labels" {
  description = "Labels to apply to dataset"
  type        = map(string)
  default     = {}
}

variable "tables" {
  description = "Map of BigQuery tables to create"
  type = map(object({
    schema_file       = string
    description       = optional(string)
    partition_field   = optional(string)
    clustering_fields = optional(list(string))
  }))
}