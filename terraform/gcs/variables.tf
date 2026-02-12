variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west1"
}

variable "bucket_name" {
  description = "Unique GCS bucket name"
  type        = string
}

variable "location" {
  description = "Bucket location"
  type        = string
  default     = "EUROPE"
}

variable "storage_class" {
  description = "Storage class for bucket"
  type        = string
  default     = "STANDARD"
}

variable "versioning_enabled" {
  description = "Enable versioning"
  type        = bool
  default     = true
}