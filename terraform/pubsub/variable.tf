variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "Default region"
  type        = string
  default     = "europe-west1"
}

variable "topic_name" {
  description = "PubSub topic name"
  type        = string
}

variable "subscription_name" {
  description = "Subscription name"
  type        = string
}

variable "ack_deadline" {
  description = "Ack deadline in seconds"
  type        = number
  default     = 20
}