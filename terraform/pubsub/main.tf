resource "google_pubsub_topic" "topic" {
  name = var.topic_name

  labels = {
    env  = "dev"
    team = "terraform"
  }

  message_retention_duration = "604800s" # 7 days
}
resource "google_pubsub_subscription" "subscription" {
  name  = var.subscription_name
  topic = google_pubsub_topic.topic.name

  ack_deadline_seconds = var.ack_deadline

  message_retention_duration = "604800s"

  retain_acked_messages = false

  expiration_policy {
    ttl = ""
  }
}