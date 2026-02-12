resource "google_storage_bucket" "gcs_bucket" {
  name          = var.bucket_name
  location      = var.location
  storage_class = var.storage_class

  # Security best practice
  uniform_bucket_level_access = true

  # Versioning
  versioning {
    enabled = var.versioning_enabled
  }

  # Lifecycle rule (delete old files after 30 days)
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  # Labels
  labels = {
    environment = "dev"
    managed_by  = "terraform"
  }

  # Allow destroy even if objects exist
  force_destroy = true
}