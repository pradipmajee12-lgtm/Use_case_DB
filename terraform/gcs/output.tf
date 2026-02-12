output "bucket_name" {
  description = "Bucket name"
  value       = google_storage_bucket.gcs_bucket.name
}

output "bucket_url" {
  description = "Bucket URL"
  value       = google_storage_bucket.gcs_bucket.url
}