output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.dataset.dataset_id
}

output "dataset_self_link" {
  description = "Dataset self link"
  value       = google_bigquery_dataset.dataset.self_link
}

output "table_ids" {
  description = "List of created table IDs"
  value       = [for table in google_bigquery_table.tables : table.table_id]
}