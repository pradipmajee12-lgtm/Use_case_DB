############################################################
# BigQuery Dataset
############################################################

resource "google_bigquery_dataset" "dataset" {
  dataset_id  = var.dataset_id
  location    = var.location
  description = "Dataset created using Terraform"

  delete_contents_on_destroy = true

  labels = var.labels
}

############################################################
# Multiple BigQuery Tables (Dynamic)
############################################################

resource "google_bigquery_table" "tables" {
  for_each = var.tables

  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = each.key

  description = lookup(each.value, "description", null)

  schema = file("${path.module}/${each.value.schema_file}")

  ##########################################################
  # Optional Partitioning
  ##########################################################

  dynamic "time_partitioning" {
    for_each = lookup(each.value, "partition_field", null) != null ? [1] : []
    content {
      type  = "DAY"
      field = each.value.partition_field
    }
  }

  ##########################################################
  # Optional Clustering
  ##########################################################

  clustering = lookup(each.value, "clustering_fields", null)

  deletion_protection = false
}

