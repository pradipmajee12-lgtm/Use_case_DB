project_id = "my-first-project"
region     = "europe-west1"
location   = "EU"
dataset_id = "trade_event"
labels = {
  env        = "dev"
  team       = "data"
  managed_by = "terraform"
}

tables = {
  trade_landing = {
    schema_file       = "schema/trade_landing_schema.json"
    description       = "Users master table"
    partition_field   = "received_at"
    clustering_fields = ["trade_id"]
  }

  trade_current = {
    schema_file       = "schema/trade_landing_schema.json"
    description       = "Trade events table"
    partition_field   = "updated_at"
    clustering_fields = ["trade_id"]
  }

  trade_rejected = {
    schema_file       = "schema/trade_rejected_schema.json"
    description       = "Trade events table"
    partition_field   = "received_at"
    clustering_fields = ["trade_id"]
  }

    trade_history = {
    schema_file       = "schema/trade_history_schema.json"
    description       = "Trade events table"
    partition_field   = "ingested_at"
    clustering_fields = ["trade_id"]
  }

}
