# ══════════════════════════════════════════════════════════
# OUTPUTS — Values printed after terraform apply
# Useful for connecting other tools to these resources
# ══════════════════════════════════════════════════════════

output "input_papers_bucket" {
  description = "GCS bucket for input PDF/DOCX files"
  value       = google_storage_bucket.input_papers.name
}

output "diagrams_bucket" {
  description = "GCS bucket for extracted diagram images"
  value       = google_storage_bucket.diagrams.name
}

output "raw_json_bucket" {
  description = "GCS bucket for raw JSON extraction results"
  value       = google_storage_bucket.raw_json.name
}

output "bigquery_dataset" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.question_bank.dataset_id
}
