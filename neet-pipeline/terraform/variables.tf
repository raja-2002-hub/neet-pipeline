variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "project-3639c8e1-b432-4a18-99f"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "BigQuery and GCS location"
  type        = string
  default     = "US-CENTRAL1"
}
