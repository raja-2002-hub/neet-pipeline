# ══════════════════════════════════════════════════════════
# NEET Pipeline — Infrastructure as Code
# ══════════════════════════════════════════════════════════
# This file defines ALL GCP resources for the NEET pipeline.
# Run: terraform init → terraform plan → terraform apply
# ══════════════════════════════════════════════════════════

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}


# ──────────────────────────────────────────────────────────
# GCS BUCKETS — File storage
# ──────────────────────────────────────────────────────────

# Input papers (PDF + DOCX uploads)
resource "google_storage_bucket" "input_papers" {
  name          = "${var.project_id}-input-papers"
  location      = var.location
  force_destroy = false

  uniform_bucket_level_access = true
}

# Extracted diagram images
resource "google_storage_bucket" "diagrams" {
  name          = "${var.project_id}-diagrams"
  location      = var.location
  force_destroy = false

  uniform_bucket_level_access = true
}

# Raw JSON extraction results
resource "google_storage_bucket" "raw_json" {
  name          = "${var.project_id}-raw-json"
  location      = var.location
  force_destroy = false

  uniform_bucket_level_access = true
}

# Failed question reports
resource "google_storage_bucket" "failed" {
  name          = "${var.project_id}-failed"
  location      = var.location
  force_destroy = false

  uniform_bucket_level_access = true
}


# ──────────────────────────────────────────────────────────
# BIGQUERY — Data warehouse
# ──────────────────────────────────────────────────────────

# Dataset
resource "google_bigquery_dataset" "question_bank" {
  dataset_id = "question_bank"
  location   = "US"

  description = "NEET question bank"

  labels = {
    project = "neet-pipeline"
    env     = "dev"
  }
}

# Note: BigQuery tables (dim_questions, dim_papers) are managed by the pipeline
# Cloud Function creates them automatically with autodetect schema.
# Terraform manages the dataset, not individual tables, to avoid
# accidental data loss from schema drift.


# ──────────────────────────────────────────────────────────
# CLOUD FUNCTION — Pipeline processor (gen2)
# ──────────────────────────────────────────────────────────

# Service account for Cloud Function
resource "google_service_account" "pipeline_sa" {
  account_id   = "neet-pipeline-sa"
  display_name = "NEET Pipeline Service Account"
}

# Grant BigQuery access
resource "google_project_iam_member" "pipeline_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Grant GCS access
resource "google_project_iam_member" "pipeline_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Grant Vertex AI access (for Gemini)
resource "google_project_iam_member" "pipeline_vertex" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}


# ──────────────────────────────────────────────────────────
# CLOUD RUN — Review dashboard
# ──────────────────────────────────────────────────────────

# Note: Cloud Run service is deployed via gcloud/Dockerfile
# Terraform manages the service account and IAM permissions

resource "google_service_account" "dashboard_sa" {
  account_id   = "neet-dashboard-sa"
  display_name = "NEET Dashboard Service Account"
}

resource "google_project_iam_member" "dashboard_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dashboard_sa.email}"
}

resource "google_project_iam_member" "dashboard_storage" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.dashboard_sa.email}"
}
