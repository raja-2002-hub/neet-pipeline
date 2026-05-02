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

# Note: Pipeline-generated tables (dim_questions, dim_papers, stg_questions)
# are managed by the Cloud Function with autodetect schema — intentionally
# kept out of Terraform to avoid drift as extraction logic evolves.
#
# App-owned tables with stable, hand-authored schemas ARE managed by
# Terraform below — they're infrastructure, not pipeline output.

# ── Student Attempts ──
resource "google_bigquery_table" "student_attempts" {
  dataset_id          = google_bigquery_dataset.question_bank.dataset_id
  table_id            = "student_attempts"
  project             = var.project_id
  deletion_protection = true

  description = "Per-student per-question attempt log for NEET Student App"

  time_partitioning {
    type  = "DAY"
    field = "attempt_timestamp"
  }

  clustering = ["student_id", "section", "topic"]

  labels = {
    app = "neet-student"
  }

  schema = jsonencode([
    { name = "attempt_id",            type = "STRING",    mode = "REQUIRED", description = "UUID of this attempt" },
    { name = "quiz_session_id",       type = "STRING",    mode = "REQUIRED", description = "Groups all attempts within one quiz" },
    { name = "student_id",            type = "STRING",    mode = "REQUIRED", description = "Firebase UID" },
    { name = "student_email",         type = "STRING",    mode = "NULLABLE", description = "Firebase email" },
    { name = "question_id",           type = "STRING",    mode = "REQUIRED", description = "FK to dim_questions_clean.question_id" },
    { name = "paper_id",              type = "STRING",    mode = "NULLABLE", description = "Snapshot from dim_questions_clean" },
    { name = "section",               type = "STRING",    mode = "NULLABLE", description = "Physics / Chemistry / Biology" },
    { name = "topic",                 type = "STRING",    mode = "NULLABLE", description = "Snapshot of topic at attempt time" },
    { name = "difficulty",            type = "STRING",    mode = "NULLABLE", description = "Easy / Medium / Hard" },
    { name = "selected_answer",       type = "STRING",    mode = "NULLABLE", description = "'1'..'4' or NULL if skipped" },
    { name = "correct_answer",        type = "STRING",    mode = "REQUIRED", description = "Snapshot of correct answer" },
    { name = "is_correct",            type = "BOOL",      mode = "NULLABLE", description = "NULL if skipped" },
    { name = "is_skipped",            type = "BOOL",      mode = "REQUIRED", description = "True when selected_answer IS NULL" },
    { name = "time_taken_seconds",    type = "INT64",     mode = "NULLABLE", description = "Seconds spent on this question" },
    { name = "expected_time_seconds", type = "INT64",     mode = "NULLABLE", description = "Snapshot from dim_questions_clean" },
    { name = "quiz_mode",             type = "STRING",    mode = "NULLABLE", description = "custom | mock | adaptive" },
    { name = "quiz_filters_json",     type = "STRING",    mode = "NULLABLE", description = "JSON of filters used to generate quiz" },
    { name = "attempt_timestamp",     type = "TIMESTAMP", mode = "REQUIRED", description = "When answer was submitted" }
  ])
}

# ── Chat Sessions ──
resource "google_bigquery_table" "chat_sessions" {
  dataset_id          = google_bigquery_dataset.question_bank.dataset_id
  table_id            = "chat_sessions"
  project             = var.project_id
  deletion_protection = true

  description = "Chat conversation threads for NEET Student App"

  time_partitioning {
    type  = "DAY"
    field = "updated_at"
  }

  clustering = ["student_id"]

  labels = {
    app = "neet-student"
  }

  schema = jsonencode([
    { name = "session_id", type = "STRING",    mode = "REQUIRED", description = "UUID for this chat thread" },
    { name = "student_id", type = "STRING",    mode = "REQUIRED", description = "Firebase UID" },
    { name = "title",      type = "STRING",    mode = "NULLABLE", description = "Auto-generated title from first message" },
    { name = "messages_json", type = "STRING", mode = "NULLABLE", description = "JSON array of {role, text, tool_outputs}" },
    { name = "created_at", type = "TIMESTAMP", mode = "REQUIRED", description = "When chat was created" },
    { name = "updated_at", type = "TIMESTAMP", mode = "REQUIRED", description = "Last message timestamp" }
  ])
}

# ── Bookmarks ──
resource "google_bigquery_table" "bookmarks" {
  dataset_id          = google_bigquery_dataset.question_bank.dataset_id
  table_id            = "bookmarks"
  project             = var.project_id
  deletion_protection = true

  description = "Student question bookmarks for NEET Student App"

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }

  clustering = ["student_id"]

  labels = {
    app = "neet-student"
  }

  schema = jsonencode([
    { name = "student_id",  type = "STRING",    mode = "REQUIRED", description = "Firebase UID" },
    { name = "question_id", type = "STRING",    mode = "REQUIRED", description = "Question ID from dim_questions" },
    { name = "section",     type = "STRING",    mode = "NULLABLE", description = "Physics/Chemistry/Biology" },
    { name = "topic",       type = "STRING",    mode = "NULLABLE", description = "Topic name" },
    { name = "note",        type = "STRING",    mode = "NULLABLE", description = "Student's personal note" },
    { name = "created_at",  type = "TIMESTAMP", mode = "REQUIRED", description = "When bookmarked" }
  ])
}

# ── Review Schedule (Spaced Repetition) ──
resource "google_bigquery_table" "review_schedule" {
  dataset_id          = google_bigquery_dataset.question_bank.dataset_id
  table_id            = "review_schedule"
  project             = var.project_id
  deletion_protection = true

  description = "Spaced repetition review schedule for NEET Student App"

  time_partitioning {
    type  = "DAY"
    field = "next_review_date"
  }

  clustering = ["student_id"]

  labels = {
    app = "neet-student"
  }

  schema = jsonencode([
    { name = "student_id",       type = "STRING",    mode = "REQUIRED", description = "Firebase UID" },
    { name = "question_id",      type = "STRING",    mode = "REQUIRED", description = "Question ID" },
    { name = "section",          type = "STRING",    mode = "NULLABLE", description = "Physics/Chemistry/Biology" },
    { name = "topic",            type = "STRING",    mode = "NULLABLE", description = "Topic name" },
    { name = "interval_days",    type = "INT64",     mode = "REQUIRED", description = "Current interval: 1, 3, 7, 14, 30" },
    { name = "next_review_date", type = "DATE",      mode = "REQUIRED", description = "When due for review" },
    { name = "times_reviewed",   type = "INT64",     mode = "REQUIRED", description = "How many times reviewed" },
    { name = "last_result",      type = "STRING",    mode = "NULLABLE", description = "correct or incorrect" },
    { name = "created_at",       type = "TIMESTAMP", mode = "REQUIRED", description = "When first scheduled" },
    { name = "updated_at",       type = "TIMESTAMP", mode = "REQUIRED", description = "Last update" }
  ])
}

# ── Formulas ──
resource "google_bigquery_table" "formulas" {
  dataset_id          = google_bigquery_dataset.question_bank.dataset_id
  table_id            = "formulas"
  project             = var.project_id
  deletion_protection = true

  description = "Extracted formulas from NEET questions for formula quiz feature"

  clustering = ["section", "topic"]

  labels = {
    app = "neet-student"
  }

  schema = jsonencode([
    { name = "formula_id",           type = "STRING",    mode = "REQUIRED", description = "Unique formula ID" },
    { name = "formula_text",         type = "STRING",    mode = "REQUIRED", description = "The formula itself" },
    { name = "formula_name",         type = "STRING",    mode = "NULLABLE", description = "Short name" },
    { name = "concept",              type = "STRING",    mode = "NULLABLE", description = "What this formula represents" },
    { name = "section",              type = "STRING",    mode = "REQUIRED", description = "Physics/Chemistry/Biology" },
    { name = "topic",                type = "STRING",    mode = "REQUIRED", description = "Topic name" },
    { name = "difficulty",           type = "STRING",    mode = "NULLABLE", description = "Easy/Medium/Hard" },
    { name = "hint",                 type = "STRING",    mode = "NULLABLE", description = "Memory trick" },
    { name = "related_question_ids", type = "STRING",    mode = "NULLABLE", description = "JSON array of question IDs" },
    { name = "created_at",           type = "TIMESTAMP", mode = "REQUIRED", description = "When extracted" }
  ])
}


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
