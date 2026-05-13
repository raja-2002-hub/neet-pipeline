from google.cloud import bigquery
from google.cloud import storage
import json
import re

# Config
PROJECT = "project-3639c8e1-b432-4a18-99f"
BUCKET = f"{PROJECT}-raw-json"
JSON_FILE = "neet_2016_phase1.json"
DATASET = "question_bank"
TABLE = "dim_questions"

# ─────────────────────────────────────────
# NORMALISATION HELPERS
# ─────────────────────────────────────────

def normalise_difficulty(raw):
    """
    Converts all difficulty variations to Easy/Medium/Hard.
    Physics:   Easy, Medium, Hard
    Chemistry: Easy, Moderate, Hard
    Biology:   Easy, Moderate, Tough, Difficult
    """
    if not raw:
        return "Medium"
    raw = str(raw).strip().lower()
    if raw in ["easy"]:
        return "Easy"
    if raw in ["medium", "moderate"]:
        return "Medium"
    if raw in ["hard", "tough", "difficult"]:
        return "Hard"
    return "Medium"  # default fallback


def normalise_time(q):
    """
    Extracts expected time as integer seconds.
    Handles: 45, "45", "45 sec", "45 seconds", "1 min"
    """
    # Try different field names
    raw = (
        q.get("expected_time_seconds") or
        q.get("expected_time_to_solve") or
        q.get("expected_time") or
        0
    )

    if isinstance(raw, int) or isinstance(raw, float):
        return int(raw)

    # Extract number from string like "45 sec" or "1 min"
    raw = str(raw).strip().lower()
    numbers = re.findall(r'\d+', raw)
    if not numbers:
        return 0

    value = int(numbers[0])

    # Convert minutes to seconds
    if "min" in raw:
        value = value * 60

    return value


def normalise_answer(raw):
    """
    Ensures correct_answer is always a clean string 1/2/3/4.
    Handles: 1, "1", "(1)", "A", "none"
    """
    if not raw:
        return ""
    raw = str(raw).strip()
    # Remove brackets
    raw = raw.replace("(", "").replace(")", "")
    # Handle letter options
    letter_map = {"a": "1", "b": "2", "c": "3", "d": "4"}
    if raw.lower() in letter_map:
        return letter_map[raw.lower()]
    # Return as is if it's already 1-4
    if raw in ["1", "2", "3", "4"]:
        return raw
    return raw


def clean_text(text):
    """Remove extra whitespace from text fields."""
    if not text:
        return ""
    return " ".join(str(text).split())


# ─────────────────────────────────────────
# MAIN LOADER
# ─────────────────────────────────────────

print(f"Loading {JSON_FILE} into BigQuery...")

# Step 1 — Read JSON from GCS
print("Reading JSON from GCS bucket...")
storage_client = storage.Client(project=PROJECT)
bucket = storage_client.bucket(BUCKET)
blob = bucket.blob(JSON_FILE)
content = blob.download_as_text(encoding="utf-8")
data = json.loads(content)
questions = data["questions"]
print(f"Found {len(questions)} questions in JSON")

# Step 2 — Prepare and normalise rows
print("Preparing and normalising rows...")
rows = []
skipped = 0

for q in questions:
    try:
        row = {
            "question_id":          q.get("question_id", ""),
            "paper_id":             q.get("paper_id", ""),
            "section":              q.get("section", ""),
            "question_number":      int(q.get("question_number", 0)),
            "question_text":        clean_text(q.get("question_text", "")),
            "option_1":             clean_text(q.get("options", {}).get("1", "")),
            "option_2":             clean_text(q.get("options", {}).get("2", "")),
            "option_3":             clean_text(q.get("options", {}).get("3", "")),
            "option_4":             clean_text(q.get("options", {}).get("4", "")),
            "correct_answer":       normalise_answer(q.get("correct_answer", "")),
            "solution":             clean_text(q.get("solution_text", "")),
            "subject":              q.get("section", ""),
            "topic":                q.get("topic", ""),
            "difficulty":           normalise_difficulty(q.get("difficulty", "")),
            "question_level":       normalise_difficulty(q.get("difficulty", "")),
            "expected_time_seconds": normalise_time(q),
            "has_diagram":          bool(q.get("has_diagram", False)),
            "confidence":           float(q.get("confidence", 0.9)),
            "is_reviewed":          False,
        }

        # Skip rows with empty question_id or question_text
        if not row["question_id"] or not row["question_text"]:
            skipped += 1
            continue

        rows.append(row)

    except Exception as e:
        print(f"  Skipping question due to error: {e}")
        skipped += 1

print(f"Prepared {len(rows)} rows ({skipped} skipped)")

# Step 3 — Insert into BigQuery in batches
print("Inserting into BigQuery...")
bq_client = bigquery.Client(project=PROJECT)
table_ref = f"{PROJECT}.{DATASET}.{TABLE}"

batch_size = 500
total_inserted = 0
total_errors = 0

for i in range(0, len(rows), batch_size):
    batch = rows[i:i+batch_size]
    errors = bq_client.insert_rows_json(table_ref, batch)
    if errors:
        total_errors += len(errors)
        print(f"  Errors in batch {i//batch_size + 1}:")
        for e in errors[:3]:  # Show first 3 errors only
            print(f"    {e}")
    else:
        total_inserted += len(batch)
        print(f"  Inserted {total_inserted} rows so far...")

print(f"\n{'='*50}")
print(f"LOAD COMPLETE")
print(f"{'='*50}")
print(f"Total inserted : {total_inserted}")
print(f"Total skipped  : {skipped}")
print(f"Total errors   : {total_errors}")
print(f"Table          : {table_ref}")

