import functions_framework
from google.cloud import storage
from google.cloud import bigquery
from google import genai
import json
import re
import os

# Config
PROJECT = "project-3639c8e1-b432-4a18-99f"
DATASET = "question_bank"
TABLE = "dim_questions"
RAW_JSON_BUCKET = f"{PROJECT}-raw-json"
FAILED_BUCKET = f"{PROJECT}-failed"
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# ─────────────────────────────────────────
# PAPER METADATA EXTRACTION
# ─────────────────────────────────────────

def extract_paper_metadata(filename):
    """
    Extracts year, exam name and phase from filename.
    2016-NEET-Solutions-Phase-1.pdf → year=2016, exam=NEET, phase=Phase 1
    """
    year_match = re.search(r'20[0-2][0-9]', filename)
    year = int(year_match.group()) if year_match else 0

    filename_upper = filename.upper()
    if "NEET" in filename_upper:
        exam = "NEET"
    elif "JEE" in filename_upper:
        exam = "JEE"
    else:
        exam = "UNKNOWN"

    if "PHASE-2" in filename_upper or "PHASE 2" in filename_upper:
        phase = "Phase 2"
    else:
        phase = "Phase 1"

    return {"year": year, "exam_name": exam, "phase": phase}


# ─────────────────────────────────────────
# NORMALISATION HELPERS
# ─────────────────────────────────────────

def get_gemini_client():
    return genai.Client(api_key=GEMINI_API_KEY)


def clean_json_response(raw_text, expect_object=False):
    """
    Extracts valid JSON from Gemini response.
    Handles extra text before or after the JSON.
    """
    text = raw_text.strip()

    if expect_object:
        # Try to find valid JSON object {}
        start = text.find("{")
        if start != -1:
            for end in range(len(text)-1, start, -1):
                if text[end] == "}":
                    candidate = text[start:end+1]
                    try:
                        result = json.loads(candidate)
                        if isinstance(result, dict):
                            return candidate
                    except:
                        continue

    # Try to find valid JSON array []
    if "[" in text:
        start = text.find("[")
        for end in range(len(text)-1, start, -1):
            if text[end] == "]":
                candidate = text[start:end+1]
                try:
                    json.loads(candidate)
                    return candidate
                except:
                    continue

    # Fallback — try object {}
    if "{" in text:
        start = text.find("{")
        for end in range(len(text)-1, start, -1):
            if text[end] == "}":
                candidate = text[start:end+1]
                try:
                    json.loads(candidate)
                    return candidate
                except:
                    continue

    return text


def normalise_difficulty(raw):
    if not raw:
        return "Medium"
    raw = str(raw).strip().lower()
    if raw in ["easy"]:
        return "Easy"
    if raw in ["medium", "moderate"]:
        return "Medium"
    if raw in ["hard", "tough", "difficult"]:
        return "Hard"
    return "Medium"


def normalise_time(q):
    raw = (
        q.get("expected_time_seconds") or
        q.get("expected_time_to_solve") or
        q.get("expected_time") or
        0
    )
    if isinstance(raw, (int, float)):
        return int(raw)
    raw = str(raw).strip().lower()
    numbers = re.findall(r'\d+', raw)
    if not numbers:
        return 0
    value = int(numbers[0])
    if "min" in raw:
        value = value * 60
    return value


def normalise_answer(raw):
    if not raw:
        return ""
    raw = str(raw).strip()
    raw = raw.replace("(", "").replace(")", "")
    letter_map = {"a": "1", "b": "2", "c": "3", "d": "4"}
    if raw.lower() in letter_map:
        return letter_map[raw.lower()]
    if raw in ["1", "2", "3", "4"]:
        return raw
    return raw


def clean_text(text):
    if not text:
        return ""
    return " ".join(str(text).split())


# ─────────────────────────────────────────
# PASS 1 — Detect paper pattern
# ─────────────────────────────────────────

def detect_pattern(pdf_bytes, client):
    print("Pass 1 — detecting paper pattern...")
    prompt = """
    Analyze this NEET question paper PDF carefully.
    
    Return ONLY a valid JSON object (not an array) with exactly these fields:
    {
        "pattern_type": "section_based",
        "sections": ["Physics", "Chemistry", "Biology"],
        "question_number_format": "exact format used e.g. Question No. X",
        "answer_marker": "exact format used e.g. Sol. (X)",
        "has_diagrams": true,
        "has_formulas": true,
        "total_questions_estimate": 180
    }
    
    Return ONLY the JSON object starting with {
    No explanation. No markdown. No array.
    """
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[{
                "role": "user",
                "parts": [
                    {"text": prompt},
                    {"inline_data": {
                        "mime_type": "application/pdf",
                        "data": pdf_bytes
                    }}
                ]
            }]
        )
        cleaned = clean_json_response(response.text, expect_object=True)
        result = json.loads(cleaned)

        # Safety check — must be a dict
        if isinstance(result, list):
            raise ValueError("Gemini returned list instead of dict")

        print(f"Pattern detected: sections={result.get('sections')}")
        return result

    except Exception as e:
        print(f"Pattern detection failed: {e}. Using defaults.")
        return {
            "sections": ["Physics", "Chemistry", "Biology"],
            "question_number_format": "Question No. X",
            "answer_marker": "Sol. (X)",
            "has_diagrams": True,
            "has_formulas": True
        }


# ─────────────────────────────────────────
# PASS 2 — Extract one section
# ─────────────────────────────────────────

def extract_section(pdf_bytes, section, pattern, client):
    print(f"Extracting {section} questions...")
    prompt = f"""
    Extract ALL {section} questions from this NEET question paper.

    Paper structure:
    - Question format: {pattern.get('question_number_format', 'Question No. X')}
    - Answer marker: {pattern.get('answer_marker', 'Sol. (X)')}
    - Options format: (1)(2)(3)(4)

    For EACH {section} question return a JSON object with EXACTLY these fields:
    {{
        "question_number": <integer>,
        "section": "{section}",
        "topic": "<topic from metadata>",
        "concept": "<concept from metadata>",
        "subject_concept": "<subject concept>",
        "difficulty": "<Easy/Medium/Hard>",
        "expected_time_seconds": <integer>,
        "question_text": "<complete question text, write [DIAGRAM] if question contains a diagram>",
        "options": {{
            "1": "<option 1 text, write [DIAGRAM] if this option is a diagram/image>",
            "2": "<option 2 text, write [DIAGRAM] if this option is a diagram/image>",
            "3": "<option 3 text, write [DIAGRAM] if this option is a diagram/image>",
            "4": "<option 4 text, write [DIAGRAM] if this option is a diagram/image>"
        }},
        "has_question_diagram": <true if question text contains a diagram/figure>,
        "has_option_diagram": <true if ANY option is a diagram/image>,
        "has_solution_diagram": <true if solution contains a diagram/figure>,
        "option_diagrams": {{
            "1": <true/false>,
            "2": <true/false>,
            "3": <true/false>,
            "4": <true/false>
        }},
        "correct_answer": "<1/2/3/4>",
        "solution_text": "<complete solution text, write [DIAGRAM] where diagrams appear>",
        "has_diagram": <true if ANY diagram exists in question options or solution>,
        "diagram_description": "<describe ALL diagrams present in detail>",
        "confidence": <0.0 to 1.0>
    }}

    Extract ONLY {section} questions.
    Return ONLY a JSON array starting with [
    No markdown. No explanation.
    """
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[{
                "role": "user",
                "parts": [
                    {"text": prompt},
                    {"inline_data": {
                        "mime_type": "application/pdf",
                        "data": pdf_bytes
                    }}
                ]
            }]
        )
        cleaned = clean_json_response(response.text)
        questions = json.loads(cleaned)
        if isinstance(questions, list):
            print(f"Extracted {len(questions)} {section} questions")
            return questions
        return []
    except Exception as e:
        print(f"Error extracting {section}: {e}")
        return []


# ─────────────────────────────────────────
# LOAD INTO BIGQUERY — dim_questions
# ─────────────────────────────────────────

def load_to_bigquery(questions, paper_id, metadata):
    print(f"Loading {len(questions)} questions into BigQuery...")
    bq_client = bigquery.Client(project=PROJECT)
    table_ref = f"{PROJECT}.{DATASET}.{TABLE}"

    rows = []
    for q in questions:
        try:
            row = {
                "question_id":           q.get("question_id", ""),
                "paper_id":              paper_id,
                "year":                  metadata.get("year", 0),
                "exam_name":             metadata.get("exam_name", "NEET"),
                "phase":                 metadata.get("phase", "Phase 1"),
                "section":               q.get("section", ""),
                "question_number":       int(q.get("question_number", 0)),
                "question_text":         clean_text(q.get("question_text", "")),
                "option_1":              clean_text(q.get("options", {}).get("1", "")),
                "option_2":              clean_text(q.get("options", {}).get("2", "")),
                "option_3":              clean_text(q.get("options", {}).get("3", "")),
                "option_4":              clean_text(q.get("options", {}).get("4", "")),
                "correct_answer":        normalise_answer(q.get("correct_answer", "")),
                "solution":              clean_text(q.get("solution_text", "")),
                "subject":               q.get("section", ""),
                "topic":                 q.get("topic", ""),
                "difficulty":            normalise_difficulty(q.get("difficulty", "")),
                "question_level":        normalise_difficulty(q.get("difficulty", "")),
                "expected_time_seconds": normalise_time(q),
                "has_diagram":           bool(q.get("has_diagram", False)),
                "has_question_diagram":  bool(q.get("has_question_diagram", False)),
                "has_option_diagram":    bool(q.get("has_option_diagram", False)),
                "has_solution_diagram":  bool(q.get("has_solution_diagram", False)),
                "confidence":            float(q.get("confidence", 0.9)),
                "is_reviewed":           False,
                "question_diagram_url":  None,
                "solution_diagram_url":  None,
                "option_1_diagram_url":  None,
                "option_2_diagram_url":  None,
                "option_3_diagram_url":  None,
                "option_4_diagram_url":  None,
            }
            if row["question_id"] and row["question_text"]:
                rows.append(row)
        except Exception as e:
            print(f"Skipping question: {e}")

    total = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        errors = bq_client.insert_rows_json(table_ref, batch)
        if not errors:
            total += len(batch)
        else:
            print(f"Batch errors: {errors[:2]}")

    print(f"Loaded {total} rows into BigQuery")
    return total


# ─────────────────────────────────────────
# LOAD INTO BIGQUERY — dim_papers
# ─────────────────────────────────────────

def load_dim_papers(paper_id, file_name, metadata, questions):
    """Inserts one row into dim_papers for this paper."""
    bq_client = bigquery.Client(project=PROJECT)
    table_ref = f"{PROJECT}.{DATASET}.dim_papers"

    physics_count = len([q for q in questions if q.get("section") == "Physics"])
    chemistry_count = len([q for q in questions if q.get("section") == "Chemistry"])
    biology_count = len([q for q in questions if q.get("section") == "Biology"])

    row = [{
        "paper_id":        paper_id,
        "source_file":     file_name,
        "exam_name":       metadata.get("exam_name", "NEET"),
        "year":            metadata.get("year", 0),
        "phase":           metadata.get("phase", "Phase 1"),
        "total_questions": len(questions),
        "physics_count":   physics_count,
        "chemistry_count": chemistry_count,
        "biology_count":   biology_count,
        "status":          "completed",
        "processed_at":    None
    }]

    errors = bq_client.insert_rows_json(table_ref, row)
    if errors:
        print(f"dim_papers error: {errors}")
    else:
        print(f"dim_papers row inserted: year={metadata.get('year')}, exam={metadata.get('exam_name')}")


# ─────────────────────────────────────────
# MAIN CLOUD FUNCTION ENTRY POINT
# ─────────────────────────────────────────

@functions_framework.cloud_event
def process_pdf(cloud_event):
    """
    Triggers automatically when a PDF is uploaded
    to the input-papers GCS bucket.
    """
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"New file detected: {file_name} in {bucket_name}")

    if not file_name.lower().endswith(".pdf"):
        print("Not a PDF file. Skipping.")
        return

    # Extract metadata from filename
    metadata = extract_paper_metadata(file_name)
    print(f"Metadata: {metadata}")

    # Generate paper_id
    paper_id = file_name.replace(".pdf", "").replace(" ", "_").lower()
    paper_id = re.sub(r'[^a-z0-9_]', '_', paper_id)
    print(f"Paper ID: {paper_id}")

    # Read PDF from GCS
    print("Reading PDF from GCS...")
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    pdf_bytes = blob.download_as_bytes()
    print(f"PDF size: {len(pdf_bytes)/1024/1024:.1f} MB")

    # Initialise Gemini
    client = get_gemini_client()

    # Pass 1 — detect pattern
    pattern = detect_pattern(pdf_bytes, client)

    # Pass 2 — extract section by section
    all_questions = []
    sections = pattern.get("sections", ["Physics", "Chemistry", "Biology"])

    for section in sections:
        questions = extract_section(pdf_bytes, section, pattern, client)
        for q in questions:
            q["paper_id"] = paper_id
            q["question_id"] = f"{paper_id}_{section.lower()}_q{q.get('question_number', 0)}"
        all_questions.extend(questions)

    print(f"Total extracted: {len(all_questions)} questions")

    # Save JSON to raw-json bucket
    output = {
        "paper_id":        paper_id,
        "source_file":     file_name,
        "year":            metadata.get("year"),
        "exam_name":       metadata.get("exam_name"),
        "total_questions": len(all_questions),
        "questions":       all_questions
    }

    json_filename = f"{paper_id}.json"
    json_bucket = storage_client.bucket(RAW_JSON_BUCKET)
    json_blob = json_bucket.blob(json_filename)
    json_blob.upload_from_string(
        json.dumps(output, ensure_ascii=False),
        content_type="application/json"
    )
    print(f"JSON saved to gs://{RAW_JSON_BUCKET}/{json_filename}")

    # Load into BigQuery
    load_to_bigquery(all_questions, paper_id, metadata)
    load_dim_papers(paper_id, file_name, metadata, all_questions)

    print(f"Pipeline complete for {file_name}")
    return f"Processed {len(all_questions)} questions from {file_name}"