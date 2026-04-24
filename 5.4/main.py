import functions_framework
from google.cloud import storage
from google.cloud import bigquery
from google import genai
import google.auth
import json
import re
import os
from datetime import datetime
from collections import defaultdict

from extract_diagrams import extract_diagrams

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

PROJECT         = "project-3639c8e1-b432-4a18-99f"
DATASET         = "question_bank"
TABLE           = "dim_questions"
RAW_JSON_BUCKET = f"{PROJECT}-raw-json"
FAILED_BUCKET   = f"{PROJECT}-failed"
GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY")
GCP_PROJECT     = "project-3639c8e1-b432-4a18-99f"
GCP_LOCATION    = "us-central1"


# ─────────────────────────────────────────
# DUPLICATE GUARD — GCS lock file
# ─────────────────────────────────────────

def paper_already_exists(paper_id, storage_client):
    """Atomic GCS lock — only one Cloud Run instance proceeds."""
    lock_path = f"locks/{paper_id}.lock"
    blob      = storage_client.bucket(RAW_JSON_BUCKET).blob(lock_path)
    try:
        if blob.exists():
            print(f"LOCK EXISTS: '{paper_id}' already processed. Skipping.")
            return True
        blob.upload_from_string(
            f"locked at {datetime.utcnow().isoformat()}",
            content_type="text/plain",
            if_generation_match=0
        )
        print(f"Lock acquired for {paper_id}. Proceeding.")
        return False
    except Exception as e:
        print(f"Lock already taken: {e}")
        return True


# ─────────────────────────────────────────
# NORMALISATION HELPERS
# ─────────────────────────────────────────

def get_gemini_client():
    """
    Returns Vertex AI Gemini client using GCP Application Default Credentials.
    Uses GCP billing credits — no daily quota limit.
    Falls back to API key if Vertex AI fails.
    """
    try:
        client = genai.Client(
            vertexai=True,
            project=GCP_PROJECT,
            location=GCP_LOCATION
        )
        print("Using Vertex AI Gemini client (no quota limits)")
        return client
    except Exception as e:
        print(f"Vertex AI failed ({e}) — falling back to API key")
        return genai.Client(api_key=GEMINI_API_KEY)


def clean_json_response(raw_text, expect_object=False):
    text = raw_text.strip()
    # Strip markdown code fences — Gemini sometimes wraps JSON in ```json ... ```
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    if expect_object:
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
    # Use rfind to get the outermost array — most reliable approach
    # This matches the old working script: raw[raw.find("["):raw.rfind("]")+1]
    if "[" in text:
        start = text.find("[")
        end   = text.rfind("]")
        if start != -1 and end != -1 and end > start:
            candidate = text[start:end+1]
            try:
                result = json.loads(candidate)
                if isinstance(result, list):
                    return candidate
            except:
                pass

    # Fall back to outermost object
    if "{" in text:
        start = text.find("{")
        end   = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidate = text[start:end+1]
            try:
                result = json.loads(candidate)
                if isinstance(result, dict):
                    # Extract list from inside dict if present
                    for val in result.values():
                        if isinstance(val, list) and len(val) > 0:
                            return json.dumps(val)
                    return candidate
            except:
                pass
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
        q.get("expected_time") or 0
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
    raw = str(raw).strip().replace("(", "").replace(")", "")
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


def extract_paper_metadata(filename):
    year_match = re.search(r'20[0-2][0-9]', filename)
    year       = int(year_match.group()) if year_match else 0
    fu         = filename.upper()
    exam       = "NEET" if "NEET" in fu else "JEE" if "JEE" in fu else "UNKNOWN"
    phase      = "Phase 2" if ("PHASE-2" in fu or "PHASE 2" in fu) else "Phase 1"
    return {"year": year, "exam_name": exam, "phase": phase}


# ─────────────────────────────────────────
# FIX 2 — Renumber questions sequentially
# ─────────────────────────────────────────

def renumber_questions(all_questions, sections_in_order):
    """
    Normalises question numbers to sequential across the full paper.

    PROBLEM:
      Gemini extracts each section independently and may reset
      numbering to Q1 per section. NEET uses sequential numbers
      throughout the whole paper.

    SOLUTION:
      Use COUNT of questions processed as offset — not max number.
      This correctly handles:
        - One section resets  (NEET 2016 Biology Q1-90 → Q91-180)
        - All sections reset  (Physics Q1-45, Chem Q1-45, Bio Q1-90)
        - Already sequential  (no changes needed)
        - Partial reset       (only some sections reset)

    WHY COUNT NOT MAX:
      If Chemistry is Q136-180 (already sequential) and Biology
      resets to Q1-90, using max=180 as offset gives Biology Q181-270
      which is wrong. Using count=90 (45 Physics + 45 Chemistry)
      gives Biology Q91-180 which is correct.

    RUNS BEFORE extract_diagrams() so GCS filenames and BigQuery
    keys use the same corrected question numbers — fixing orphan mismatch.
    """
    print("\nRenumbering questions sequentially...")

    by_section = defaultdict(list)
    for q in all_questions:
        by_section[q.get("section", "")].append(q)

    for section in by_section:
        by_section[section].sort(key=lambda q: q.get("question_number", 0))

    total_before = 0
    changes      = 0

    for section in sections_in_order:
        if section not in by_section:
            print(f"  {section}: not found — skipping")
            continue

        qs    = by_section[section]
        min_q = min(q.get("question_number", 0) for q in qs)
        max_q = max(q.get("question_number", 0) for q in qs)
        count = len(qs)

        # Only renumber if section resets to Q1
        # Do NOT renumber if min_q > 1 — section already has correct numbers
        # Example: Biology Q91-180 should NOT be renumbered even though 91 < 180
        needs_renumber = (min_q == 1 and total_before > 0)

        if needs_renumber:
            print(f"  {section}: Q{min_q}-{max_q} → "
                  f"Q{total_before+1}-{total_before+count} "
                  f"(offset={total_before})")
            for i, q in enumerate(qs):
                old_num = q.get("question_number", 0)
                new_num = total_before + i + 1
                old_id  = q.get("question_id", "")
                q["question_number"] = new_num
                q["question_id"]     = old_id.replace(
                    f"_{section.lower()}_q{old_num}",
                    f"_{section.lower()}_q{new_num}"
                )
                changes += 1
        else:
            print(f"  {section}: Q{min_q}-{max_q} — already sequential")

        total_before += count

    if changes > 0:
        print(f"  Renumbered {changes} questions")
    else:
        print(f"  No renumbering needed — all sections sequential")

    print(f"  Final range: Q1-{total_before} ({total_before} total)")
    return all_questions


# ─────────────────────────────────────────
# ATTACH DIAGRAM URLS TO QUESTIONS
# ─────────────────────────────────────────

def attach_diagram_urls(questions, url_map):
    """
    Attaches GCS diagram URLs to question dicts in memory BEFORE INSERT.
    No UPDATE needed — streaming buffer problem permanently eliminated.
    url_map keys use the same corrected question numbers as BigQuery
    because renumber_questions() ran before extract_diagrams().
    
    Zones handled:
      question  → question_diagram_urls
      solution  → solution_diagram_urls
      option_1  → option_1_diagram_urls (cropped from page render)
      option_2  → option_2_diagram_urls
      option_3  → option_3_diagram_urls
      option_4  → option_4_diagram_urls
    """
    attached = 0
    for q in questions:
        section = q.get("section", "")
        q_num   = q.get("question_number", 0)
        key     = f"{section}_{q_num}"
        zones   = url_map.get(key, {})

        q_imgs = zones.get("question", [])

        q["question_diagram_urls"] = json.dumps(q_imgs)
        q["solution_diagram_urls"] = json.dumps(zones.get("solution", []))
        q["option_1_diagram_urls"] = json.dumps(zones.get("option_1", []))
        q["option_2_diagram_urls"] = json.dumps(zones.get("option_2", []))
        q["option_3_diagram_urls"] = json.dumps(zones.get("option_3", []))
        q["option_4_diagram_urls"] = json.dumps(zones.get("option_4", []))

        q["question_diagram_url"] = q_imgs[0] if q_imgs else None
        q["solution_diagram_url"] = zones.get("solution", [None])[0]
        q["option_1_diagram_url"] = zones.get("option_1", [None])[0]
        q["option_2_diagram_url"] = zones.get("option_2", [None])[0]
        q["option_3_diagram_url"] = zones.get("option_3", [None])[0]
        q["option_4_diagram_url"] = zones.get("option_4", [None])[0]

        if zones:
            attached += 1
            opt_count = sum(1 for i in range(1,5) if zones.get(f"option_{i}"))
            if opt_count:
                print(f"  {section} Q{q_num}: {opt_count} option images attached")

    print(f"Diagram URLs attached to {attached} questions")
    return questions


# ─────────────────────────────────────────
# PASS 1 — Detect paper pattern
# ─────────────────────────────────────────

def detect_pattern(pdf_bytes, client):
    """Always returns a dict — never a list."""
    print("Pass 1 — detecting paper pattern...")

    default_pattern = {
        "sections":               ["Physics", "Chemistry", "Biology"],
        "question_number_format": "Question No. X",
        "answer_marker":          "Sol. (X)",
        "has_diagrams":           True,
        "has_formulas":           True
    }

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

    IMPORTANT: Return ONLY the JSON object starting with {
    Do NOT return a list or array. No explanation. No markdown.
    """
    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[{"role": "user", "parts": [
                {"text": prompt},
                {"inline_data": {"mime_type": "application/pdf", "data": pdf_bytes}}
            ]}],
        )
        cleaned = clean_json_response(response.text, expect_object=True)
        result  = json.loads(cleaned)
        if not isinstance(result, dict):
            return default_pattern
        sections = result.get("sections", [])
        if not isinstance(sections, list) or len(sections) == 0:
            return default_pattern
        print(f"Pattern detected: sections={sections}")
        return result
    except Exception as e:
        print(f"Pattern detection failed: {e}. Using defaults.")
        return default_pattern



# ─────────────────────────────────────────
# PDF SECTION SPLITTER
# ─────────────────────────────────────────

def split_pdf_by_section(pdf_bytes, boundaries):
    """
    Splits the full PDF into section-specific PDFs.
    Sends only relevant pages to Gemini — faster and more reliable.
    
    Physics fails when sent the full 83-page PDF because Gemini
    has to scan everything. Sending only pages 1-33 is much faster
    and always succeeds.

    Args:
      pdf_bytes:  full PDF bytes
      boundaries: {"Physics": 1, "Chemistry": 34, "Biology": 53}

    Returns:
      dict: {"Physics": bytes, "Chemistry": bytes, "Biology": bytes}
    """
    import fitz
    doc    = fitz.open(stream=pdf_bytes, filetype="pdf")
    total  = len(doc)
    result = {}

    # Physics: NO overlap — exact pages only to avoid truncation
    # Chemistry/Biology: 2-page overlap for boundary questions
    OVERLAP = 2
    section_pages = {
        "Physics":   (boundaries.get("Physics", 1) - 1,
                      boundaries.get("Chemistry", 34) - 1),
        "Chemistry": (boundaries.get("Chemistry", 34) - 1,
                      min(boundaries.get("Biology", 53) - 1 + OVERLAP, total)),
        "Biology":   (boundaries.get("Biology", 53) - 1,
                      total),
    }

    for section, (start, end) in section_pages.items():
        new_doc = fitz.open()
        new_doc.insert_pdf(doc, from_page=start, to_page=end - 1)
        buf = new_doc.tobytes()
        new_doc.close()
        result[section] = buf
        print(f"  {section}: pages {start+1}-{end} "
              f"({end-start} pages, {len(buf)//1024}KB)")

    doc.close()
    return result

# ─────────────────────────────────────────
# PASS 2 — Extract one section
# ─────────────────────────────────────────


def clean_physics_response(raw_text):
    """
    Cleans Gemini response for JSON parsing.
    Handles:
    1. Markdown code fences (```json ... ```)
    2. Control characters inside JSON strings (newlines in solutions)
    """
    text = raw_text.strip()

    # Strip markdown fences
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    # Find outermost array
    start = text.find("[")
    end   = text.rfind("]")
    if start == -1 or end == -1 or end <= start:
        # Try object
        start = text.find("{")
        end   = text.rfind("}")
        if start == -1 or end == -1:
            return text
    
    candidate = text[start:end+1]

    # Fix control characters inside JSON strings
    cleaned    = []
    in_string  = False
    escape_next = False
    for char in candidate:
        if escape_next:
            cleaned.append(char)
            escape_next = False
        elif char == '\\':
            cleaned.append(char)
            escape_next = True
        elif char == '"':
            in_string = not in_string
            cleaned.append(char)
        elif in_string and ord(char) < 32:
            # Replace control character with space
            cleaned.append(' ')
        else:
            cleaned.append(char)

    return ''.join(cleaned)

def extract_section(pdf_bytes, section, pattern, client, max_retries=2):
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
        "topic": "<topic name>",
        "concept": "<concept name>",
        "subject_concept": "<subject concept>",
        "difficulty": "<Easy/Medium/Hard>",
        "expected_time_seconds": <integer>,
        "question_text": "<complete question text, write [DIAGRAM] if diagram present>",
        "options": {{
            "1": "<option 1 text or exactly [DIAGRAM] if it is a diagram>",
            "2": "<option 2 text or exactly [DIAGRAM] if it is a diagram>",
            "3": "<option 3 text or exactly [DIAGRAM] if it is a diagram>",
            "4": "<option 4 text or exactly [DIAGRAM] if it is a diagram>"
        }},
        "has_question_diagram": <true/false>,
        "has_option_diagram": <true/false>,
        "has_solution_diagram": <true/false>,
        "option_diagrams": {{
            "1": <true/false>, "2": <true/false>,
            "3": <true/false>, "4": <true/false>
        }},
        "correct_answer": "<1/2/3/4>",
        "solution_text": "<complete solution text>",
        "has_diagram": <true/false>,
        "diagram_description": "<describe all diagrams>",
        "confidence": <0.0 to 1.0>
    }}

    CRITICAL RULES:
    - Extract ONLY {section} questions
    - Return ONLY a valid JSON array starting with [ and ending with ]
    - NO markdown code blocks, NO ```json wrappers, NO ``` 
    - NO explanation text before or after the JSON
    - Keep solution_text on ONE LINE — no actual newlines inside strings
    - Replace any newlines in text with a space character
    - Every string value must be on a single line
    - The response must be parseable by json.loads() directly
    """

    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                print(f"  Retry {attempt} for {section}...")

            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[{"role": "user", "parts": [
                    {"text": prompt},
                    {"inline_data": {"mime_type": "application/pdf", "data": pdf_bytes}}
                ]}]
            )

            raw_text = response.text
            print(f"  {section} raw response: {len(raw_text)} chars")
            print(f"  {section} raw preview: {raw_text[:100]}")

            cleaned = clean_physics_response(raw_text)
            print(f"  {section} cleaned preview: {cleaned[:100]}")

            questions = json.loads(cleaned)

            if isinstance(questions, list):
                if len(questions) < 10:
                    print(f"  WARNING: only {len(questions)} — retrying")
                    continue
                print(f"Extracted {len(questions)} {section} questions")
                return questions
            elif isinstance(questions, dict):
                for key, val in questions.items():
                    if isinstance(val, list) and len(val) >= 10:
                        print(f"Extracted {len(val)} {section} questions")
                        return val

        except json.JSONDecodeError as e:
            print(f"  JSON error {section} attempt {attempt}: {e}")
            if attempt == max_retries:
                print(f"  All retries failed for {section}")
                return []
        except Exception as e:
            print(f"  Error {section} attempt {attempt}: {e}")
            if attempt == max_retries:
                return []

    return []


# ─────────────────────────────────────────
# LOAD INTO BIGQUERY — dim_questions
# ─────────────────────────────────────────

def load_to_bigquery(questions, paper_id, metadata):
    """Inserts questions WITH diagram URLs already attached."""
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
                "question_diagram_url":  q.get("question_diagram_url"),
                "solution_diagram_url":  q.get("solution_diagram_url"),
                "option_1_diagram_url":  q.get("option_1_diagram_url"),
                "option_2_diagram_url":  q.get("option_2_diagram_url"),
                "option_3_diagram_url":  q.get("option_3_diagram_url"),
                "option_4_diagram_url":  q.get("option_4_diagram_url"),
                "question_diagram_urls": q.get("question_diagram_urls", json.dumps([])),
                "solution_diagram_urls": q.get("solution_diagram_urls", json.dumps([])),
                "option_1_diagram_urls": q.get("option_1_diagram_urls", json.dumps([])),
                "option_2_diagram_urls": q.get("option_2_diagram_urls", json.dumps([])),
                "option_3_diagram_urls": q.get("option_3_diagram_urls", json.dumps([])),
                "option_4_diagram_urls": q.get("option_4_diagram_urls", json.dumps([])),
            }
            if row["question_id"] and row["question_text"]:
                rows.append(row)
        except Exception as e:
            print(f"Skipping question: {e}")

    # Batch load — no streaming buffer, DELETE/UPDATE work immediately
    import json as _json, io
    from google.cloud.bigquery import LoadJobConfig, SourceFormat

    total = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        try:
            job_config = LoadJobConfig()
            job_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.write_disposition = "WRITE_APPEND"
            job_config.autodetect = False
            ndjson = "\n".join(_json.dumps(r) for r in batch)
            job = bq_client.load_table_from_file(
                io.BytesIO(ndjson.encode()), table_ref, job_config=job_config
            )
            job.result()
            if job.errors:
                print(f"Batch errors: {job.errors[:2]}")
            else:
                total += len(batch)
        except Exception as e:
            print(f"Batch load error: {e}")

    print(f"Loaded {total} rows into BigQuery")
    return total


# ─────────────────────────────────────────
# LOAD INTO BIGQUERY — dim_papers
# ─────────────────────────────────────────

def load_dim_papers(paper_id, file_name, metadata, questions):
    bq_client = bigquery.Client(project=PROJECT)
    table_ref = f"{PROJECT}.{DATASET}.dim_papers"

    physics_count   = len([q for q in questions if q.get("section") == "Physics"])
    chemistry_count = len([q for q in questions if q.get("section") == "Chemistry"])
    biology_count   = len([q for q in questions if q.get("section") == "Biology"])
    total           = len(questions)

    status = "partial" if total < 150 else "completed"
    if status == "partial":
        print(f"WARNING: Only {total} questions — marking partial")

    row = [{
        "paper_id":        paper_id,
        "source_file":     file_name,
        "exam_name":       metadata.get("exam_name", "NEET"),
        "year":            metadata.get("year", 0),
        "phase":           metadata.get("phase", "Phase 1"),
        "total_questions": total,
        "physics_count":   physics_count,
        "chemistry_count": chemistry_count,
        "biology_count":   biology_count,
        "status":          status,
        "processed_at":    datetime.utcnow().isoformat()
    }]

    import json as _json, io
    from google.cloud.bigquery import LoadJobConfig, SourceFormat
    try:
        job_config = LoadJobConfig()
        job_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = "WRITE_APPEND"
        job_config.autodetect = False
        ndjson = _json.dumps(row[0])
        job = bq_client.load_table_from_file(
            io.BytesIO(ndjson.encode()), table_ref, job_config=job_config
        )
        job.result()
        if job.errors:
            print(f"dim_papers error: {job.errors}")
        else:
            print(f"dim_papers inserted: status={status}, total={total}")
    except Exception as e:
        print(f"dim_papers load error: {e}")


# ─────────────────────────────────────────
# SAVE FAILED QUESTIONS TO GCS
# ─────────────────────────────────────────

def save_failed_questions(paper_id, failed_rows):
    if not failed_rows:
        print("No failed questions.")
        return
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename  = f"{paper_id}_failed_{timestamp}.json"
    output = {
        "paper_id":     paper_id,
        "saved_at":     timestamp,
        "total_failed": len(failed_rows),
        "failure_summary": {
            "low_confidence": len([r for r in failed_rows if r["quality_flag"] == "low_confidence"]),
            "missing_text":   len([r for r in failed_rows if r["quality_flag"] == "missing_text"]),
            "invalid_answer": len([r for r in failed_rows if r["quality_flag"] == "invalid_answer"]),
        },
        "failed_questions": failed_rows
    }
    storage_client = storage.Client(project=PROJECT)
    blob = storage_client.bucket(FAILED_BUCKET).blob(filename)
    blob.upload_from_string(
        json.dumps(output, ensure_ascii=False, indent=2),
        content_type="application/json"
    )
    print(f"Saved {len(failed_rows)} failed → gs://{FAILED_BUCKET}/{filename}")


# ─────────────────────────────────────────
# RUN DBT TRANSFORMATIONS
# ─────────────────────────────────────────

def run_dbt_transformations(paper_id):
    print("Running dbt transformations...")
    bq_client = bigquery.Client(project=PROJECT)

    sql_stg = f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.stg_questions` AS
    SELECT
        question_id, paper_id, year, exam_name, phase,
        section, question_number, question_text,
        option_1, option_2, option_3, option_4,
        correct_answer, solution, subject, topic,
        difficulty, expected_time_seconds,
        has_diagram, confidence, is_reviewed,
        question_diagram_url,  solution_diagram_url,
        option_1_diagram_url,  option_2_diagram_url,
        option_3_diagram_url,  option_4_diagram_url,
        question_diagram_urls, solution_diagram_urls,
        option_1_diagram_urls, option_2_diagram_urls,
        option_3_diagram_urls, option_4_diagram_urls,
        CASE
            WHEN question_text IS NULL OR question_text = '' THEN 'missing_text'
            WHEN correct_answer NOT IN ('1','2','3','4')     THEN 'invalid_answer'
            WHEN confidence < 0.8                            THEN 'low_confidence'
            ELSE 'ok'
        END AS quality_flag,
        CASE
            WHEN expected_time_seconds <= 30 THEN 'quick'
            WHEN expected_time_seconds <= 60 THEN 'standard'
            ELSE 'long'
        END AS time_bucket
    FROM `{PROJECT}.{DATASET}.dim_questions`
    """

    sql_clean = f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.dim_questions_clean` AS
    SELECT *
    FROM `{PROJECT}.{DATASET}.stg_questions`
    WHERE quality_flag = 'ok'
      AND confidence >= 0.8
      AND question_text IS NOT NULL
      AND question_text != ''
    """

    sql_summary = f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.subject_summary` AS
    SELECT
        exam_name, year, section, topic, difficulty,
        COUNT(*)                                     AS question_count,
        ROUND(AVG(confidence), 2)                    AS avg_confidence,
        ROUND(AVG(expected_time_seconds), 0)         AS avg_time_seconds,
        SUM(CASE WHEN has_diagram THEN 1 ELSE 0 END) AS diagram_count
    FROM `{PROJECT}.{DATASET}.dim_questions_clean`
    GROUP BY exam_name, year, section, topic, difficulty
    ORDER BY section, topic, difficulty
    """

    sql_fetch_failed = f"""
    SELECT
        question_id, paper_id, section, question_number,
        SUBSTR(question_text, 1, 200) AS question_text_preview,
        correct_answer, confidence,
        CASE
            WHEN question_text IS NULL OR question_text = '' THEN 'missing_text'
            WHEN correct_answer NOT IN ('1','2','3','4')     THEN 'invalid_answer'
            WHEN confidence < 0.8                            THEN 'low_confidence'
            ELSE 'ok'
        END AS quality_flag
    FROM `{PROJECT}.{DATASET}.dim_questions`
    WHERE paper_id = '{paper_id}'
      AND (
            question_text IS NULL OR question_text = ''
         OR correct_answer NOT IN ('1','2','3','4')
         OR confidence < 0.8
      )
    ORDER BY section, question_number
    """

    for table_name, sql in [
        ("stg_questions",       sql_stg),
        ("dim_questions_clean", sql_clean),
        ("subject_summary",     sql_summary),
    ]:
        print(f"  Creating {table_name}...")
        try:
            bq_client.query(sql).result()
            print(f"  ✓ {table_name} done")
        except Exception as e:
            print(f"  ✗ {table_name} FAILED: {e}")
            raise

    print("  Fetching failed questions...")
    try:
        failed_rows = [
            {
                "question_id":           row.question_id,
                "paper_id":              row.paper_id,
                "section":               row.section,
                "question_number":       row.question_number,
                "question_text_preview": row.question_text_preview,
                "correct_answer":        row.correct_answer,
                "confidence":            row.confidence,
                "quality_flag":          row.quality_flag,
            }
            for row in bq_client.query(sql_fetch_failed).result()
        ]
        save_failed_questions(paper_id, failed_rows)
    except Exception as e:
        print(f"  Warning: failed export error: {e}")

    print("dbt transformations complete ✓")


# ─────────────────────────────────────────
# MAIN CLOUD FUNCTION ENTRY POINT
# ─────────────────────────────────────────

@functions_framework.cloud_event
def process_pdf(cloud_event):
    """
    Triggers when a PDF is uploaded to input-papers GCS bucket.

    Pipeline — ALL 3 FIXES APPLIED IN CORRECT ORDER:

      1.  Metadata + paper_id
      2.  GCS lock (duplicate guard)
      3.  Read PDF bytes
      4.  Gemini Pass 1 — detect pattern
      5.  Gemini Pass 2 — extract questions per section

      6.  FIX 2a: renumber_questions()
          Corrects Biology Q1-90 → Q91-180
          Uses COUNT of questions as offset (not max number)
          Handles all edge cases: one reset, all reset, no reset

      7.  FIX 2b + FIX 1: extract_diagrams(corrected_questions)
          Passes corrected questions to derive_boundaries_from_questions()
          which finds exact page where Q46 (first Chemistry) appears
          and Q91 (first Biology) appears — cross-validated with headers
          FIX 1: all images converted to RGB PNG (fixes black Chemistry)
          Returns url_map with matching section+question keys

      8.  attach_diagram_urls() — in memory before INSERT
          Keys match because renumber and boundary derivation
          both use the same corrected question numbers

      9.  Save raw JSON to GCS
      10. INSERT dim_questions WITH URLs (no UPDATE needed)
      11. INSERT dim_papers
      12. Run dbt → dim_questions_clean gets URLs automatically
    """
    data        = cloud_event.data
    bucket_name = data["bucket"]
    file_name   = data["name"]

    print(f"New file: {file_name} in {bucket_name}")

    if not file_name.lower().endswith(".pdf"):
        print("Not a PDF. Skipping.")
        return

    # Step 1
    metadata = extract_paper_metadata(file_name)
    print(f"Metadata: {metadata}")

    paper_id = file_name.replace(".pdf", "").replace(" ", "_").lower()
    paper_id = re.sub(r'[^a-z0-9_]', '_', paper_id)
    print(f"Paper ID: {paper_id}")

    # Step 2 — GCS lock
    storage_client = storage.Client(project=PROJECT)
    if paper_already_exists(paper_id, storage_client):
        return f"Skipped duplicate: {paper_id}"

    # Step 3 — read PDF
    print("Reading PDF from GCS...")
    pdf_bytes = (
        storage_client.bucket(bucket_name)
        .blob(file_name)
        .download_as_bytes()
    )
    print(f"PDF: {len(pdf_bytes)/1024/1024:.1f} MB")

    # Step 4 — Gemini pattern detection
    client  = get_gemini_client()
    pattern = detect_pattern(pdf_bytes, client)
    sections_in_order = pattern.get("sections", ["Physics", "Chemistry", "Biology"])

    # Step 5 — extract questions using FULL PDF
    # Sending full PDF works reliably — section splitting caused truncation
    all_questions = []
    for section in sections_in_order:
        questions = extract_section(pdf_bytes, section, pattern, client)
        for q in questions:
            q["paper_id"]    = paper_id
            q["question_id"] = (
                f"{paper_id}_{section.lower()}_q{q.get('question_number', 0)}"
            )
        all_questions.extend(questions)
    print(f"Total extracted: {len(all_questions)} questions")

    # Step 6 — No renumbering needed
    # question_id has section prefix making it unique:
    # physics_q1, chemistry_q1, biology_q1 — no conflict
    # Gemini returns PDF question numbers which match diagram extraction

    # Step 7 — FIX 2b + FIX 1: extract diagrams with corrected questions
    # corrected_questions → derive_boundaries_from_questions() finds exact
    # page for first Chemistry and first Biology question
    # convert_to_rgb_png() fixes black Chemistry images
    print("\nExtracting diagrams...")
    url_map = {}
    try:
        url_map = extract_diagrams(
            pdf_bytes,
            paper_id,
            corrected_questions=all_questions  # ← FIX 2b key argument
        )
        print(f"Diagrams done: {len(url_map)} questions have images")
    except Exception as e:
        print(f"Diagram extraction failed (non-critical): {e}")

    # Step 8 — attach URLs in memory BEFORE INSERT
    # Keys guaranteed to match because both used corrected numbers
    all_questions = attach_diagram_urls(all_questions, url_map)

    # Step 9 — save JSON to GCS
    storage_client.bucket(RAW_JSON_BUCKET).blob(
        f"{paper_id}.json"
    ).upload_from_string(
        json.dumps({
            "paper_id":        paper_id,
            "source_file":     file_name,
            "year":            metadata.get("year"),
            "exam_name":       metadata.get("exam_name"),
            "total_questions": len(all_questions),
            "questions":       all_questions
        }, ensure_ascii=False),
        content_type="application/json"
    )
    print(f"JSON saved → gs://{RAW_JSON_BUCKET}/{paper_id}.json")

    # Step 10+11 — INSERT with URLs (no UPDATE = no streaming buffer issue)
    load_to_bigquery(all_questions, paper_id, metadata)
    load_dim_papers(paper_id, file_name, metadata, all_questions)

    # Step 12 — dbt immediately
    run_dbt_transformations(paper_id)

    print(f"\n✓ Pipeline complete for {file_name}")
    return f"Processed {len(all_questions)} questions from {file_name}"