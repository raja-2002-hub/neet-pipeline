"""
extract_diagrams.py — Fixed version

Bugs fixed vs original:
  FIX 1: fitz.open(stream=pdf_bytes) instead of fitz.open(pdf_path)
          PDF bytes passed in, no file path needed
  FIX 2: Dynamic section boundary detection
          No more hardcoded page numbers (was NEET 2016 only)
          Detects Chemistry/Biology start pages from actual content
  FIX 3: paper_id passed as parameter everywhere
          No more hardcoded PAPER_ID global string
  FIX 4: Removed input(), load_dotenv(), local file saving
          All incompatible with Cloud Function environment
  FIX 5: BigQuery UPDATE uses PARSE_JSON() not JSON 'value'
          JSON 'value' is non-standard and fails on some BQ versions
  FIX 6: Can be run locally (reads PDF from disk)
          OR called from Cloud Function (receives pdf_bytes)
          Same function handles both modes
"""

import fitz  # PyMuPDF
import re
import json
from collections import defaultdict
from google.cloud import storage
from google.cloud import bigquery

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

PROJECT        = "project-3639c8e1-b432-4a18-99f"
DIAGRAMS_BUCKET = f"{PROJECT}-diagrams"
MIN_IMAGE_SIZE = 50
LOGO_Y_THRESHOLD = 60


# ─────────────────────────────────────────
# FIX 2 — Dynamic section boundary detection
# ─────────────────────────────────────────

def detect_section_boundaries(doc):
    """
    Detects which page each section starts on by scanning
    for section header text in the PDF.

    Replaces hardcoded page numbers (was NEET 2016 specific).
    Works for any NEET year or phase.

    Strategy (in order of priority):
      1. Look for standalone section headers like
         "CHEMISTRY", "BIOLOGY", "PHYSICS" on a page
         (these are the bold section title pages in NEET)
      2. Fall back to question-number-based mapping:
         Physics  Q1-60   → first 20 pages
         Chemistry Q61-120 → next 19 pages
         Biology  Q121-180 → remaining pages

    Returns:
      dict: {
        "Physics":   1,    ← start page (1-indexed)
        "Chemistry": 34,
        "Biology":   53
      }
    """
    chemistry_start = None
    biology_start   = None

    # Section header patterns — NEET uses these exact formats
    # "SECTION - CHEMISTRY", "CHEMISTRY", "Part B Chemistry" etc.
    chem_pattern = re.compile(
        r'\b(CHEMISTRY|Chemistry)\b', re.IGNORECASE
    )
    bio_pattern = re.compile(
        r'\b(BIOLOGY|Biology)\b', re.IGNORECASE
    )

    for page_num in range(len(doc)):
        page     = doc[page_num]
        page_key = page_num + 1
        text     = page.get_text()

        # Only look for section headers in first 80 pages
        # (Biology never starts after page 80 in any NEET paper)
        if page_key > 80:
            break

        # Chemistry section header
        if chemistry_start is None and chem_pattern.search(text):
            # Confirm it's a section header not just a question mentioning chemistry
            # Section headers appear near the top of a page (y < 100)
            blocks = page.get_text("dict")["blocks"]
            for block in blocks:
                if block["type"] != 0:
                    continue
                for line in block["lines"]:
                    for span in line["spans"]:
                        span_text = span["text"].strip()
                        span_y    = span["bbox"][1]
                        if (chem_pattern.search(span_text)
                                and span_y < 120
                                and len(span_text) < 40):
                            chemistry_start = page_key
                            print(f"  Chemistry section detected at page {page_key}")
                            break
                    if chemistry_start:
                        break
                if chemistry_start:
                    break

        # Biology section header
        if (chemistry_start is not None
                and biology_start is None
                and bio_pattern.search(text)):
            blocks = page.get_text("dict")["blocks"]
            for block in blocks:
                if block["type"] != 0:
                    continue
                for line in block["lines"]:
                    for span in line["spans"]:
                        span_text = span["text"].strip()
                        span_y    = span["bbox"][1]
                        if (bio_pattern.search(span_text)
                                and span_y < 120
                                and len(span_text) < 40):
                            biology_start = page_key
                            print(f"  Biology section detected at page {page_key}")
                            break
                    if biology_start:
                        break
                if biology_start:
                    break

    # Fallback — if header detection failed, use question number logic
    # NEET standard: Physics Q1-60, Chemistry Q61-120, Biology Q121-180
    # Approximate page mapping based on ~3 questions per page
    if chemistry_start is None:
        chemistry_start = 22  # ~Q61 ÷ 3 questions per page
        print(f"  Chemistry start not detected — using fallback page {chemistry_start}")

    if biology_start is None:
        biology_start = 42  # ~Q121 ÷ 3 questions per page
        print(f"  Biology start not detected — using fallback page {biology_start}")

    return {
        "Physics":   1,
        "Chemistry": chemistry_start,
        "Biology":   biology_start
    }


def get_section_from_page(page_num, boundaries):
    """
    Returns section name for a given page number.
    Uses dynamically detected boundaries, not hardcoded values.

    Args:
      page_num:   1-indexed page number
      boundaries: dict from detect_section_boundaries()
    """
    if page_num >= boundaries["Biology"]:
        return "Biology"
    elif page_num >= boundaries["Chemistry"]:
        return "Chemistry"
    else:
        return "Physics"


# ─────────────────────────────────────────
# FILTERS
# ─────────────────────────────────────────

def is_watermark(width, height):
    """Large images are background watermarks — skip them."""
    return width > 500 or height > 400

def is_too_small(width, height):
    """Tiny images are artifacts or dots — skip them."""
    return width < MIN_IMAGE_SIZE or height < MIN_IMAGE_SIZE

def is_logo(y0):
    """Images near top of page are header logos — skip them."""
    return y0 < LOGO_Y_THRESHOLD


# ─────────────────────────────────────────
# STEP 1 — Scan page structure
# ─────────────────────────────────────────

def scan_page_structure(doc):
    """
    Scans every page and finds all zone markers.

    FIX 1: Accepts fitz.Document object instead of file path.
           Caller opens the doc with fitz.open(stream=...) or fitz.open(path).

    Key fix retained from original:
      solution_y_positions set — tracks Sol. y coordinates so
      answer numbers like (2) after Sol. are not mistaken as
      option markers.

    Returns:
      dict: { page_number: [sorted list of markers] }
    """
    page_structure = {}

    for page_num in range(len(doc)):
        page   = doc[page_num]
        blocks = page.get_text("dict")["blocks"]

        markers              = []
        solution_y_positions = set()

        for block in blocks:
            if block["type"] != 0:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    text  = span["text"].strip()
                    y_pos = round(span["bbox"][1], 2)

                    # Format 1 — "Question No. 1"
                    if re.match(
                        r'^Question No\.?\s*\d+', text, re.IGNORECASE
                    ):
                        q_num = re.search(r'\d+', text)
                        if q_num:
                            markers.append({
                                "type":            "question_start",
                                "question_number": int(q_num.group()),
                                "y":               y_pos
                            })

                    # Format 2 — "2." (number dot alone)
                    elif re.match(r'^\d+\.$', text):
                        q_num = int(re.search(r'\d+', text).group())
                        if 1 <= q_num <= 180:
                            markers.append({
                                "type":            "question_start",
                                "question_number": q_num,
                                "y":               y_pos
                            })

                    # Solution marker — checked BEFORE options
                    # to populate solution_y_positions first
                    elif re.match(r'^Sol\.', text):
                        markers.append({
                            "type": "solution_start",
                            "y":    y_pos
                        })
                        solution_y_positions.add(y_pos)

                    # Options — only if NOT on same y as a Sol. marker
                    # This prevents "(2)" in "Sol. (2)" being an option marker
                    elif y_pos not in solution_y_positions:

                        if re.match(r'^\(1\)', text):
                            markers.append({
                                "type": "option_1_start", "y": y_pos
                            })
                        elif re.match(r'^\(2\)', text):
                            markers.append({
                                "type": "option_2_start", "y": y_pos
                            })
                        elif re.match(r'^\(3\)', text):
                            markers.append({
                                "type": "option_3_start", "y": y_pos
                            })
                        elif re.match(r'^\(4\)', text):
                            markers.append({
                                "type": "option_4_start", "y": y_pos
                            })

        page_structure[page_num + 1] = sorted(
            markers, key=lambda x: x["y"]
        )

    return page_structure


# ─────────────────────────────────────────
# STEP 2 — Extract and map images
# ─────────────────────────────────────────

def extract_and_map_images(doc, page_structure, boundaries):
    """
    Extracts all valid images and maps each to:
      - Which question it belongs to
      - Which section (Physics/Chemistry/Biology)
      - Which zone: question/option_1/.../solution

    FIX 1: Accepts fitz.Document object (not file path).
    FIX 2: Uses dynamic boundaries dict (not hardcoded pages).

    All original logic retained:
      - Cross-page solution diagrams handled correctly
      - Logo, watermark, tiny image filters all kept
      - Zone detection order kept
    """
    mapped_images = []

    for page_num in range(len(doc)):
        page     = doc[page_num]
        page_key = page_num + 1
        markers  = page_structure.get(page_key, [])
        section  = get_section_from_page(page_key, boundaries)

        image_list = page.get_images(full=True)

        for img_index, img in enumerate(image_list):
            xref       = img[0]
            base_image = doc.extract_image(xref)
            img_bytes  = base_image["image"]
            width      = base_image["width"]
            height     = base_image["height"]

            # Filter out watermarks, tiny images
            if is_watermark(width, height):
                print(f"  Page {page_key} — skipping watermark ({width}x{height})")
                continue
            if is_too_small(width, height):
                continue

            img_rects = page.get_image_rects(xref)
            if not img_rects:
                continue

            rect    = img_rects[0]
            image_y = rect.y0

            # Filter out logos at top of page
            if is_logo(image_y):
                print(f"  Page {page_key} — skipping logo at y0={round(image_y,2)}")
                continue

            # ── Find which question and zone this image belongs to ──
            last_question = None
            last_zone     = "question"

            for marker in markers:
                if marker["y"] > image_y:
                    break
                mtype = marker["type"]
                if mtype == "question_start":
                    last_question = marker["question_number"]
                    last_zone     = "question"
                elif mtype == "option_1_start":
                    last_zone = "option_1"
                elif mtype == "option_2_start":
                    last_zone = "option_2"
                elif mtype == "option_3_start":
                    last_zone = "option_3"
                elif mtype == "option_4_start":
                    last_zone = "option_4"
                elif mtype == "solution_start":
                    last_zone = "solution"

            # ── Handle cross-page solution diagrams ──
            # Image appears before the first question on this page
            # → it belongs to the last question on the previous page (solution zone)
            if not last_question:
                markers_below = [
                    m for m in markers
                    if m["type"] == "question_start" and m["y"] > image_y
                ]
                if markers_below:
                    prev_markers   = page_structure.get(page_key - 1, [])
                    prev_questions = [
                        m for m in prev_markers
                        if m["type"] == "question_start"
                    ]
                    if prev_questions:
                        last_question = prev_questions[-1]["question_number"]
                        last_zone     = "solution"
                        # Use previous page's section for the image
                        section = get_section_from_page(
                            page_key - 1, boundaries
                        )
                        print(
                            f"  Page {page_key} | cross-page → "
                            f"{section} Q{last_question} solution"
                        )
                    else:
                        print(
                            f"  Page {page_key} | img{img_index} "
                            f"| no question found — skipping"
                        )
                        continue
                else:
                    print(
                        f"  Page {page_key} | img{img_index} "
                        f"| no question found — skipping"
                    )
                    continue

            filename = (
                f"p{page_key}_{section.lower()}"
                f"_q{last_question}_{last_zone}_{img_index}.png"
            )

            mapped_images.append({
                "page":            page_key,
                "question_number": last_question,
                "section":         section,
                "zone":            last_zone,
                "width":           width,
                "height":          height,
                "image_bytes":     img_bytes,
                "filename":        filename,
                "position": {
                    "x0": round(rect.x0, 2),
                    "y0": round(image_y, 2),
                    "x1": round(rect.x1, 2),
                    "y1": round(rect.y1, 2)
                }
            })

            print(
                f"  Page {page_key} | {section} Q{last_question}"
                f" | zone={last_zone} | {width}x{height}"
            )

    return mapped_images


# ─────────────────────────────────────────
# STEP 3 — Upload to GCS
# ─────────────────────────────────────────

def upload_to_gcs(image_bytes, filename, paper_id):
    """
    Uploads one image to the diagrams GCS bucket.
    Returns the gs:// URL.
    """
    storage_client = storage.Client(project=PROJECT)
    bucket         = storage_client.bucket(DIAGRAMS_BUCKET)
    blob_path      = f"{paper_id}/{filename}"
    blob           = bucket.blob(blob_path)
    blob.upload_from_string(image_bytes, content_type="image/png")
    gcs_url = f"gs://{DIAGRAMS_BUCKET}/{blob_path}"
    return gcs_url


# ─────────────────────────────────────────
# STEP 4 — Update BigQuery with diagram URLs
# ─────────────────────────────────────────

def update_bigquery_batch(url_map, paper_id):
    """
    Updates dim_questions with JSON arrays of GCS diagram URLs.

    FIX 3: paper_id passed as parameter (was hardcoded global).
    FIX 5: Uses PARSE_JSON() instead of JSON 'value' literal.
           PARSE_JSON() is the official BigQuery function and
           works reliably across all BQ versions.

    Args:
      url_map:  dict — { "Physics_31": {"question": [...], "solution": [...]}, ... }
      paper_id: str  — e.g. "2016_neet_solutions_phase_1_code_a_p_w"
    """
    bq_client = bigquery.Client(project=PROJECT)

    zone_to_column = {
        "question": "question_diagram_urls",
        "solution": "solution_diagram_urls",
        "option_1": "option_1_diagram_urls",
        "option_2": "option_2_diagram_urls",
        "option_3": "option_3_diagram_urls",
        "option_4": "option_4_diagram_urls",
    }

    updated = 0

    for key, zones in url_map.items():
        # key format: "Physics_31" → section=Physics, question_number=31
        parts           = key.rsplit("_", 1)
        section         = parts[0]
        question_number = int(parts[1])

        set_clauses = []
        for zone, urls in zones.items():
            column = zone_to_column.get(zone)
            if column:
                # FIX 5 — PARSE_JSON() is the correct BQ function
                # JSON 'value' literal syntax fails on many BQ versions
                urls_json = json.dumps(urls)
                set_clauses.append(
                    f"{column} = PARSE_JSON('{urls_json}')"
                )

        if not set_clauses:
            continue

        # FIX 3 — paper_id is now a parameter not a hardcoded global
        query = f"""
        UPDATE `{PROJECT}.question_bank.dim_questions`
        SET {', '.join(set_clauses)}
        WHERE paper_id = '{paper_id}'
          AND question_number = {question_number}
          AND section = '{section}'
        """

        try:
            bq_client.query(query).result()
            print(
                f"  Updated {section} Q{question_number} "
                f"→ zones: {list(zones.keys())}"
            )
            updated += 1
        except Exception as e:
            print(f"  Error updating {section} Q{question_number}: {e}")

    return updated


# ─────────────────────────────────────────
# MAIN EXTRACTION FUNCTION
# Called from Cloud Function OR run locally
# ─────────────────────────────────────────

def extract_diagrams(pdf_bytes, paper_id):
    """
    Full diagram extraction pipeline.
    Called from Cloud Function main.py after load_to_bigquery().

    FIX 1: Accepts pdf_bytes (bytes) — no file path needed.
           Works in Cloud Function where PDF never touches disk.
    FIX 2: Detects section boundaries dynamically.
    FIX 3: paper_id passed as parameter throughout.
    FIX 4: No input(), no load_dotenv(), no local file saves.

    Args:
      pdf_bytes: bytes — PDF content downloaded from GCS
      paper_id:  str   — e.g. "2016_neet_solutions_phase_1_code_a_p_w"

    Returns:
      dict: {
        "images_uploaded":   43,
        "questions_updated": 25
      }
    """
    print(f"\n{'='*55}")
    print(f"DIAGRAM EXTRACTION — {paper_id}")
    print(f"{'='*55}")

    # FIX 1 — Open PDF from bytes, not file path
    # This works in both Cloud Function (bytes from GCS)
    # and locally (bytes read from disk)
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    print(f"PDF opened: {len(doc)} pages")

    # FIX 2 — Detect section boundaries dynamically
    print("\nDetecting section boundaries...")
    boundaries = detect_section_boundaries(doc)
    print(f"  Boundaries: {boundaries}")

    # Step 1 — scan page structure
    print("\nStep 1 — Scanning page structure...")
    page_structure = scan_page_structure(doc)

    pages_with_markers = {
        p: m for p, m in page_structure.items() if m
    }
    print(f"Pages with markers: {sorted(pages_with_markers.keys())}")

    # Step 2 — extract and map images
    print("\nStep 2 — Extracting and mapping images...")
    mapped_images = extract_and_map_images(
        doc, page_structure, boundaries
    )
    print(f"\nTotal mapped images: {len(mapped_images)}")

    doc.close()

    if not mapped_images:
        print("No diagrams found. Skipping upload.")
        return {"images_uploaded": 0, "questions_updated": 0}

    # Step 3 — upload to GCS
    print("\nStep 3 — Uploading to GCS...")
    url_map = defaultdict(lambda: defaultdict(list))

    images_uploaded = 0
    for img in mapped_images:
        try:
            gcs_url = upload_to_gcs(
                img["image_bytes"],
                img["filename"],
                paper_id
            )
            print(f"  Uploaded: {img['filename']}")

            # Build url_map for BigQuery update
            # key: "Physics_31", value: {"solution": ["gs://..."], ...}
            key = f"{img['section']}_{img['question_number']}"
            url_map[key][img["zone"]].append(gcs_url)
            images_uploaded += 1

        except Exception as e:
            print(f"  Upload error for {img['filename']}: {e}")

    # Step 4 — update BigQuery with diagram URLs
    print("\nStep 4 — Updating BigQuery...")
    # FIX 3 — pass paper_id as parameter
    questions_updated = update_bigquery_batch(url_map, paper_id)

    print(f"\nDiagram extraction complete:")
    print(f"  Images uploaded:   {images_uploaded}")
    print(f"  Questions updated: {questions_updated}")

    return {
        "images_uploaded":   images_uploaded,
        "questions_updated": questions_updated
    }


# ─────────────────────────────────────────
# LOCAL RUN — for testing from laptop
# ─────────────────────────────────────────

if __name__ == "__main__":
    """
    Run locally to test before deploying to Cloud Function.

    FIX 4: No input() prompt, no load_dotenv().
           Just reads PDF from disk and runs the pipeline.
    """
    PDF_FILE = "2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf"
    PAPER_ID = "2016_neet_solutions_phase_1_code_a_p_w"

    print(f"Reading {PDF_FILE} from disk...")
    with open(PDF_FILE, "rb") as f:
        pdf_bytes = f.read()
    print(f"PDF size: {len(pdf_bytes)/1024/1024:.1f} MB")

    result = extract_diagrams(pdf_bytes, PAPER_ID)

    print(f"\nResult: {result}")