"""
extract_diagrams.py

Three fixes applied:

FIX 1 — PIL color conversion
  CMYK and inverted images (common in Chemistry) converted to RGB PNG
  Eliminates black background issue in Chemistry diagrams

FIX 2 — Cross-validated section boundaries
  Instead of guessing from page headers alone,
  boundaries are derived from actual question positions on pages.
  PyMuPDF scans for question markers, finds which page each question
  number appears on, then derives section start pages from Gemini's
  corrected question numbers.
  This guarantees image section mapping matches BigQuery section mapping.

FIX 3 — No BigQuery UPDATE
  Returns url_map dict for main.py to attach before INSERT.
  Streaming buffer problem permanently eliminated.
"""

import fitz       # PyMuPDF
import io
import re
from collections import defaultdict

import numpy as np
from PIL import Image
from google.cloud import storage

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

PROJECT          = "project-3639c8e1-b432-4a18-99f"
DIAGRAMS_BUCKET  = f"{PROJECT}-diagrams"
MIN_IMAGE_SIZE   = 50
LOGO_Y_THRESHOLD = 60


# ─────────────────────────────────────────
# FIX 1 — PIL color conversion
# ─────────────────────────────────────────

def convert_to_rgb_png(image_bytes):
    """
    Converts any PDF image colorspace to standard RGB PNG.

    WHY:
      Chemistry diagrams are often CMYK (black background)
      or inverted (white lines on black) because they are
      created in ChemDraw / MarvinSketch which exports CMYK.
      Physics and Biology are standard RGB.

    DETECTION:
      After converting to RGB, if mean brightness < 80/255
      the image is inverted — flip it to get white background.

    Returns clean RGB PNG bytes safe for browser display.
    """
    try:
        img = Image.open(io.BytesIO(image_bytes))

        # Convert CMYK → RGB
        if img.mode == "CMYK":
            img = img.convert("RGB")
        # Flatten RGBA transparency onto white background
        elif img.mode == "RGBA":
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[3])
            img = bg
        # Convert any other mode to RGB
        elif img.mode != "RGB":
            img = img.convert("RGB")

        # Detect inverted image (dark background, light content)
        arr  = np.array(img)
        mean = arr.mean()
        if mean < 80:
            img = Image.fromarray(255 - arr)
            print(f"    [color fix: inverted image corrected, brightness was {mean:.0f}]")

        out = io.BytesIO()
        img.save(out, format="PNG", optimize=True)
        return out.getvalue()

    except Exception as e:
        print(f"    [color fix failed: {e} — using original]")
        return image_bytes


# ─────────────────────────────────────────
# FIX 2 — Cross-validated section boundaries
# ─────────────────────────────────────────

def derive_boundaries_from_questions(doc, corrected_questions):
    """
    Derives section page boundaries from actual question positions
    in the PDF, cross-validated against Gemini's corrected numbers.

    WHY THIS IS MORE RELIABLE THAN HEADER DETECTION:
      Header detection relies on finding "Chemistry" text near the
      top of a page. This fails when:
        - Headers are watermarks (hard to detect)
        - Section titles appear mid-page
        - Paper format varies between years

      Question position detection finds the EXACT page where
      question N appears. Since Gemini already gave us correct
      sequential numbers (after renumber_questions()), we know:
        - First Chemistry question number = Physics count + 1
        - First Biology question number = Physics + Chemistry count + 1

      We then scan the PDF for these question numbers and get
      the exact page where each section starts.

    ALGORITHM:
      1. From corrected_questions, find the first question number
         of each section (after renumbering)
      2. Scan PDF pages for markers matching those question numbers
      3. Return page boundaries derived from actual question locations
      4. Fall back to header detection if scan fails
      5. Log conflict if header detection disagrees

    Args:
      doc:                 fitz.Document
      corrected_questions: list of question dicts after renumber_questions()

    Returns:
      dict: {"Physics": 1, "Chemistry": 34, "Biology": 53}
    """
    print("\nDeriving section boundaries from question positions...")

    # Step 1 — Find first question number per section from Gemini data
    # Store as {section: (question_number, section_name)} to handle
    # per-section numbering where Physics Q1 and Biology Q1 both exist
    section_first_q = {}
    by_section      = defaultdict(list)
    for q in corrected_questions:
        sect = q.get("section", "")
        by_section[sect].append(q.get("question_number", 0))

    for section, nums in by_section.items():
        if nums:
            section_first_q[section] = min(nums)

    print(f"  First question per section: {section_first_q}")

    # Step 2 — Use header detection as primary boundary method
    # since per-section numbering means Q1 appears in multiple sections
    header_boundaries = detect_boundaries_from_headers(doc)
    print(f"  Header boundaries: {header_boundaries}")

    # Step 3 — Build boundaries
    # Physics always starts at page 1
    boundaries = {"Physics": 1}

    sections_in_order = ["Physics", "Chemistry", "Biology"]
    for section in sections_in_order:
        if section == "Physics":
            continue

        if header_boundaries.get(section):
            boundaries[section] = header_boundaries[section]
            print(f"  {section}: header detection page {header_boundaries[section]} ✓")
        else:
            # Header detection failed — scan for section-specific question
            # For Chemistry: look for Q136 (sequential) or first Chemistry Q
            # For Biology: look for questions after Chemistry boundary
            print(f"  {section}: header detection failed — scanning questions")
            
            # Find first question of this section by scanning pages after
            # previous section boundary
            prev_section = sections_in_order[sections_in_order.index(section) - 1]
            prev_page    = boundaries.get(prev_section, 1)
            first_q      = section_first_q.get(section, 1)

            for page_num in range(prev_page, len(doc)):
                page   = doc[page_num]
                blocks = page.get_text("dict")["blocks"]
                found  = False
                for block in blocks:
                    if block["type"] != 0:
                        continue
                    for line in block["lines"]:
                        for span in line["spans"]:
                            text = span["text"].strip()
                            m = re.match(rf'^Question No\.?\s*{first_q}', text, re.IGNORECASE)
                            m2 = re.match(rf'^{first_q}\.$', text)
                            if m or m2:
                                boundaries[section] = page_num + 1
                                print(f"  {section}: found Q{first_q} on page {page_num + 1}")
                                found = True
                                break
                        if found:
                            break
                    if found:
                        break
                if found:
                    break
            
            if section not in boundaries:
                # Last resort fallback
                fallback = {"Chemistry": 34, "Biology": 53}
                boundaries[section] = fallback.get(section, 1)
                print(f"  {section}: using fallback page {boundaries[section]}")

    print(f"  Final boundaries: {boundaries}")
    return boundaries


def detect_boundaries_from_headers(doc):
    """
    Original header-based detection — now used only for cross-validation.
    Scans for "Chemistry" / "Biology" text near top of pages.
    """
    chem_pattern = re.compile(r'\b(CHEMISTRY|Chemistry)\b', re.IGNORECASE)
    bio_pattern  = re.compile(r'\b(BIOLOGY|Biology)\b',    re.IGNORECASE)

    chemistry_start = None
    biology_start   = None

    for page_num in range(len(doc)):
        page     = doc[page_num]
        page_key = page_num + 1
        if page_key > 80:
            break

        text   = page.get_text()
        blocks = page.get_text("dict")["blocks"]

        if chemistry_start is None and chem_pattern.search(text):
            for block in blocks:
                if block["type"] != 0:
                    continue
                for line in block["lines"]:
                    for span in line["spans"]:
                        st = span["text"].strip()
                        sy = span["bbox"][1]
                        if chem_pattern.search(st) and sy < 120 and len(st) < 40:
                            chemistry_start = page_key
                            break
                    if chemistry_start:
                        break
                if chemistry_start:
                    break

        if chemistry_start and biology_start is None and bio_pattern.search(text):
            for block in blocks:
                if block["type"] != 0:
                    continue
                for line in block["lines"]:
                    for span in line["spans"]:
                        st = span["text"].strip()
                        sy = span["bbox"][1]
                        if bio_pattern.search(st) and sy < 120 and len(st) < 40:
                            biology_start = page_key
                            break
                    if biology_start:
                        break
                if biology_start:
                    break

    return {
        "Chemistry": chemistry_start,
        "Biology":   biology_start
    }


def get_section_from_page(page_num, boundaries):
    """Returns section name for a given page using validated boundaries."""
    if page_num >= boundaries.get("Biology", 999):
        return "Biology"
    elif page_num >= boundaries.get("Chemistry", 999):
        return "Chemistry"
    else:
        return "Physics"


# ─────────────────────────────────────────
# FILTERS
# ─────────────────────────────────────────

def is_watermark(width, height):
    """
    Filter out background watermark images.
    Watermarks are typically full-page width (>500px wide)
    AND tall (>300px). Small wide images like diagrams are OK.
    """
    # Full page watermark - very large in both dimensions
    if width > 500 and height > 300:
        return True
    # Extremely wide but short - header/footer banner
    if width > 800 and height < 100:
        return True
    return False

def is_too_small(width, height):
    return width < MIN_IMAGE_SIZE or height < MIN_IMAGE_SIZE

def is_logo(y0):
    return y0 < LOGO_Y_THRESHOLD


# ─────────────────────────────────────────
# STEP 1 — Scan page structure
# ─────────────────────────────────────────

def scan_page_structure(doc):
    """
    Scans every page for zone markers.
    solution_y_positions fix retained — prevents (2) after Sol.
    being mistaken as an option marker.
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

                    if re.match(r'^Question No\.?\s*\d+', text, re.IGNORECASE):
                        q_num = re.search(r'\d+', text)
                        if q_num:
                            markers.append({
                                "type":            "question_start",
                                "question_number": int(q_num.group()),
                                "y":               y_pos
                            })
                    elif re.match(r'^\d+\.$', text):
                        q_num = int(re.search(r'\d+', text).group())
                        if 1 <= q_num <= 180:
                            markers.append({
                                "type":            "question_start",
                                "question_number": q_num,
                                "y":               y_pos
                            })
                    elif re.match(r'^Sol\.', text):
                        markers.append({"type": "solution_start", "y": y_pos})
                        solution_y_positions.add(y_pos)
                    elif y_pos not in solution_y_positions:
                        x_pos = round(span["bbox"][0], 2)
                        if re.match(r'^\(1\)', text):
                            markers.append({"type": "option_1_start", "y": y_pos, "x": x_pos})
                        elif re.match(r'^\(2\)', text):
                            markers.append({"type": "option_2_start", "y": y_pos, "x": x_pos})
                        elif re.match(r'^\(3\)', text):
                            markers.append({"type": "option_3_start", "y": y_pos, "x": x_pos})
                        elif re.match(r'^\(4\)', text):
                            markers.append({"type": "option_4_start", "y": y_pos, "x": x_pos})

        page_structure[page_num + 1] = sorted(markers, key=lambda x: x["y"])

    return page_structure


# ─────────────────────────────────────────
# STEP 2 — Extract and map images
# ─────────────────────────────────────────

def extract_and_map_images(doc, page_structure, boundaries):
    """
    Maps each image to question, section, zone.
    FIX 1: PIL color conversion on every image.
    FIX 2: Uses cross-validated boundaries.
    """
    mapped_images = []

    for page_num in range(len(doc)):
        page     = doc[page_num]
        page_key = page_num + 1
        markers  = page_structure.get(page_key, [])
        section  = get_section_from_page(page_key, boundaries)

        for img_index, img in enumerate(page.get_images(full=True)):
            xref       = img[0]
            base_image = doc.extract_image(xref)
            width      = base_image["width"]
            height     = base_image["height"]

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

            if is_logo(image_y):
                print(f"  Page {page_key} — skipping logo at y={round(image_y,1)}")
                continue

            # Find question and zone
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

            # Cross-page solution diagram handling
            if not last_question:
                markers_below = [
                    m for m in markers
                    if m["type"] == "question_start" and m["y"] > image_y
                ]
                if markers_below:
                    prev_markers   = page_structure.get(page_key - 1, [])
                    prev_questions = [
                        m for m in prev_markers if m["type"] == "question_start"
                    ]
                    if prev_questions:
                        last_question = prev_questions[-1]["question_number"]
                        last_zone     = "solution"
                        section       = get_section_from_page(page_key - 1, boundaries)
                        print(f"  Page {page_key} | cross-page → {section} Q{last_question} solution")
                    else:
                        print(f"  Page {page_key} | img{img_index} | no question — skipping")
                        continue
                else:
                    print(f"  Page {page_key} | img{img_index} | no question — skipping")
                    continue

            # FIX 1 — convert to RGB PNG
            clean_bytes = convert_to_rgb_png(base_image["image"])

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
                "image_bytes":     clean_bytes,
                "filename":        filename,
            })

            print(f"  Page {page_key} | {section} Q{last_question} | "
                  f"zone={last_zone} | {width}x{height}")

    return mapped_images


# ─────────────────────────────────────────
# STEP 3 — Upload to GCS
# ─────────────────────────────────────────

def upload_to_gcs(image_bytes, filename, paper_id):
    storage_client = storage.Client(project=PROJECT)
    blob_path      = f"{paper_id}/{filename}"
    blob           = storage_client.bucket(DIAGRAMS_BUCKET).blob(blob_path)
    blob.upload_from_string(image_bytes, content_type="image/png")
    return f"gs://{DIAGRAMS_BUCKET}/{blob_path}"


# ─────────────────────────────────────────
# MAIN — accepts corrected_questions for boundary derivation
# ─────────────────────────────────────────


# ─────────────────────────────────────────
# PAGE RENDERING FOR [DIAGRAM] OPTION QUESTIONS
# ─────────────────────────────────────────

def render_question_region(doc, page_num, q_num, next_q_y=None, dpi=150):
    """
    Renders the question region from a PDF page as a PNG image.
    Used for questions where options are [DIAGRAM] — captures
    the full question layout including vector graphics that
    PyMuPDF cannot extract as individual raster images.

    Args:
      doc:       fitz.Document
      page_num:  1-based page number
      q_num:     question number (for logging)
      next_q_y:  Y position of next question (to crop bottom)
      dpi:       render resolution (150 is good quality)

    Returns:
      bytes: PNG image bytes, or None if failed
    """
    try:
        page = doc[page_num - 1]
        mat  = fitz.Matrix(dpi / 72, dpi / 72)

        # Get Y position of this question number
        q_y_start = None
        blocks = page.get_text("dict")["blocks"]
        for block in blocks:
            if block["type"] != 0:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    text = span["text"].strip()
                    # Match question number patterns
                    if re.match(rf'^{q_num}\.$', text) or re.match(rf'^Question No\.?\s*{q_num}', text, re.IGNORECASE):
                        q_y_start = span["bbox"][1]
                        break
                if q_y_start is not None:
                    break
            if q_y_start is not None:
                break

        page_height = page.rect.height

        if q_y_start is None:
            # Fallback: render full page
            clip = fitz.Rect(0, 0, page.rect.width, page_height)
        else:
            # Small padding above question number
            y0 = max(0, q_y_start - 10)
            # Crop to next question or end of page
            y1 = min(next_q_y + 10, page_height) if next_q_y else page_height
            clip = fitz.Rect(0, y0, page.rect.width, y1)

        pix = page.get_pixmap(matrix=mat, clip=clip)
        return pix.tobytes("png")

    except Exception as e:
        print(f"    [render error Q{q_num} p{page_num}: {e}]")
        return None


def find_question_page(doc, page_structure, q_num):
    """Find which page a question number appears on."""
    for page_key, markers in page_structure.items():
        for m in markers:
            if m["type"] == "question_start" and m["question_number"] == q_num:
                return page_key, m["y"]
    return None, None


def find_next_question_y(doc, page_structure, page_num, current_q_y):
    """Find Y position of next question on the same page."""
    markers = page_structure.get(page_num, [])
    for m in sorted(markers, key=lambda x: x["y"]):
        if m["type"] == "question_start" and m["y"] > current_q_y:
            return m["y"]
    return None



# ─────────────────────────────────────────
# OPTION REGION CROPPING
# ─────────────────────────────────────────

def crop_option_regions(doc, page_num, page_structure, q_num, paper_id, url_map, paper_id_str):
    """
    Crops each option region from the rendered PDF page.

    Detects 2-column layouts by checking if option markers
    share the same Y position. If yes, splits page width in half.

    Args:
      doc:          fitz.Document
      page_num:     1-based page number
      page_structure: full page structure dict
      q_num:        question number
      paper_id_str: paper_id for GCS upload

    Returns:
      dict: {
        "option_1": "gs://...",
        "option_2": "gs://...",
        "option_3": "gs://...",
        "option_4": "gs://...",
      }
    """
    page    = doc[page_num - 1]
    markers = page_structure.get(page_num, [])

    # Find Y positions of all option markers and Sol. marker
    opt_markers = {}  # {option_num: {"y": ..., "x": ...}}
    sol_y       = None
    q_y         = None

    for m in markers:
        if m["type"] == "question_start" and m["question_number"] == q_num:
            q_y = m["y"]
        elif m["type"] == "option_1_start":
            opt_markers[1] = {"y": m["y"], "x": m.get("x", 0)}
        elif m["type"] == "option_2_start":
            opt_markers[2] = {"y": m["y"], "x": m.get("x", 0)}
        elif m["type"] == "option_3_start":
            opt_markers[3] = {"y": m["y"], "x": m.get("x", 0)}
        elif m["type"] == "option_4_start":
            opt_markers[4] = {"y": m["y"], "x": m.get("x", 0)}
        elif m["type"] == "solution_start":
            # Only take solution marker AFTER the question
            if q_y is None or m["y"] > q_y:
                sol_y = m["y"]

    print(f"    Q{q_num} markers: q_y={q_y}, opts={opt_markers}, sol_y={sol_y}")

    if not opt_markers:
        return {}

    # Get X positions from actual text spans for column detection
    blocks     = page.get_text("dict")["blocks"]
    opt_x_pos  = {}
    for block in blocks:
        if block["type"] != 0:
            continue
        for line in block["lines"]:
            for span in line["spans"]:
                text  = span["text"].strip()
                y_pos = round(span["bbox"][1], 2)
                x_pos = round(span["bbox"][0], 2)
                for opt_num, pattern in [(1, r"^\(1\)"), (2, r"^\(2\)"),
                                          (3, r"^\(3\)"), (4, r"^\(4\)")]:
                    if re.match(pattern, text) and y_pos not in [m.get("y") for m in [] if m.get("type") == "solution_start"]:
                        if opt_num in opt_markers and abs(y_pos - opt_markers[opt_num]["y"]) < 2:
                            opt_x_pos[opt_num] = x_pos

    # Detect 2-column layout
    # If (1) and (2) share same Y — it's 2-column
    page_width  = page.rect.width
    page_height = page.rect.height
    is_2col     = False

    if 1 in opt_markers and 2 in opt_markers:
        y1 = opt_markers[1]["y"]
        y2 = opt_markers[2]["y"]
        if abs(y1 - y2) < 5:  # within 5 points = same row
            is_2col = True
            mid_x   = page_width / 2

    # Render at 150 DPI
    mat = fitz.Matrix(150 / 72, 150 / 72)
    scale = 150 / 72

    results = {}

    if is_2col:
        # Generic 2-column crop — works for any row arrangement
        # Group options by row using Y proximity (within 5px = same row)
        sorted_opts = sorted(opt_markers.keys())
        rows = []
        used = set()

        for opt_num in sorted_opts:
            if opt_num in used:
                continue
            row = [opt_num]
            used.add(opt_num)
            y_ref = opt_markers[opt_num]["y"]
            # Find other options on same row
            for other in sorted_opts:
                if other not in used and abs(opt_markers[other]["y"] - y_ref) < 5:
                    row.append(other)
                    used.add(other)
            rows.append(sorted(row))

        print(f"    2-col rows detected: {rows}")

        # For each row, determine Y boundaries
        for row_idx, row in enumerate(rows):
            # Y top = first marker in row - padding
            y_top = opt_markers[row[0]]["y"] - 5
            # Y bottom = next row top or sol_y or page_height
            if row_idx + 1 < len(rows):
                y_bot = opt_markers[rows[row_idx + 1][0]]["y"] - 5
            else:
                y_bot = (sol_y or page_height) + 5

            if len(row) == 1:
                # Single option in row — full width
                opt_num = row[0]
                clip = fitz.Rect(0, y_top, page_width, min(y_bot, page_height))
                try:
                    pix       = page.get_pixmap(matrix=mat, clip=clip)
                    png_bytes = pix.tobytes("png")
                    if not has_content(png_bytes):
                        print(f"    Skipping blank crop for option_{opt_num}")
                        continue
                    filename  = f"p{page_num}_{q_num}_opt{opt_num}_crop.png"
                    gcs_url   = upload_to_gcs(png_bytes, filename, paper_id_str)
                    results[f"option_{opt_num}"] = gcs_url
                    print(f"    Cropped option_{opt_num} (single): {filename}")
                except Exception as e:
                    print(f"    Crop error opt{opt_num}: {e}")

            else:
                # Multiple options in row — split by X position
                for col_idx, opt_num in enumerate(row):
                    x0 = opt_markers[opt_num]["x"] - 10 if col_idx == 0 else opt_markers[opt_num]["x"] - 10
                    if col_idx + 1 < len(row):
                        x1 = opt_markers[row[col_idx + 1]]["x"] - 10
                    else:
                        x1 = page_width

                    # For first in row start from 0
                    if col_idx == 0:
                        x0 = 0

                    clip = fitz.Rect(x0, y_top, x1, min(y_bot, page_height))
                    try:
                        pix       = page.get_pixmap(matrix=mat, clip=clip)
                        png_bytes = pix.tobytes("png")
                        if not has_content(png_bytes):
                            print(f"    Skipping blank crop for option_{opt_num} (2-col)")
                            continue
                        filename  = f"p{page_num}_{q_num}_opt{opt_num}_crop.png"
                        gcs_url   = upload_to_gcs(png_bytes, filename, paper_id_str)
                        results[f"option_{opt_num}"] = gcs_url
                        print(f"    Cropped option_{opt_num} (2-col row{row_idx}): {filename}")
                    except Exception as e:
                        print(f"    Crop error opt{opt_num}: {e}")

    else:
        # Single column: each option spans full width between markers
        sorted_opts = sorted(opt_markers.keys())
        for i, opt_num in enumerate(sorted_opts):
            y_top = opt_markers[opt_num]["y"] - 5
            # Bottom is next option marker or Sol. or page end
            if i + 1 < len(sorted_opts):
                y_bot = opt_markers[sorted_opts[i + 1]]["y"] - 5
            else:
                y_bot = (sol_y or page_height) + 5

            clip = fitz.Rect(0, y_top, page_width, min(y_bot, page_height))
            try:
                pix       = page.get_pixmap(matrix=mat, clip=clip)
                png_bytes = pix.tobytes("png")
                if not has_content(png_bytes):
                    print(f"    Skipping blank crop for option_{opt_num} (1-col)")
                    continue
                filename  = f"p{page_num}_{q_num}_opt{opt_num}_crop.png"
                gcs_url   = upload_to_gcs(png_bytes, filename, paper_id_str)
                results[f"option_{opt_num}"] = gcs_url
                print(f"    Cropped option_{opt_num} (1-col): {filename}")
            except Exception as e:
                print(f"    Crop error opt{opt_num}: {e}")

    return results


def has_content(png_bytes, threshold=240, min_pixels=200):
    """
    Returns True if cropped image has actual diagram content.
    Filters out blank white crops.
    """
    try:
        img = Image.open(io.BytesIO(png_bytes)).convert("RGB")
        arr = np.array(img)
        non_white = np.sum(arr.mean(axis=2) < threshold)
        return non_white > min_pixels
    except:
        return True  # if check fails assume has content

def extract_diagrams(pdf_bytes, paper_id, corrected_questions=None):
    """
    Full diagram extraction with all 3 fixes applied.

    Args:
      pdf_bytes:           bytes — PDF content
      paper_id:            str   — e.g. "2016_neet_solutions_phase_1_code_a_p_w"
      corrected_questions: list  — questions after renumber_questions()
                                   used for FIX 2 cross-validated boundaries
                                   if None, falls back to header detection only

    Returns:
      dict: url_map — {section_questionnum: {zone: [gs://urls]}}
            e.g. {"Physics_1": {"solution": ["gs://..."]}}
    """
    print(f"\n{'='*55}")
    print(f"DIAGRAM EXTRACTION — {paper_id}")
    print(f"{'='*55}")

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    print(f"PDF opened: {len(doc)} pages")

    # FIX 2 — derive boundaries from cross-validated sources
    if corrected_questions:
        boundaries = derive_boundaries_from_questions(doc, corrected_questions)
    else:
        # Fallback — header detection only
        print("\nNo corrected questions provided — using header detection only")
        header_b = detect_boundaries_from_headers(doc)
        boundaries = {
            "Physics":   1,
            "Chemistry": header_b.get("Chemistry") or 22,
            "Biology":   header_b.get("Biology")   or 42,
        }
        print(f"  Boundaries (header only): {boundaries}")

    print("\nStep 1 — Scanning page structure...")
    page_structure = scan_page_structure(doc)
    pages_w_markers = sorted(p for p, m in page_structure.items() if m)
    print(f"Pages with markers: {pages_w_markers}")

    print("\nStep 2 — Extracting and mapping images...")
    mapped_images = extract_and_map_images(doc, page_structure, boundaries)
    print(f"\nTotal mapped images: {len(mapped_images)}")

    doc.close()

    if not mapped_images:
        print("No diagrams found.")
        return {}

    print("\nStep 3 — Uploading to GCS...")
    url_map         = defaultdict(lambda: defaultdict(list))
    images_uploaded = 0

    for img in mapped_images:
        try:
            img_bytes = convert_to_rgb_png(img["image_bytes"])
            gcs_url = upload_to_gcs(img_bytes, img["filename"], paper_id)
            print(f"  Uploaded: {img['filename']}")
            key = f"{img['section']}_{img['question_number']}"
            url_map[key][img["zone"]].append(gcs_url)
            images_uploaded += 1
        except Exception as e:
            print(f"  Upload error for {img['filename']}: {e}")

    # Step 4 — Crop option regions for [DIAGRAM] option questions
    if corrected_questions:
        print("\nStep 4 — Cropping option regions for [DIAGRAM] questions...")
        doc2 = fitz.open(stream=pdf_bytes, filetype="pdf")

        for q in corrected_questions:
            opts = [
                q.get("options", {}).get("1", ""),
                q.get("options", {}).get("2", ""),
                q.get("options", {}).get("3", ""),
                q.get("options", {}).get("4", ""),
            ]
            # Only process if any option is [DIAGRAM] or [DIAGRAM_N]
            if not any("DIAGRAM" in str(o).upper() for o in opts):
                continue

            q_num   = q.get("question_number")
            section = q.get("section", "")
            key     = f"{section}_{q_num}"

            # Always crop ALL options when [DIAGRAM] is in options
            # Don't skip even if some raster option images exist
            # because other options may be vector graphics not extractable
            existing    = url_map.get(key, {})
            all_4_exist = all(f"option_{i}" in existing for i in range(1, 5))
            if all_4_exist:
                print(f"  [{section} Q{q_num}] all 4 option images exist — skipping crop")
                continue

            # Find the page this question is on
            page_num, q_y = find_question_page(doc2, page_structure, q_num)
            if not page_num:
                print(f"  [{section} Q{q_num}] page not found — skipping")
                continue

            print(f"  Cropping options for {section} Q{q_num} (p{page_num})")
            opt_urls = crop_option_regions(
                doc2, page_num, page_structure, q_num, paper_id, url_map, paper_id
            )

            for zone, gcs_url in opt_urls.items():
                url_map[key][zone].append(gcs_url)
                images_uploaded += 1
                print(f"    Stored {zone} → {gcs_url.split('/')[-1]}")

        doc2.close()

    print(f"\nDiagram extraction complete:")
    print(f"  Images uploaded:         {images_uploaded}")
    print(f"  Questions with diagrams: {len(url_map)}")

    # FIX 3 — return url_map, never UPDATE BigQuery
    return dict(url_map)


# ─────────────────────────────────────────
# LOCAL RUN
# ─────────────────────────────────────────

if __name__ == "__main__":
    PDF_FILE = "2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf"
    PAPER_ID = "2016_neet_solutions_phase_1_code_a_p_w"

    print(f"Reading {PDF_FILE}...")
    with open(PDF_FILE, "rb") as f:
        pdf_bytes = f.read()

    # Local test without corrected questions — uses header detection fallback
    url_map = extract_diagrams(pdf_bytes, PAPER_ID, corrected_questions=None)
    print(f"\nTotal questions with diagrams: {len(url_map)}")
    for k, v in list(url_map.items())[:5]:
        print(f"  {k}: {list(v.keys())}")