import fitz  # PyMuPDF
import os
import re
import json
from collections import defaultdict
from google.cloud import storage
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# Config
PROJECT = "project-3639c8e1-b432-4a18-99f"
DIAGRAMS_BUCKET = f"{PROJECT}-diagrams"
PDF_FILE = "2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf"
PAPER_ID = "2016_neet_solutions_phase_1_code_a_p_w"
MIN_IMAGE_SIZE = 50
LOGO_Y_THRESHOLD = 60


# ─────────────────────────────────────────
# SECTION PAGE MAPPING
# ─────────────────────────────────────────

def get_section_from_page(page_num):
    if page_num <= 33:
        return "Physics"
    elif page_num <= 52:
        return "Chemistry"
    else:
        return "Biology"


# ─────────────────────────────────────────
# FILTERS
# ─────────────────────────────────────────

def is_watermark(width, height):
    return width > 500 or height > 400

def is_too_small(width, height):
    return width < MIN_IMAGE_SIZE or height < MIN_IMAGE_SIZE

def is_logo(y0):
    return y0 < LOGO_Y_THRESHOLD


# ─────────────────────────────────────────
# STEP 1 — Scan page structure
# ─────────────────────────────────────────

def scan_page_structure(pdf_path):
    """
    Scans every page and finds all zone markers.
    Key fix: tracks Sol. y positions so answer
    numbers like (2) after Sol. are not mistaken
    for option markers.
    """
    doc = fitz.open(pdf_path)
    page_structure = {}

    for page_num in range(len(doc)):
        page = doc[page_num]
        blocks = page.get_text("dict")["blocks"]
        markers = []
        solution_y_positions = set()

        for block in blocks:
            if block["type"] != 0:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    text = span["text"].strip()
                    y_pos = round(span["bbox"][1], 2)

                    # Format 1 — "Question No. 1"
                    if re.match(
                        r'^Question No\.?\s*\d+',
                        text, re.IGNORECASE
                    ):
                        q_num = re.search(r'\d+', text)
                        if q_num:
                            markers.append({
                                "type": "question_start",
                                "question_number": int(q_num.group()),
                                "y": y_pos
                            })

                    # Format 2 — "2." (number dot alone)
                    elif re.match(r'^\d+\.$', text):
                        q_num = int(re.search(r'\d+', text).group())
                        if 1 <= q_num <= 180:
                            markers.append({
                                "type": "question_start",
                                "question_number": q_num,
                                "y": y_pos
                            })

                    # Solution marker — checked BEFORE options
                    elif re.match(r'^Sol\.', text):
                        markers.append({
                            "type": "solution_start",
                            "y": y_pos
                        })
                        solution_y_positions.add(y_pos)

                    # Options — only if NOT on same line as Sol.
                    elif y_pos not in solution_y_positions:

                        if re.match(r'^\(1\)', text):
                            markers.append({
                                "type": "option_1_start",
                                "y": y_pos
                            })

                        elif re.match(r'^\(2\)', text):
                            markers.append({
                                "type": "option_2_start",
                                "y": y_pos
                            })

                        elif re.match(r'^\(3\)', text):
                            markers.append({
                                "type": "option_3_start",
                                "y": y_pos
                            })

                        elif re.match(r'^\(4\)', text):
                            markers.append({
                                "type": "option_4_start",
                                "y": y_pos
                            })

        page_structure[page_num + 1] = sorted(
            markers, key=lambda x: x["y"]
        )

    doc.close()
    return page_structure


# ─────────────────────────────────────────
# STEP 2 — Extract and map images
# ─────────────────────────────────────────

def extract_and_map_images(pdf_path, page_structure):
    """
    Extracts all valid images and maps each to:
    - Which question it belongs to
    - Which section (Physics/Chemistry/Biology)
    - Which zone: question/option_1/option_2/
                  option_3/option_4/solution
    Handles cross-page solution diagrams correctly.
    """
    doc = fitz.open(pdf_path)
    mapped_images = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        page_key = page_num + 1
        markers = page_structure.get(page_key, [])
        section = get_section_from_page(page_key)

        image_list = page.get_images(full=True)

        for img_index, img in enumerate(image_list):
            xref = img[0]
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]
            width = base_image["width"]
            height = base_image["height"]

            if is_watermark(width, height):
                print(f"  Page {page_key} — skipping background "
                      f"({width}x{height})")
                continue
            if is_too_small(width, height):
                continue

            img_rects = page.get_image_rects(xref)
            if not img_rects:
                continue

            rect = img_rects[0]
            image_y = rect.y0

            if is_logo(image_y):
                print(f"  Page {page_key} — skipping logo "
                      f"at y0={round(image_y,2)}")
                continue

            # Find which question and zone
            last_question = None
            last_zone = "question"

            for marker in markers:
                if marker["y"] > image_y:
                    break
                mtype = marker["type"]
                if mtype == "question_start":
                    last_question = marker["question_number"]
                    last_zone = "question"
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

            # Handle cross-page solution diagrams
            if not last_question:
                markers_below = [
                    m for m in markers
                    if m["type"] == "question_start"
                    and m["y"] > image_y
                ]
                if markers_below:
                    # Image is above first question on page
                    # belongs to previous page last question solution
                    prev_markers = page_structure.get(
                        page_key - 1, []
                    )
                    prev_questions = [
                        m for m in prev_markers
                        if m["type"] == "question_start"
                    ]
                    if prev_questions:
                        last_question = prev_questions[-1][
                            "question_number"
                        ]
                        last_zone = "solution"
                        section = get_section_from_page(page_key - 1)
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
                "image_bytes":     image_bytes,
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

    doc.close()
    return mapped_images


# ─────────────────────────────────────────
# STEP 3 — Upload to GCS
# ─────────────────────────────────────────

def upload_to_gcs(image_bytes, filename, paper_id):
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.bucket(DIAGRAMS_BUCKET)
    blob_path = f"{paper_id}/{filename}"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        image_bytes, content_type="image/png"
    )
    return f"gs://{DIAGRAMS_BUCKET}/{blob_path}"


# ─────────────────────────────────────────
# STEP 4 — Update BigQuery with JSON arrays
# ─────────────────────────────────────────

def update_bigquery_batch(url_map):
    """
    Updates BigQuery with JSON arrays of URLs.
    Handles multiple images per zone correctly.
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

    for key, zones in url_map.items():
        parts = key.rsplit("_", 1)
        section = parts[0]
        question_number = int(parts[1])

        set_clauses = []
        for zone, urls in zones.items():
            column = zone_to_column.get(zone)
            if column:
                urls_json = json.dumps(urls)
                set_clauses.append(
                    f"{column} = JSON '{urls_json}'"
                )

        if not set_clauses:
            continue

        query = f"""
        UPDATE `{PROJECT}.question_bank.dim_questions`
        SET {', '.join(set_clauses)}
        WHERE paper_id = '{PAPER_ID}'
        AND question_number = {question_number}
        AND section = '{section}'
        """

        try:
            bq_client.query(query).result()
            print(
                f"  Updated {section} Q{question_number} "
                f"→ zones: {list(zones.keys())}"
            )
        except Exception as e:
            print(
                f"  Error updating {section} "
                f"Q{question_number}: {e}"
            )


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

print("=" * 55)
print("DIAGRAM EXTRACTION AND MAPPING — NEET 2016")
print("=" * 55)

# Step 1 — scan page structure
print("\nStep 1 — Scanning page structure...")
page_structure = scan_page_structure(PDF_FILE)

pages_with_markers = {
    p: m for p, m in page_structure.items() if m
}
print(f"Pages with markers: {sorted(pages_with_markers.keys())}")

for page, markers in sorted(pages_with_markers.items()):
    q_markers = [
        m for m in markers
        if m["type"] == "question_start"
    ]
    section = get_section_from_page(page)
    print(
        f"  Page {page} ({section}): "
        f"{len(q_markers)} questions "
        f"{len(markers)} total markers"
    )

# Step 2 — extract and map images
print("\nStep 2 — Extracting and mapping images...")
mapped_images = extract_and_map_images(PDF_FILE, page_structure)
print(f"\nTotal mapped images: {len(mapped_images)}")

# Step 3 — save locally to verify
print("\nStep 3 — Saving locally to verify...")
os.makedirs("mapped_images", exist_ok=True)

# Clear old images
for f in os.listdir("mapped_images"):
    os.remove(os.path.join("mapped_images", f))

for img in mapped_images:
    filepath = os.path.join("mapped_images", img["filename"])
    with open(filepath, "wb") as f:
        f.write(img["image_bytes"])
print(f"Saved {len(mapped_images)} images to mapped_images/")

# Step 4 — print summary
print("\nMapped images summary:")
for img in mapped_images:
    print(
        f"  {img['section']} Q{img['question_number']} | "
        f"zone={img['zone']} | "
        f"{img['width']}x{img['height']} | "
        f"{img['filename']}"
    )

# Step 5 — confirm before uploading
print(
    f"\nReady to upload {len(mapped_images)} images "
    f"to GCS and update BigQuery."
)
print("Do you want to proceed? (yes/no)")
confirm = input().strip().lower()

if confirm in ["yes", "y"]:
    print("\nStep 5 — Uploading to GCS...")

    url_map = defaultdict(lambda: defaultdict(list))

    for img in mapped_images:
        try:
            gcs_url = upload_to_gcs(
                img["image_bytes"],
                img["filename"],
                PAPER_ID
            )
            print(f"  Uploaded: {img['filename']}")

            key = f"{img['section']}_{img['question_number']}"
            url_map[key][img["zone"]].append(gcs_url)

        except Exception as e:
            print(f"  Upload error for {img['filename']}: {e}")

    print("\nStep 6 — Updating BigQuery with JSON arrays...")
    update_bigquery_batch(url_map)

    print(f"\nDone.")
    print(f"Total images uploaded : {len(mapped_images)}")
    print(f"Total questions updated: {len(url_map)}")

else:
    print("Skipped. Check mapped_images/ folder to verify.")