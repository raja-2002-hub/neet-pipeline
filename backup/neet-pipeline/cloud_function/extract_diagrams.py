"""
extract_diagrams.py — Complete diagram extraction with 7 fixes

FIX 1: PIL color conversion (CMYK/inverted → RGB)
FIX 2: Cross-validated section boundaries
FIX 3: No BigQuery UPDATE (returns url_map)
FIX 4: Question-scoped marker filtering
FIX 5: Strict zone detection (upper+lower bounds)
FIX 6: Smart crop with content bounding box
FIX 7: Watermark removal (gray threshold)
"""

import fitz
import io
import re
import json
from collections import defaultdict

import numpy as np
from PIL import Image
from google.cloud import storage

PROJECT          = "project-3639c8e1-b432-4a18-99f"
DIAGRAMS_BUCKET  = f"{PROJECT}-diagrams"
MIN_IMAGE_SIZE   = 50
LOGO_Y_THRESHOLD = 60


# ═══════════════════════════════════════════
# FIX 1 — PIL color conversion
# ═══════════════════════════════════════════

def convert_to_rgb_png(image_bytes):
    """Converts CMYK/RGBA/inverted images to clean RGB PNG."""
    try:
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode == "CMYK":
            img = img.convert("RGB")
        elif img.mode == "RGBA":
            bg = Image.new("RGB", img.size, (255, 255, 255))
            bg.paste(img, mask=img.split()[3])
            img = bg
        elif img.mode != "RGB":
            img = img.convert("RGB")
        arr = np.array(img)
        if arr.mean() < 80:
            img = Image.fromarray(255 - arr)
            print(f"    [color fix: inverted, brightness {arr.mean():.0f}]")
        out = io.BytesIO()
        img.save(out, format="PNG", optimize=True)
        return out.getvalue()
    except Exception as e:
        print(f"    [color fix failed: {e}]")
        return image_bytes


# ═══════════════════════════════════════════
# FIX 7 — Watermark removal
# ═══════════════════════════════════════════

def remove_watermark(png_bytes, threshold=180):
    """
    Removes gray watermark from PDF page renders.
    Any pixel where ALL RGB channels > threshold → pure white.
    NEET watermark is ~RGB(200,200,200), content is RGB(<100).
    """
    try:
        img = Image.open(io.BytesIO(png_bytes)).convert("RGB")
        arr = np.array(img)
        mask = np.all(arr > threshold, axis=2)
        arr[mask] = [255, 255, 255]
        out = io.BytesIO()
        Image.fromarray(arr).save(out, format="PNG", optimize=True)
        return out.getvalue()
    except:
        return png_bytes


def has_content(png_bytes, threshold=240, min_dark_pixels=200):
    """Returns True if image has actual content (not blank white)."""
    try:
        img = Image.open(io.BytesIO(png_bytes)).convert("RGB")
        arr = np.array(img)
        return np.sum(arr.mean(axis=2) < threshold) > min_dark_pixels
    except:
        return True


# ═══════════════════════════════════════════
# FIX 2 — Cross-validated section boundaries
# ═══════════════════════════════════════════

def derive_boundaries_from_questions(doc, corrected_questions):
    """Derives section page boundaries from question positions + headers."""
    print("\nDeriving section boundaries...")
    section_first_q = {}
    by_section = defaultdict(list)
    for q in corrected_questions:
        by_section[q.get("section", "")].append(q.get("question_number", 0))
    for s, nums in by_section.items():
        if nums:
            section_first_q[s] = min(nums)

    header_b = detect_boundaries_from_headers(doc)
    boundaries = {"Physics": 1}

    for section in ["Chemistry", "Biology"]:
        if header_b.get(section):
            boundaries[section] = header_b[section]
        else:
            prev = "Physics" if section == "Chemistry" else "Chemistry"
            first_q = section_first_q.get(section, 1)
            for pn in range(boundaries.get(prev, 1), len(doc)):
                found = False
                for block in doc[pn].get_text("dict")["blocks"]:
                    if block["type"] != 0:
                        continue
                    for line in block["lines"]:
                        for span in line["spans"]:
                            t = span["text"].strip()
                            if (re.match(rf'^Question No\.?\s*{first_q}', t, re.I)
                                    or re.match(rf'^{first_q}\.$', t)):
                                boundaries[section] = pn + 1
                                found = True
                                break
                        if found: break
                    if found: break
                if found: break
            if section not in boundaries:
                boundaries[section] = {"Chemistry": 34, "Biology": 53}.get(section, 1)

    print(f"  Boundaries: {boundaries}")
    return boundaries


def detect_boundaries_from_headers(doc):
    """Header-based boundary detection (Chemistry/Biology text near top of page)."""
    chem_p = re.compile(r'\b(CHEMISTRY|Chemistry)\b', re.I)
    bio_p = re.compile(r'\b(BIOLOGY|Biology)\b', re.I)
    cs, bs = None, None
    for pn in range(min(len(doc), 80)):
        page = doc[pn]
        text = page.get_text()
        pk = pn + 1
        if cs is None and chem_p.search(text):
            for b in page.get_text("dict")["blocks"]:
                if b["type"] != 0: continue
                for l in b["lines"]:
                    for s in l["spans"]:
                        if chem_p.search(s["text"].strip()) and s["bbox"][1] < 120 and len(s["text"].strip()) < 40:
                            cs = pk; break
                    if cs: break
                if cs: break
        if cs and bs is None and bio_p.search(text):
            for b in page.get_text("dict")["blocks"]:
                if b["type"] != 0: continue
                for l in b["lines"]:
                    for s in l["spans"]:
                        if bio_p.search(s["text"].strip()) and s["bbox"][1] < 120 and len(s["text"].strip()) < 40:
                            bs = pk; break
                    if bs: break
                if bs: break
    return {"Chemistry": cs, "Biology": bs}


def get_section_from_page(pn, boundaries):
    if pn >= boundaries.get("Biology", 999): return "Biology"
    if pn >= boundaries.get("Chemistry", 999): return "Chemistry"
    return "Physics"


# ═══════════════════════════════════════════
# STEP 1 — Scan page structure
# ═══════════════════════════════════════════

def scan_page_structure(doc):
    """Scans every page for question/option/solution markers with Y and X positions."""
    page_structure = {}
    for pn in range(len(doc)):
        page = doc[pn]
        markers = []
        sol_ys = set()
        for block in page.get_text("dict")["blocks"]:
            if block["type"] != 0:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    t = span["text"].strip()
                    y = round(span["bbox"][1], 2)
                    x = round(span["bbox"][0], 2)

                    if re.match(r'^Question No\.?\s*\d+', t, re.I):
                        qn = re.search(r'\d+', t)
                        if qn:
                            markers.append({"type": "question_start",
                                            "question_number": int(qn.group()), "y": y})
                    elif re.match(r'^\d+\.$', t):
                        qn = int(re.search(r'\d+', t).group())
                        if 1 <= qn <= 200:
                            markers.append({"type": "question_start",
                                            "question_number": qn, "y": y})
                    elif re.match(r'^Sol\.', t):
                        markers.append({"type": "solution_start", "y": y})
                        sol_ys.add(y)
                    elif y not in sol_ys:
                        for i, pat in [(1, r'^\(1\)'), (2, r'^\(2\)'),
                                       (3, r'^\(3\)'), (4, r'^\(4\)')]:
                            if re.match(pat, t):
                                markers.append({"type": f"option_{i}_start",
                                                "y": y, "x": x})

        page_structure[pn + 1] = sorted(markers, key=lambda m: m["y"])
    return page_structure


# ═══════════════════════════════════════════
# FIX 4 — Question-scoped marker filtering
# ═══════════════════════════════════════════

def get_question_bounds(all_markers, target_q_num):
    """
    Returns the full set of markers for ONE specific question.
    Filters to only markers between this question's Y and its Sol. Y.
    Prevents markers from adjacent questions being used.

    Returns:
        q_y:         Y position of question number
        opt1_y:      Y position of (1) marker
        sol_y:       Y position of Sol. marker
        next_q_y:    Y position of next question number
        opt_markers: {1: {"y":..., "x":...}, 2: {...}, ...}
    """
    q_y = None
    for m in all_markers:
        if m["type"] == "question_start" and m.get("question_number") == target_q_num:
            q_y = m["y"]
            break
    if q_y is None:
        return None, None, None, None, {}

    # Find Sol. Y — first solution marker AFTER this question
    sol_y = None
    for m in all_markers:
        if m["type"] == "solution_start" and m["y"] > q_y:
            sol_y = m["y"]
            break

    # Find next question Y — first question marker AFTER Sol.
    next_q_y = None
    search_after = sol_y if sol_y else q_y
    for m in all_markers:
        if m["type"] == "question_start" and m["y"] > search_after:
            next_q_y = m["y"]
            break

    # Find (1) marker — first option_1 AFTER question
    opt1_y = None
    for m in all_markers:
        if m["type"] == "option_1_start" and m["y"] > q_y:
            if sol_y is None or m["y"] < sol_y:
                opt1_y = m["y"]
                break

    # Filter option markers: only between q_y and sol_y
    opt_markers = {}
    for m in all_markers:
        if m["y"] <= q_y:
            continue
        if sol_y and m["y"] >= sol_y:
            continue
        for i in range(1, 5):
            if m["type"] == f"option_{i}_start":
                opt_markers[i] = {"y": m["y"], "x": m.get("x", 0)}

    return q_y, opt1_y, sol_y, next_q_y, opt_markers


# ═══════════════════════════════════════════
# FIX 5 — Strict zone detection
# ═══════════════════════════════════════════

def determine_zone_strict(image_y, image_x, q_y, opt1_y, sol_y, next_q_y,
                          opt_markers, page_width=600):
    """
    Determines which zone an image belongs to using strict boundary rules.

    Zones:
      question: q_y → opt1_y
      option_N: opt1_y → sol_y (sub-determined by Y/X)
      solution: sol_y → next_q_y

    Uses BOTH upper and lower bounds — not just "what's above."
    Handles 2-column option layouts using X position.
    """
    # Rule 1: Before first option → QUESTION
    if opt1_y and image_y < opt1_y:
        return "question"

    # Rule 2: Between first option and Sol → OPTION
    if opt1_y and sol_y and opt1_y <= image_y < sol_y:
        if not opt_markers:
            return "option_1"

        # Detect 2-column layout
        is_2col = False
        if 1 in opt_markers and 2 in opt_markers:
            if abs(opt_markers[1]["y"] - opt_markers[2]["y"]) < 5:
                is_2col = True

        if is_2col:
            # Find which row
            row1_y = opt_markers.get(1, {}).get("y", 0)
            row2_y = opt_markers.get(3, {}).get("y", 9999)
            mid_x = page_width / 2

            if image_y < row2_y - 5:
                return "option_1" if image_x < mid_x else "option_2"
            else:
                return "option_3" if image_x < mid_x else "option_4"
        else:
            # Single column — find last option marker before image
            best = 1
            for opt_num in sorted(opt_markers.keys()):
                if opt_markers[opt_num]["y"] <= image_y:
                    best = opt_num
            return f"option_{best}"

    # Rule 3: After Sol and before next question → SOLUTION
    if sol_y and image_y >= sol_y:
        if next_q_y is None or image_y < next_q_y:
            return "solution"

    # Fallback
    if opt1_y and image_y < opt1_y:
        return "question"
    return "question"


# ═══════════════════════════════════════════
# STEP 2 — Extract and map RASTER images
# ═══════════════════════════════════════════

def extract_and_map_images(doc, page_structure, boundaries):
    """
    Extracts raster images (PNG/JPEG embedded in PDF).
    Uses FIX 5 strict zone detection for correct assignment.
    """
    mapped = []
    for pn in range(len(doc)):
        page = doc[pn]
        pk = pn + 1
        all_markers = page_structure.get(pk, [])
        section = get_section_from_page(pk, boundaries)

        # Build per-question bounds for this page
        q_nums_on_page = [m["question_number"] for m in all_markers
                          if m["type"] == "question_start"]

        for idx, img in enumerate(page.get_images(full=True)):
            xref = img[0]
            bi = doc.extract_image(xref)
            w, h = bi["width"], bi["height"]

            # Filter watermarks, logos, tiny images
            if (w > 500 and h > 300) or (w > 800 and h < 100):
                continue
            if w < MIN_IMAGE_SIZE or h < MIN_IMAGE_SIZE:
                continue
            rects = page.get_image_rects(xref)
            if not rects:
                continue
            iy = rects[0].y0
            ix = rects[0].x0
            if iy < LOGO_Y_THRESHOLD:
                continue

            # FIX 5: Find which question this image belongs to
            # Try each question on this page
            assigned = False
            for qn in q_nums_on_page:
                q_y, opt1_y, sol_y, next_q_y, opt_m = get_question_bounds(
                    all_markers, qn)
                if q_y is None:
                    continue

                # Check if image is within this question's range
                upper = q_y
                lower = next_q_y if next_q_y else 9999

                if upper <= iy < lower:
                    zone = determine_zone_strict(
                        iy, ix, q_y, opt1_y, sol_y, next_q_y,
                        opt_m, page.rect.width)

                    clean = convert_to_rgb_png(bi["image"])
                    fn = f"p{pk}_{section.lower()}_q{qn}_{zone}_{idx}.png"
                    mapped.append({
                        "page": pk, "question_number": qn,
                        "section": section, "zone": zone,
                        "image_bytes": clean, "filename": fn,
                    })
                    print(f"  Page {pk} | {section} Q{qn} | zone={zone} | {w}x{h}")
                    assigned = True
                    break

            if not assigned:
                # Cross-page: image before first question → previous page's last question
                if q_nums_on_page and iy < all_markers[0].get("y", 9999):
                    prev_markers = page_structure.get(pk - 1, [])
                    prev_qs = [m for m in prev_markers if m["type"] == "question_start"]
                    if prev_qs:
                        last_q = prev_qs[-1]["question_number"]
                        prev_section = get_section_from_page(pk - 1, boundaries)
                        clean = convert_to_rgb_png(bi["image"])
                        fn = f"p{pk}_{prev_section.lower()}_q{last_q}_solution_{idx}.png"
                        mapped.append({
                            "page": pk, "question_number": last_q,
                            "section": prev_section, "zone": "solution",
                            "image_bytes": clean, "filename": fn,
                        })
                        print(f"  Page {pk} | cross-page → {prev_section} Q{last_q} solution | {w}x{h}")

    return mapped


# ═══════════════════════════════════════════
# STEP 3 — Upload to GCS
# ═══════════════════════════════════════════

def upload_to_gcs(image_bytes, filename, paper_id):
    sc = storage.Client(project=PROJECT)
    bp = f"{paper_id}/{filename}"
    sc.bucket(DIAGRAMS_BUCKET).blob(bp).upload_from_string(
        image_bytes, content_type="image/png")
    return f"gs://{DIAGRAMS_BUCKET}/{bp}"


# ═══════════════════════════════════════════
# FIX 6 — Smart crop with content bounding box
# ═══════════════════════════════════════════

def find_content_bbox(page, y_top, y_bot, x_left=0, x_right=None):
    """
    Finds the actual bounding box of content (drawings + text)
    within a Y range. Handles tall structures that extend
    above/below the option marker positions.

    Returns (actual_y_top, actual_y_bot) or None if no content.
    """
    if x_right is None:
        x_right = page.rect.width

    min_y = y_bot
    max_y = y_top

    # Check vector drawings
    try:
        for d in page.get_drawings():
            rect = d["rect"]
            # Skip watermark (gray fill)
            fill = d.get("fill")
            if fill and all(c > 0.7 for c in fill[:3]):
                continue
            # Check if drawing overlaps our region
            if rect.y1 > y_top - 30 and rect.y0 < y_bot + 30:
                if rect.x0 >= x_left - 10 and rect.x1 <= x_right + 10:
                    min_y = min(min_y, rect.y0)
                    max_y = max(max_y, rect.y1)
    except:
        pass

    # Check text blocks
    for block in page.get_text("dict")["blocks"]:
        if block["type"] != 0:
            continue
        for line in block["lines"]:
            for span in line["spans"]:
                sy = span["bbox"][1]
                sx = span["bbox"][0]
                text = span["text"].strip()
                # Skip option markers and Sol. markers
                if re.match(r'^\(\d\)$', text) or re.match(r'^Sol\.', text):
                    continue
                if sy > y_top - 30 and sy < y_bot + 30:
                    if sx >= x_left - 10 and sx <= x_right + 10:
                        min_y = min(min_y, span["bbox"][1])
                        max_y = max(max_y, span["bbox"][3])

    if max_y <= min_y:
        return None

    return (min_y, max_y)


# ═══════════════════════════════════════════
# STEP 4 — Crop option regions (vector graphics)
# ═══════════════════════════════════════════

def crop_option_regions(doc, page_num, page_structure, q_num, paper_id):
    """
    Crops each option region from the PDF page.
    Captures vector graphics by rendering the page region.

    Uses:
      FIX 4: Question-scoped markers
      FIX 5: Strict zone boundaries
      FIX 6: Smart content bounding box
      FIX 7: Watermark removal
    """
    page = doc[page_num - 1]
    all_markers = page_structure.get(page_num, [])
    page_width = page.rect.width
    page_height = page.rect.height

    # FIX 4: Get only THIS question's markers
    q_y, opt1_y, sol_y, next_q_y, opt_markers = get_question_bounds(
        all_markers, q_num)

    if not opt_markers:
        print(f"    Q{q_num}: no option markers on page {page_num}")
        return {}

    print(f"    Q{q_num}: q_y={q_y}, opt1_y={opt1_y}, sol_y={sol_y}, "
          f"next_q_y={next_q_y}, opts={list(opt_markers.keys())}")

    # Detect 2-column layout
    is_2col = False
    if 1 in opt_markers and 2 in opt_markers:
        if abs(opt_markers[1]["y"] - opt_markers[2]["y"]) < 5:
            is_2col = True

    mat = fitz.Matrix(150 / 72, 150 / 72)  # 150 DPI
    results = {}

    if is_2col:
        # Group options by row
        sorted_opts = sorted(opt_markers.keys())
        rows = []
        used = set()
        for opt_num in sorted_opts:
            if opt_num in used:
                continue
            row = [opt_num]
            used.add(opt_num)
            for other in sorted_opts:
                if other not in used and abs(opt_markers[other]["y"] - opt_markers[opt_num]["y"]) < 5:
                    row.append(other)
                    used.add(other)
            rows.append(sorted(row))

        print(f"    2-col rows: {rows}")

        for row_idx, row in enumerate(rows):
            # Y boundaries for this row
            marker_y_top = opt_markers[row[0]]["y"]
            if row_idx + 1 < len(rows):
                marker_y_bot = opt_markers[rows[row_idx + 1][0]]["y"]
            else:
                marker_y_bot = sol_y if sol_y else page_height

            for col_idx, opt_num in enumerate(row):
                # X boundaries
                if col_idx == 0:
                    x_left = 0
                else:
                    x_left = opt_markers[opt_num]["x"] - 15
                if col_idx + 1 < len(row):
                    x_right = opt_markers[row[col_idx + 1]]["x"] - 15
                else:
                    x_right = page_width

                # FIX 6: Find actual content extent
                bbox = find_content_bbox(page, marker_y_top, marker_y_bot,
                                         x_left, x_right)
                if bbox:
                    y_top = max(0, bbox[0] - 8)
                    y_bot = min(page_height, bbox[1] + 8)
                else:
                    y_top = max(0, marker_y_top - 15)
                    y_bot = min(page_height, marker_y_bot)

                clip = fitz.Rect(x_left, y_top, x_right, y_bot)
                try:
                    pix = page.get_pixmap(matrix=mat, clip=clip)
                    png_bytes = pix.tobytes("png")
                    # FIX 7: Remove watermark
                    png_bytes = remove_watermark(png_bytes)
                    if not has_content(png_bytes):
                        continue
                    fn = f"p{page_num}_{q_num}_opt{opt_num}_crop.png"
                    gcs_url = upload_to_gcs(png_bytes, fn, paper_id)
                    results[f"option_{opt_num}"] = gcs_url
                    print(f"    Cropped opt{opt_num} (2-col): {fn}")
                except Exception as e:
                    print(f"    Crop error opt{opt_num}: {e}")
    else:
        # Single column
        sorted_opts = sorted(opt_markers.keys())
        for i, opt_num in enumerate(sorted_opts):
            marker_y_top = opt_markers[opt_num]["y"]
            if i + 1 < len(sorted_opts):
                marker_y_bot = opt_markers[sorted_opts[i + 1]]["y"]
            else:
                marker_y_bot = sol_y if sol_y else page_height

            # FIX 6: Find actual content extent
            bbox = find_content_bbox(page, marker_y_top, marker_y_bot)
            if bbox:
                y_top = max(0, bbox[0] - 8)
                y_bot = min(page_height, bbox[1] + 8)
            else:
                y_top = max(0, marker_y_top - 15)
                y_bot = min(page_height, marker_y_bot)

            clip = fitz.Rect(0, y_top, page_width, y_bot)
            try:
                pix = page.get_pixmap(matrix=mat, clip=clip)
                png_bytes = pix.tobytes("png")
                # FIX 7: Remove watermark
                png_bytes = remove_watermark(png_bytes)
                if not has_content(png_bytes):
                    continue
                fn = f"p{page_num}_{q_num}_opt{opt_num}_crop.png"
                gcs_url = upload_to_gcs(png_bytes, fn, paper_id)
                results[f"option_{opt_num}"] = gcs_url
                print(f"    Cropped opt{opt_num} (1-col): {fn}")
            except Exception as e:
                print(f"    Crop error opt{opt_num}: {e}")

    return results


# ═══════════════════════════════════════════
# MAIN — Full extraction pipeline
# ═══════════════════════════════════════════

def extract_diagrams(pdf_bytes, paper_id, corrected_questions=None):
    """
    Complete diagram extraction:
      Step 1: Scan page structure (markers)
      Step 2: Extract raster images (FIX 5 strict zones)
      Step 3: Upload raster images
      Step 4: Crop option regions for [DIAGRAM] questions
              (FIX 4 + FIX 6 + FIX 7)
    """
    print(f"\n{'='*55}")
    print(f"DIAGRAM EXTRACTION — {paper_id}")
    print(f"{'='*55}")

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    print(f"PDF: {len(doc)} pages")

    # FIX 2: Cross-validated boundaries
    if corrected_questions:
        boundaries = derive_boundaries_from_questions(doc, corrected_questions)
    else:
        hb = detect_boundaries_from_headers(doc)
        boundaries = {"Physics": 1,
                       "Chemistry": hb.get("Chemistry") or 22,
                       "Biology": hb.get("Biology") or 42}

    # Step 1
    print("\nStep 1 — Scanning page structure...")
    page_structure = scan_page_structure(doc)
    pages_with_markers = sorted(p for p, m in page_structure.items() if m)
    print(f"  Pages with markers: {pages_with_markers[:20]}...")

    # Step 2 — Raster images with strict zone detection
    print("\nStep 2 — Extracting raster images (strict zones)...")
    mapped = extract_and_map_images(doc, page_structure, boundaries)
    print(f"  Total raster images: {len(mapped)}")

    doc.close()

    # Step 3 — Upload raster images
    print("\nStep 3 — Uploading raster images...")
    url_map = defaultdict(lambda: defaultdict(list))
    uploaded = 0
    for img in mapped:
        try:
            clean = convert_to_rgb_png(img["image_bytes"])
            gcs_url = upload_to_gcs(clean, img["filename"], paper_id)
            key = f"{img['section']}_{img['question_number']}"
            url_map[key][img["zone"]].append(gcs_url)
            uploaded += 1
        except Exception as e:
            print(f"  Upload error: {img['filename']}: {e}")

    # Step 4 — Crop option regions for [DIAGRAM] questions
    if corrected_questions:
        print("\nStep 4 — Cropping option regions for [DIAGRAM] questions...")
        doc2 = fitz.open(stream=pdf_bytes, filetype="pdf")

        for q in corrected_questions:
            opts = [q.get("options", {}).get(str(i), "") for i in range(1, 5)]
            if not any("[DIAGRAM]" in str(o).upper() for o in opts):
                continue

            q_num = q.get("question_number")
            section = q.get("section", "")
            key = f"{section}_{q_num}"

            # Skip if ALL 4 options already have images from raster extraction
            existing = url_map.get(key, {})
            if all(f"option_{i}" in existing for i in range(1, 5)):
                print(f"  {section} Q{q_num}: all 4 options have raster images — skip crop")
                continue

            # Find which page this question is on
            page_num = None
            for pk, markers in page_structure.items():
                for m in markers:
                    if m["type"] == "question_start" and m.get("question_number") == q_num:
                        page_num = pk
                        break
                if page_num:
                    break
            if not page_num:
                print(f"  {section} Q{q_num}: page not found — skip")
                continue

            print(f"  Cropping {section} Q{q_num} (page {page_num})...")
            opt_urls = crop_option_regions(
                doc2, page_num, page_structure, q_num, paper_id)

            for zone, gcs_url in opt_urls.items():
                # Only add if this option doesn't already have a raster image
                if zone not in existing:
                    url_map[key][zone].append(gcs_url)
                    uploaded += 1

        doc2.close()

    print(f"\nExtraction complete:")
    print(f"  Total uploaded: {uploaded}")
    print(f"  Questions with diagrams: {len(url_map)}")

    return dict(url_map)


# ═══════════════════════════════════════════
# LOCAL TEST
# ═══════════════════════════════════════════

if __name__ == "__main__":
    PDF_FILE = "2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf"
    PAPER_ID = "2016_neet_solutions_phase_1_code_a_p_w"

    with open(PDF_FILE, "rb") as f:
        pdf_bytes = f.read()
    url_map = extract_diagrams(pdf_bytes, PAPER_ID)
    print(f"\nQuestions with diagrams: {len(url_map)}")
    for k, v in list(url_map.items())[:10]:
        print(f"  {k}: {list(v.keys())}")