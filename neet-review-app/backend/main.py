"""
NEET Diagram Review API — FastAPI Backend v1.1

FIXES in v1.1:
  - save_review: Sets ALL 6 zones every time (clears empty zones)
  - save_review: Changed request format — receives question_number + zones dict
  - approve endpoint: Marks question as reviewed without URL changes
  
Previous bug: save only set zones present in corrections dict.
Zones that lost all images (moved away) kept their old URLs in BigQuery.
"""

import os
import io
import json
import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from google.cloud import bigquery, storage

PROJECT = os.environ.get("GCP_PROJECT", "project-3639c8e1-b432-4a18-99f")
DATASET = "question_bank"
DIAGRAMS_BUCKET = f"{PROJECT}-diagrams"

app = FastAPI(title="NEET Diagram Review", version="1.1")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def get_bq():
    return bigquery.Client(project=PROJECT)

def get_gcs():
    return storage.Client(project=PROJECT)

def parse_urls(url_str):
    if isinstance(url_str, list):
        return url_str
    if not url_str:
        return []
    try:
        parsed = json.loads(url_str)
        return parsed if isinstance(parsed, list) else []
    except:
        return []

def fname(url):
    return url.split("/")[-1] if url else ""


# ─────────────────────────────────────────────────────────
# Validation Engine
# ─────────────────────────────────────────────────────────

def validate_question(q, all_questions=None):
    flags = []
    q_imgs = parse_urls(q.get("question_diagram_urls"))
    sol_imgs = parse_urls(q.get("solution_diagram_urls"))
    opt_imgs = {}
    for i in range(1, 5):
        opt_imgs[i] = parse_urls(q.get(f"option_{i}_diagram_urls"))
    all_imgs = q_imgs + sol_imgs
    for i in range(1, 5):
        all_imgs += opt_imgs[i]
    correct = q.get("correct_answer", "")
    q_num = q.get("question_number", 0)

    # Check 1: [DIAGRAM] text but no image
    for i in range(1, 5):
        ot = str(q.get(f"option_{i}", "")).upper()
        if ("[DIAGRAM]" in ot or "[DIAGRAM " in ot) and len(opt_imgs[i]) == 0:
            sev = "critical" if str(i) == str(correct) else "warning"
            flags.append({"check": "diagram_text_no_image", "severity": sev, "zone": f"option_{i}",
                          "detail": f"Option {i} has [DIAGRAM] but 0 images", "score_impact": -30 if sev == "critical" else -15})

    qt = str(q.get("question_text", "")).upper()
    if ("[DIAGRAM]" in qt or "[DIAGRAM " in qt) and len(q_imgs) == 0:
        flags.append({"check": "diagram_text_no_image", "severity": "warning", "zone": "question",
                      "detail": "Question has [DIAGRAM] but 0 images", "score_impact": -10})

    # Check 2: Question overloaded
    empty_opts = sum(1 for i in range(1, 5) if len(opt_imgs[i]) == 0)
    if len(q_imgs) >= 4 and empty_opts >= 3:
        flags.append({"check": "question_zone_overloaded", "severity": "warning", "zone": "question",
                      "detail": f"{len(q_imgs)} imgs in question, {empty_opts}/4 options empty", "score_impact": -20})

    # Check 3: Junk
    has_dt = any(("[DIAGRAM]" in str(q.get(f, "")).upper()) for f in ["question_text", "option_1", "option_2", "option_3", "option_4"])
    if not has_dt and not q.get("has_diagram") and len(all_imgs) > 0:
        flags.append({"check": "junk_image", "severity": "info", "zone": "all",
                      "detail": f"Text-only question has {len(all_imgs)} images", "score_impact": -5})

    # Check 4: Gemini flag mismatch
    if q.get("has_option_diagram") and all(len(opt_imgs[i]) == 0 for i in range(1, 5)):
        flags.append({"check": "gemini_flag_mismatch", "severity": "warning", "zone": "options",
                      "detail": "has_option_diagram=true but 0 option images", "score_impact": -15})
    if q.get("has_question_diagram") and len(q_imgs) == 0:
        flags.append({"check": "gemini_flag_mismatch", "severity": "info", "zone": "question",
                      "detail": "has_question_diagram=true but 0 question images", "score_impact": -5})

    # Check 5: Multi image single option
    for i in range(1, 5):
        if len(opt_imgs[i]) >= 3:
            flags.append({"check": "multi_image_single_option", "severity": "warning", "zone": f"option_{i}",
                          "detail": f"Option {i} has {len(opt_imgs[i])} images", "score_impact": -10})

    # Check 6: Correct answer missing
    if correct in ["1", "2", "3", "4"]:
        ci = int(correct)
        ot = str(q.get(f"option_{ci}", "")).upper()
        if ("[DIAGRAM]" in ot or "[DIAGRAM " in ot) and len(opt_imgs[ci]) == 0:
            flags.append({"check": "correct_answer_no_diagram", "severity": "critical", "zone": f"option_{ci}",
                          "detail": f"CORRECT ANSWER (option {ci}) missing diagram", "score_impact": -40})

    # Check 7: Cross-question duplicate
    if all_questions:
        my_files = set(fname(u) for u in all_imgs)
        for oq in all_questions:
            if oq.get("question_number") == q_num:
                continue
            oi = []
            for f in ["question_diagram_urls", "solution_diagram_urls",
                       "option_1_diagram_urls", "option_2_diagram_urls",
                       "option_3_diagram_urls", "option_4_diagram_urls"]:
                oi += parse_urls(oq.get(f))
            shared = my_files & set(fname(u) for u in oi)
            if shared:
                flags.append({"check": "cross_question_duplicate", "severity": "warning", "zone": "all",
                              "detail": f"Shares image with Q{oq.get('question_number')}", "score_impact": -10})

    # Check 8: Solution missing
    if q.get("has_solution_diagram") and len(sol_imgs) == 0:
        flags.append({"check": "solution_diagram_missing", "severity": "info", "zone": "solution",
                      "detail": "has_solution_diagram=true but no solution image", "score_impact": -5})

    # Check 9: Multi-image zone assignment review (Chemistry/Biology only)
    # DOCX extractor can misassign images when a question has 2+ images.
    # Flag for manual zone verification via dropdown.
    if q.get("section") in ("Chemistry", "Biology") and len(all_imgs) >= 2:
        flags.append({"check": "multi_image_needs_assignment", "severity": "warning", "zone": "all",
                      "detail": f"{len(all_imgs)} images extracted — verify zone assignments via dropdown",
                      "score_impact": -15})

    score = max(0, min(100, 100 + sum(f["score_impact"] for f in flags)))
    has_crit = any(f["severity"] == "critical" for f in flags)
    has_warn = any(f["severity"] == "warning" for f in flags)
    status = "critical" if has_crit else "needs_review" if has_warn else "minor_issues" if flags else "auto_approved"

    return {"flags": flags, "diagram_confidence": score, "review_status": status,
            "total_images": len(all_imgs), "option_images": {i: len(opt_imgs[i]) for i in range(1, 5)}}


# ─────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────

@app.get("/api/papers")
def list_papers():
    bq = get_bq()
    rows = [dict(r) for r in bq.query(f"""
        SELECT paper_id, source_file, year, exam_name, phase,
               total_questions, physics_count, chemistry_count,
               biology_count, status, processed_at
        FROM `{PROJECT}.{DATASET}.dim_papers`
        ORDER BY processed_at DESC
    """).result()]
    return {"papers": rows}


@app.get("/api/questions/{paper_id}")
def get_questions(paper_id: str, section: Optional[str] = None,
                  diagrams_only: bool = True, flagged_only: bool = False):
    bq = get_bq()
    where = [f"paper_id = '{paper_id}'"]
    if section:
        where.append(f"section = '{section}'")
    if diagrams_only:
        where.append("has_diagram = true")
    rows = [dict(r) for r in bq.query(f"""
        SELECT * FROM `{PROJECT}.{DATASET}.dim_questions`
        WHERE {' AND '.join(where)} ORDER BY question_number
    """).result()]
    for r in rows:
        for k, v in r.items():
            if isinstance(v, (datetime.date, datetime.datetime)):
                r[k] = v.isoformat()
    validated = []
    for q in rows:
        q["validation"] = validate_question(q, all_questions=rows)
        validated.append(q)
    validated.sort(key=lambda q: q["validation"]["diagram_confidence"])
    if flagged_only:
        validated = [q for q in validated if q["validation"]["review_status"] != "auto_approved"]
    stats = {
        "total": len(rows),
        "critical": sum(1 for q in validated if q["validation"]["review_status"] == "critical"),
        "needs_review": sum(1 for q in validated if q["validation"]["review_status"] == "needs_review"),
        "minor_issues": sum(1 for q in validated if q["validation"]["review_status"] == "minor_issues"),
        "auto_approved": sum(1 for q in validated if q["validation"]["review_status"] == "auto_approved"),
    }
    return {"questions": validated, "stats": stats, "paper_id": paper_id}


@app.get("/api/image")
def serve_image(path: str = Query(...)):
    if not path.startswith("gs://"):
        raise HTTPException(400, "Path must start with gs://")
    parts = path[5:].split("/", 1)
    if len(parts) != 2:
        raise HTTPException(400, "Invalid GCS path")
    bucket_name, blob_path = parts
    try:
        blob = get_gcs().bucket(bucket_name).blob(blob_path)
        if not blob.exists():
            raise HTTPException(404, f"Not found: {path}")
        ct = "image/jpeg" if blob_path.endswith((".jpg", ".jpeg")) else "image/png"
        return StreamingResponse(io.BytesIO(blob.download_as_bytes()), media_type=ct,
                                 headers={"Cache-Control": "public, max-age=3600"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


# ─────────────────────────────────────────────────────────
# Serve PDF from input-papers bucket
# Used by split-screen PDF viewer in frontend
# ─────────────────────────────────────────────────────────

INPUT_PAPERS_BUCKET = f"{PROJECT}-input-papers"


@app.get("/api/pdf/{paper_id}")
def serve_pdf(paper_id: str):
    """
    Stream the source PDF for a paper so the frontend can display it
    in an iframe alongside the question review panel.

    Looks up `source_file` from dim_papers, fetches from input-papers bucket.
    """
    bq = get_bq()
    try:
        rows = list(bq.query(f"""
            SELECT source_file FROM `{PROJECT}.{DATASET}.dim_papers`
            WHERE paper_id = '{paper_id}' LIMIT 1
        """).result())
    except Exception as e:
        raise HTTPException(500, f"BigQuery error: {str(e)}")

    if not rows:
        raise HTTPException(404, f"Paper not found: {paper_id}")

    source_file = rows[0].source_file
    if not source_file:
        raise HTTPException(404, "source_file missing in dim_papers")

    try:
        blob = get_gcs().bucket(INPUT_PAPERS_BUCKET).blob(source_file)
        if not blob.exists():
            raise HTTPException(404, f"PDF not found: gs://{INPUT_PAPERS_BUCKET}/{source_file}")
        return StreamingResponse(
            io.BytesIO(blob.download_as_bytes()),
            media_type="application/pdf",
            headers={
                "Cache-Control": "public, max-age=3600",
                "Content-Disposition": f'inline; filename="{source_file}"',
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"PDF fetch error: {str(e)}")


# ─────────────────────────────────────────────────────────
# Render clean PDF page image (watermark removed via PyMuPDF)
# Used by crop tool — PyMuPDF can skip watermark objects
# ─────────────────────────────────────────────────────────

# Cache PDF bytes to avoid re-downloading for each page
_pdf_cache = {}


def _get_pdf_bytes(paper_id):
    """Get PDF bytes from GCS with caching."""
    if paper_id in _pdf_cache:
        return _pdf_cache[paper_id]
    bq = get_bq()
    rows = list(bq.query(f"""
        SELECT source_file FROM `{PROJECT}.{DATASET}.dim_papers`
        WHERE paper_id = '{paper_id}' LIMIT 1
    """).result())
    if not rows:
        return None
    source_file = rows[0].source_file
    blob = get_gcs().bucket(INPUT_PAPERS_BUCKET).blob(source_file)
    if not blob.exists():
        return None
    pdf_bytes = blob.download_as_bytes()
    _pdf_cache[paper_id] = pdf_bytes
    return pdf_bytes


@app.get("/api/pdf/{paper_id}/page/{page_num}")
def render_clean_page(paper_id: str, page_num: int, dpi: int = 150, clean: bool = True):
    """
    Render a single PDF page as PNG using PyMuPDF.
    
    If clean=True (default): removes watermark image by replacing it
    with a white pixel, keeping all small diagram images intact.
    If clean=False: renders page as-is with watermark.
    """
    try:
        import fitz
    except ImportError as e:
        raise HTTPException(500, f"PyMuPDF not installed: {e}")

    pdf_bytes = _get_pdf_bytes(paper_id)
    if not pdf_bytes:
        raise HTTPException(404, f"PDF not found for paper: {paper_id}")

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if page_num < 1 or page_num > len(doc):
            doc.close()
            raise HTTPException(400, f"Page {page_num} out of range (1-{len(doc)})")

        page = doc[page_num - 1]

        if clean:
            # Same logic as extract_diagrams.py:
            # Identify watermark by size (>500x300) and replace with white
            # Small diagram images (<500x300) are untouched
            for img in page.get_images(full=True):
                xref = img[0]
                try:
                    bi = doc.extract_image(xref)
                    w, h = bi["width"], bi["height"]
                    # Only remove LARGE images (watermark)
                    # Keep small images (diagrams: typically 50-400px)
                    if (w > 500 and h > 300) or (w > 800 and h < 100):
                        # Replace with 1x1 white pixel
                        white_pix = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 1, 1), 1)
                        white_pix.clear_with(255)
                        page.replace_image(xref, pixmap=white_pix)
                except:
                    pass

            # Also remove gray-filled vector shapes (watermark overlay)
            # Same as extract_diagrams.py find_content_bbox filter
            try:
                page.clean_contents()
            except:
                pass

        # Render the page
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat)
        png_bytes = pix.tobytes("png")

        doc.close()

        return StreamingResponse(
            io.BytesIO(png_bytes),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=3600"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Page render error: {str(e)}")


@app.get("/api/pdf/{paper_id}/page/{page_num}/debug")
def debug_page_images(paper_id: str, page_num: int):
    """Debug: list all images on a page with their sizes."""
    try:
        import fitz
    except ImportError:
        raise HTTPException(500, "PyMuPDF not installed")

    pdf_bytes = _get_pdf_bytes(paper_id)
    if not pdf_bytes:
        raise HTTPException(404, "PDF not found")

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[page_num - 1]
    images = []
    for img in page.get_images(full=True):
        xref = img[0]
        try:
            bi = doc.extract_image(xref)
            w, h = bi["width"], bi["height"]
            rects = page.get_image_rects(xref)
            rect_info = [{"x0": r.x0, "y0": r.y0, "x1": r.x1, "y1": r.y1} for r in rects]
            is_watermark = (w > 500 and h > 300) or (w > 800 and h < 100)
            images.append({
                "xref": xref, "width": w, "height": h,
                "is_watermark": is_watermark,
                "rects": rect_info
            })
        except:
            pass
    doc.close()
    return {"page": page_num, "total_images": len(images), "images": images}


@app.post("/api/validate/{paper_id}")
def run_validation(paper_id: str):
    bq = get_bq()
    rows = [dict(r) for r in bq.query(f"""
        SELECT * FROM `{PROJECT}.{DATASET}.dim_questions`
        WHERE paper_id = '{paper_id}' AND has_diagram = true ORDER BY question_number
    """).result()]
    results = [{"question_number": q["question_number"], "section": q["section"],
                **validate_question(q, all_questions=rows)} for q in rows]
    return {
        "paper_id": paper_id, "total_diagram_questions": len(rows),
        "critical": sum(1 for r in results if r["review_status"] == "critical"),
        "needs_review": sum(1 for r in results if r["review_status"] == "needs_review"),
        "auto_approved": sum(1 for r in results if r["review_status"] == "auto_approved"),
        "questions": results,
    }


# ─────────────────────────────────────────────────────────
# SAVE REVIEW — v1.1 FIX
#
# Sets ALL 6 zone columns every time.
# Zones with no images get set to '[]'.
# This fixes the bug where moved-away zones kept old URLs.
# ─────────────────────────────────────────────────────────

class ReviewSaveRequest(BaseModel):
    paper_id: str
    question_number: int
    zones: dict  # {"question": ["gs://..."], "option_1": [...], ...}


@app.post("/api/review/save")
def save_review(req: ReviewSaveRequest):
    """Save zone reassignments to BigQuery. Sets ALL 6 zones."""
    bq = get_bq()
    ALL_ZONES = ["question", "option_1", "option_2", "option_3", "option_4", "solution"]

    sets = []
    for zone in ALL_ZONES:
        urls = req.zones.get(zone, [])
        url_json = json.dumps(urls)
        first_url = urls[0] if urls else None

        if zone == "question":
            col = "question"
        elif zone == "solution":
            col = "solution"
        else:
            col = zone  # option_1, option_2, etc.

        # Use PARSE_JSON for JSON-type columns in BigQuery
        sets.append(f"{col}_diagram_urls = PARSE_JSON('{url_json}')")
        if first_url:
            # Escape single quotes in URLs
            safe_url = first_url.replace("'", "\\'")
            sets.append(f"{col}_diagram_url = '{safe_url}'")
        else:
            sets.append(f"{col}_diagram_url = NULL")

    sets.append("is_reviewed = true")

    sql = f"""
        UPDATE `{PROJECT}.{DATASET}.dim_questions`
        SET {', '.join(sets)}
        WHERE paper_id = '{req.paper_id}'
        AND question_number = {req.question_number}
    """

    try:
        bq.query(sql).result()
        return {
            "status": "ok",
            "question_number": req.question_number,
            "zones_saved": {z: len(req.zones.get(z, [])) for z in ALL_ZONES},
        }
    except Exception as e:
        raise HTTPException(500, f"BigQuery error: {str(e)}")


# ─────────────────────────────────────────────────────────
# APPROVE — mark reviewed without changing URLs
# ─────────────────────────────────────────────────────────

class ApproveRequest(BaseModel):
    paper_id: str
    question_number: int


@app.post("/api/review/approve")
def approve_question(req: ApproveRequest):
    """Approve question as-is (no URL changes, just is_reviewed=true)."""
    bq = get_bq()
    try:
        bq.query(f"""
            UPDATE `{PROJECT}.{DATASET}.dim_questions`
            SET is_reviewed = true
            WHERE paper_id = '{req.paper_id}'
            AND question_number = {req.question_number}
        """).result()
        return {"status": "ok", "question_number": req.question_number}
    except Exception as e:
        raise HTTPException(500, f"BigQuery error: {str(e)}")


# ─────────────────────────────────────────────────────────
# IMAGE UPLOAD — for manual crop from PDF
# Receives base64 image, uploads to GCS, returns GCS URL
# ─────────────────────────────────────────────────────────

import base64
import uuid


class ImageUploadRequest(BaseModel):
    paper_id: str
    question_number: int
    image_data: str  # base64 encoded PNG
    filename: str = ""  # optional custom filename


@app.post("/api/image/upload")
def upload_image(req: ImageUploadRequest):
    """
    Upload a manually cropped image to GCS.
    Receives base64 PNG, uploads to diagrams bucket, returns GCS URL.
    Applies watermark removal (gray pixels → white) for clean images.
    """
    try:
        # Decode base64 — handle both raw and data-URL formats
        img_data = req.image_data
        if "," in img_data:
            img_data = img_data.split(",", 1)[1]
        img_bytes = base64.b64decode(img_data)

        if len(img_bytes) < 100:
            raise HTTPException(400, "Image too small")

        # Normalize image size — all manual crops to max 400px width
        # This ensures consistent sizing in the dashboard regardless of crop area
        try:
            from PIL import Image as PILImage
            img = PILImage.open(io.BytesIO(img_bytes)).convert("RGB")
            w, h = img.size
            MAX_W = 400
            if w > MAX_W:
                ratio = MAX_W / w
                img = img.resize((MAX_W, int(h * ratio)), PILImage.LANCZOS)
            out = io.BytesIO()
            img.save(out, format="PNG", optimize=True)
            img_bytes = out.getvalue()
        except Exception as e:
            print(f"Image normalize skipped: {e}")

        # Generate filename
        if req.filename:
            fn = req.filename
        else:
            uid = uuid.uuid4().hex[:8]
            fn = f"q{req.question_number}_manual_crop_{uid}.png"

        # Upload to GCS
        gcs = get_gcs()
        blob_path = f"{req.paper_id}/{fn}"
        blob = gcs.bucket(DIAGRAMS_BUCKET).blob(blob_path)
        blob.upload_from_string(img_bytes, content_type="image/png")

        gcs_url = f"gs://{DIAGRAMS_BUCKET}/{blob_path}"

        return {
            "status": "ok",
            "gcs_url": gcs_url,
            "filename": fn,
            "size": len(img_bytes),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Upload error: {str(e)}")


@app.get("/api/stats/{paper_id}")
def get_stats(paper_id: str):
    bq = get_bq()
    rows = [dict(r) for r in bq.query(f"""
        SELECT section, COUNT(*) as total,
            SUM(CASE WHEN has_diagram THEN 1 ELSE 0 END) as with_diagrams,
            SUM(CASE WHEN is_reviewed THEN 1 ELSE 0 END) as reviewed,
            ROUND(AVG(confidence), 2) as avg_confidence
        FROM `{PROJECT}.{DATASET}.dim_questions`
        WHERE paper_id = '{paper_id}'
        GROUP BY section ORDER BY MIN(question_number)
    """).result()]
    return {"paper_id": paper_id, "sections": rows}


@app.get("/api/health")
def health():
    return {"status": "ok", "project": PROJECT, "version": "1.1"}


# ─────────────────────────────────────────────────────────
# Static frontend
# ─────────────────────────────────────────────────────────

import pathlib
static_dir = pathlib.Path(__file__).parent / "static"
if static_dir.exists():
    @app.get("/")
    def serve_index():
        return FileResponse(static_dir / "index.html")
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")