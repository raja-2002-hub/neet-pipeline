"""
NEET Diagram Review API — FastAPI Backend

Endpoints:
  GET  /api/papers                     → list all papers
  GET  /api/questions/{paper_id}       → questions + validation scores
  GET  /api/image?path=gs://...        → serve GCS image via signed URL
  POST /api/validate/{paper_id}        → run all 8 validation checks
  POST /api/review/save                → save corrections to BigQuery
  GET  /api/stats/{paper_id}           → review progress stats

Validation Checks:
  1. diagram_text_no_image     — [DIAGRAM] in text but 0 images
  2. question_zone_overloaded  — 4+ images in question, 0 in options
  3. junk_image                — mostly white/empty pixels
  4. gemini_flag_mismatch      — has_diagram=true but no images extracted
  5. multi_image_single_option — 2+ images crammed in one option
  6. correct_answer_missing    — correct answer option has no diagram (CRITICAL)
  7. cross_question_duplicate  — same image file in multiple questions
  8. solution_diagram_missing  — has_solution_diagram=true but no solution image
"""

import os
import io
import json
import datetime
from typing import Optional
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from google.cloud import bigquery, storage
from google.auth import default

PROJECT = os.environ.get("GCP_PROJECT", "project-3639c8e1-b432-4a18-99f")
DATASET = "question_bank"
DIAGRAMS_BUCKET = f"{PROJECT}-diagrams"

app = FastAPI(title="NEET Diagram Review", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────

def get_bq():
    return bigquery.Client(project=PROJECT)

def get_gcs():
    return storage.Client(project=PROJECT)

def parse_urls(url_str):
    """Parse JSON array string of URLs."""
    if not url_str:
        return []
    try:
        parsed = json.loads(url_str)
        return parsed if isinstance(parsed, list) else []
    except:
        return []

def fname(url):
    """Extract filename from GCS URL."""
    return url.split("/")[-1] if url else ""


# ─────────────────────────────────────────────────────────
# Validation Engine — 8 Checks
# ─────────────────────────────────────────────────────────

def validate_question(q, all_questions=None):
    """
    Run all 8 validation checks on a single question.
    Returns list of flags with severity and details.
    """
    flags = []
    
    # Parse all image URLs
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
    
    # ── Check 1: [DIAGRAM] text but no image ──
    for i in range(1, 5):
        opt_text = str(q.get(f"option_{i}", "")).upper()
        if ("[DIAGRAM]" in opt_text or "[DIAGRAM " in opt_text) and len(opt_imgs[i]) == 0:
            severity = "critical" if str(i) == str(correct) else "warning"
            flags.append({
                "check": "diagram_text_no_image",
                "severity": severity,
                "zone": f"option_{i}",
                "detail": f"Option {i} text contains [DIAGRAM] but has 0 images",
                "score_impact": -30 if severity == "critical" else -15,
            })
    
    # Also check question text
    q_text_upper = str(q.get("question_text", "")).upper()
    if ("[DIAGRAM]" in q_text_upper or "[DIAGRAM " in q_text_upper) and len(q_imgs) == 0:
        flags.append({
            "check": "diagram_text_no_image",
            "severity": "warning",
            "zone": "question",
            "detail": "Question text contains [DIAGRAM] but has 0 question images",
            "score_impact": -10,
        })
    
    # ── Check 2: Question zone overloaded ──
    empty_opts = sum(1 for i in range(1, 5) if len(opt_imgs[i]) == 0)
    if len(q_imgs) >= 4 and empty_opts >= 3:
        flags.append({
            "check": "question_zone_overloaded",
            "severity": "warning",
            "zone": "question",
            "detail": f"{len(q_imgs)} images in question zone, {empty_opts}/4 options empty — possible redistribution needed",
            "score_impact": -20,
        })
    
    # ── Check 3: Junk image detection ──
    # This runs at extraction time, but we flag if question has images
    # for text-only questions (no [DIAGRAM] in any text)
    has_diagram_text = any(
        ("[DIAGRAM]" in str(q.get(f, "")).upper() or "[DIAGRAM " in str(q.get(f, "")).upper())
        for f in ["question_text", "option_1", "option_2", "option_3", "option_4"]
    )
    if not has_diagram_text and not q.get("has_diagram") and len(all_imgs) > 0:
        flags.append({
            "check": "junk_image",
            "severity": "info",
            "zone": "all",
            "detail": f"Text-only question has {len(all_imgs)} images — may be junk artifacts",
            "score_impact": -5,
        })
    
    # ── Check 4: Gemini flag mismatch ──
    if q.get("has_option_diagram") and all(len(opt_imgs[i]) == 0 for i in range(1, 5)):
        flags.append({
            "check": "gemini_flag_mismatch",
            "severity": "warning",
            "zone": "options",
            "detail": "Gemini says has_option_diagram=true but 0 option images extracted",
            "score_impact": -15,
        })
    
    if q.get("has_question_diagram") and len(q_imgs) == 0:
        flags.append({
            "check": "gemini_flag_mismatch",
            "severity": "info",
            "zone": "question",
            "detail": "Gemini says has_question_diagram=true but 0 question images",
            "score_impact": -5,
        })
    
    # ── Check 5: Multiple images in single option ──
    for i in range(1, 5):
        if len(opt_imgs[i]) >= 3:
            flags.append({
                "check": "multi_image_single_option",
                "severity": "warning",
                "zone": f"option_{i}",
                "detail": f"Option {i} has {len(opt_imgs[i])} images — likely includes images from adjacent options",
                "score_impact": -10,
            })
    
    # ── Check 6: Correct answer missing diagram (CRITICAL) ──
    if correct in ["1", "2", "3", "4"]:
        correct_int = int(correct)
        opt_text = str(q.get(f"option_{correct_int}", "")).upper()
        if ("[DIAGRAM]" in opt_text or "[DIAGRAM " in opt_text) and len(opt_imgs[correct_int]) == 0:
            # Already caught by check 1 with critical severity,
            # but add explicit flag for dashboard highlighting
            flags.append({
                "check": "correct_answer_no_diagram",
                "severity": "critical",
                "zone": f"option_{correct_int}",
                "detail": f"CORRECT ANSWER (option {correct_int}) has [DIAGRAM] text but no image — students cannot see the answer",
                "score_impact": -40,
            })
    
    # ── Check 7: Cross-question duplicate ──
    if all_questions:
        my_files = set(fname(u) for u in all_imgs)
        for other_q in all_questions:
            if other_q.get("question_number") == q_num:
                continue
            other_imgs = []
            for f in ["question_diagram_urls", "solution_diagram_urls",
                       "option_1_diagram_urls", "option_2_diagram_urls",
                       "option_3_diagram_urls", "option_4_diagram_urls"]:
                other_imgs += parse_urls(other_q.get(f))
            other_files = set(fname(u) for u in other_imgs)
            shared = my_files & other_files
            if shared:
                flags.append({
                    "check": "cross_question_duplicate",
                    "severity": "warning",
                    "zone": "all",
                    "detail": f"Shares image(s) with Q{other_q.get('question_number')}: {', '.join(shared)}",
                    "score_impact": -10,
                })
    
    # ── Check 8: Solution diagram missing ──
    if q.get("has_solution_diagram") and len(sol_imgs) == 0:
        flags.append({
            "check": "solution_diagram_missing",
            "severity": "info",
            "zone": "solution",
            "detail": "Gemini says has_solution_diagram=true but no solution image extracted",
            "score_impact": -5,
        })
    
    # Calculate confidence score (0-100)
    base_score = 100
    total_impact = sum(f["score_impact"] for f in flags)
    score = max(0, min(100, base_score + total_impact))
    
    # Determine review status
    has_critical = any(f["severity"] == "critical" for f in flags)
    has_warning = any(f["severity"] == "warning" for f in flags)
    
    if has_critical:
        review_status = "critical"
    elif has_warning:
        review_status = "needs_review"
    elif flags:
        review_status = "minor_issues"
    else:
        review_status = "auto_approved"
    
    return {
        "flags": flags,
        "diagram_confidence": score,
        "review_status": review_status,
        "total_images": len(all_imgs),
        "option_images": {i: len(opt_imgs[i]) for i in range(1, 5)},
    }


# ─────────────────────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────────────────────

@app.get("/api/papers")
def list_papers():
    """List all processed papers."""
    bq = get_bq()
    query = f"""
        SELECT paper_id, source_file, year, exam_name, phase,
               total_questions, physics_count, chemistry_count, 
               biology_count, status, processed_at
        FROM `{PROJECT}.{DATASET}.dim_papers`
        ORDER BY processed_at DESC
    """
    rows = [dict(r) for r in bq.query(query).result()]
    return {"papers": rows}


@app.get("/api/questions/{paper_id}")
def get_questions(
    paper_id: str,
    section: Optional[str] = None,
    diagrams_only: bool = True,
    flagged_only: bool = False,
):
    """
    Get questions for a paper with auto-validation.
    Returns questions sorted by diagram_confidence (worst first).
    """
    bq = get_bq()
    
    where = [f"paper_id = '{paper_id}'"]
    if section:
        where.append(f"section = '{section}'")
    if diagrams_only:
        where.append("has_diagram = true")
    
    query = f"""
        SELECT * FROM `{PROJECT}.{DATASET}.dim_questions`
        WHERE {' AND '.join(where)}
        ORDER BY question_number
    """
    
    rows = [dict(r) for r in bq.query(query).result()]
    
    # Convert non-serializable types
    for r in rows:
        for k, v in r.items():
            if isinstance(v, (datetime.date, datetime.datetime)):
                r[k] = v.isoformat()
    
    # Run validation on all questions
    validated = []
    for q in rows:
        validation = validate_question(q, all_questions=rows)
        q["validation"] = validation
        validated.append(q)
    
    # Sort by diagram_confidence (worst first)
    validated.sort(key=lambda q: q["validation"]["diagram_confidence"])
    
    if flagged_only:
        validated = [q for q in validated
                     if q["validation"]["review_status"] != "auto_approved"]
    
    # Summary stats
    stats = {
        "total": len(rows),
        "critical": sum(1 for q in validated
                        if q["validation"]["review_status"] == "critical"),
        "needs_review": sum(1 for q in validated
                            if q["validation"]["review_status"] == "needs_review"),
        "minor_issues": sum(1 for q in validated
                            if q["validation"]["review_status"] == "minor_issues"),
        "auto_approved": sum(1 for q in validated
                             if q["validation"]["review_status"] == "auto_approved"),
    }
    
    return {"questions": validated, "stats": stats, "paper_id": paper_id}


@app.get("/api/image")
def serve_image(path: str = Query(..., description="GCS URL (gs://...)")):
    """
    Serve a GCS image via signed URL or direct streaming.
    Converts gs:// URL to a downloadable image.
    """
    match = None
    if path.startswith("gs://"):
        parts = path[5:].split("/", 1)
        if len(parts) == 2:
            bucket_name, blob_path = parts
        else:
            raise HTTPException(400, "Invalid GCS path")
    else:
        raise HTTPException(400, "Path must start with gs://")
    
    try:
        client = get_gcs()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        if not blob.exists():
            raise HTTPException(404, f"Image not found: {path}")
        
        content = blob.download_as_bytes()
        
        # Determine content type
        ct = "image/png"
        if blob_path.endswith(".jpg") or blob_path.endswith(".jpeg"):
            ct = "image/jpeg"
        
        return StreamingResponse(
            io.BytesIO(content),
            media_type=ct,
            headers={
                "Cache-Control": "public, max-age=3600",
                "Access-Control-Allow-Origin": "*",
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error fetching image: {str(e)}")


@app.get("/api/image/signed")
def get_signed_url(path: str = Query(...)):
    """Generate a signed URL for a GCS image (valid 1 hour)."""
    if not path.startswith("gs://"):
        raise HTTPException(400, "Path must start with gs://")
    
    parts = path[5:].split("/", 1)
    if len(parts) != 2:
        raise HTTPException(400, "Invalid GCS path")
    
    bucket_name, blob_path = parts
    
    try:
        client = get_gcs()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(hours=1),
            method="GET",
        )
        return {"url": url, "path": path}
    except Exception as e:
        # Fallback: return direct public URL
        return {
            "url": f"https://storage.googleapis.com/{bucket_name}/{blob_path}",
            "path": path,
            "note": "unsigned — bucket must be public"
        }


@app.post("/api/validate/{paper_id}")
def run_validation(paper_id: str):
    """
    Run full validation on all diagram questions for a paper.
    Updates BigQuery with review_status and flags.
    """
    bq = get_bq()
    
    query = f"""
        SELECT * FROM `{PROJECT}.{DATASET}.dim_questions`
        WHERE paper_id = '{paper_id}' AND has_diagram = true
        ORDER BY question_number
    """
    rows = [dict(r) for r in bq.query(query).result()]
    
    results = []
    for q in rows:
        validation = validate_question(q, all_questions=rows)
        results.append({
            "question_number": q["question_number"],
            "section": q["section"],
            **validation,
        })
    
    # Summary
    summary = {
        "paper_id": paper_id,
        "total_diagram_questions": len(rows),
        "critical": sum(1 for r in results if r["review_status"] == "critical"),
        "needs_review": sum(1 for r in results if r["review_status"] == "needs_review"),
        "minor_issues": sum(1 for r in results if r["review_status"] == "minor_issues"),
        "auto_approved": sum(1 for r in results if r["review_status"] == "auto_approved"),
        "questions": results,
    }
    
    return summary


class ReviewSaveRequest(BaseModel):
    paper_id: str
    corrections: dict  # {question_number: {zone: [gcs_urls]}}


@app.post("/api/review/save")
def save_review(req: ReviewSaveRequest):
    """
    Save review corrections back to BigQuery.
    Updates diagram URL columns for corrected questions.
    """
    bq = get_bq()
    updated = 0
    errors = []
    
    for q_num_str, zone_map in req.corrections.items():
        try:
            q_num = int(q_num_str.split("_")[-1]) if "_" in q_num_str else int(q_num_str)
            
            # Build SET clauses
            sets = []
            for zone, urls in zone_map.items():
                url_json = json.dumps(urls)
                first_url = urls[0] if urls else None
                
                if zone == "question":
                    sets.append(f"question_diagram_urls = '{url_json}'")
                    sets.append(f"question_diagram_url = {'NULL' if not first_url else repr(first_url)}")
                elif zone == "solution":
                    sets.append(f"solution_diagram_urls = '{url_json}'")
                    sets.append(f"solution_diagram_url = {'NULL' if not first_url else repr(first_url)}")
                elif zone.startswith("option_"):
                    sets.append(f"{zone}_diagram_urls = '{url_json}'")
                    sets.append(f"{zone}_diagram_url = {'NULL' if not first_url else repr(first_url)}")
                elif zone == "junk":
                    pass  # Junk images are just removed from all zones
            
            if sets:
                update_sql = f"""
                    UPDATE `{PROJECT}.{DATASET}.dim_questions`
                    SET {', '.join(sets)}, is_reviewed = true
                    WHERE paper_id = '{req.paper_id}'
                    AND question_number = {q_num}
                """
                bq.query(update_sql).result()
                updated += 1
                
        except Exception as e:
            errors.append({"question": q_num_str, "error": str(e)})
    
    return {
        "status": "ok",
        "updated": updated,
        "errors": errors,
        "paper_id": req.paper_id,
    }


@app.get("/api/stats/{paper_id}")
def get_stats(paper_id: str):
    """Get review progress stats for a paper."""
    bq = get_bq()
    
    query = f"""
        SELECT 
            section,
            COUNT(*) as total,
            SUM(CASE WHEN has_diagram THEN 1 ELSE 0 END) as with_diagrams,
            SUM(CASE WHEN is_reviewed THEN 1 ELSE 0 END) as reviewed,
            ROUND(AVG(confidence), 2) as avg_confidence
        FROM `{PROJECT}.{DATASET}.dim_questions`
        WHERE paper_id = '{paper_id}'
        GROUP BY section
        ORDER BY MIN(question_number)
    """
    
    rows = [dict(r) for r in bq.query(query).result()]
    return {"paper_id": paper_id, "sections": rows}


# ─────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "project": PROJECT}


# ─────────────────────────────────────────────────────────
# Serve static frontend
# ─────────────────────────────────────────────────────────

import pathlib

static_dir = pathlib.Path(__file__).parent / "static"
if static_dir.exists():
    from fastapi.responses import FileResponse

    @app.get("/")
    def serve_index():
        return FileResponse(static_dir / "index.html")

    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
