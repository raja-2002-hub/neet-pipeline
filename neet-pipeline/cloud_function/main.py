import functions_framework
from google.cloud import storage, bigquery
from google import genai
import json, re, os, io
from datetime import datetime
from collections import defaultdict

from extract_diagrams import extract_diagrams
from extract_diagrams_docx import extract_diagrams_docx

PROJECT         = "project-3639c8e1-b432-4a18-99f"
DATASET         = "question_bank"
TABLE           = "dim_questions"
RAW_JSON_BUCKET = f"{PROJECT}-raw-json"
FAILED_BUCKET   = f"{PROJECT}-failed"
GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY")
GCP_PROJECT     = PROJECT
GCP_LOCATION    = "us-central1"


def paper_already_exists(paper_id, sc):
    blob = sc.bucket(RAW_JSON_BUCKET).blob(f"locks/{paper_id}.lock")
    try:
        if blob.exists():
            print(f"LOCK EXISTS: '{paper_id}'")
            return True
        blob.upload_from_string(f"locked {datetime.utcnow().isoformat()}", content_type="text/plain", if_generation_match=0)
        return False
    except:
        return True


def get_gemini_client():
    try:
        c = genai.Client(vertexai=True, project=GCP_PROJECT, location=GCP_LOCATION)
        print("Using Vertex AI Gemini")
        return c
    except Exception as e:
        print(f"Vertex AI failed ({e})")
        return genai.Client(api_key=GEMINI_API_KEY)


def clean_json_response(raw, expect_object=False):
    t = raw.strip()
    if t.startswith("```"):
        t = "\n".join(l for l in t.split("\n") if not l.strip().startswith("```")).strip()
    if expect_object:
        s = t.find("{")
        if s != -1:
            for e in range(len(t)-1, s, -1):
                if t[e] == "}":
                    try:
                        r = json.loads(t[s:e+1])
                        if isinstance(r, dict): return t[s:e+1]
                    except: continue
    if "[" in t:
        s, e = t.find("["), t.rfind("]")
        if s != -1 and e > s:
            try:
                r = json.loads(t[s:e+1])
                if isinstance(r, list): return t[s:e+1]
            except: pass
    if "{" in t:
        s, e = t.find("{"), t.rfind("}")
        if s != -1 and e > s:
            try:
                r = json.loads(t[s:e+1])
                if isinstance(r, dict):
                    for v in r.values():
                        if isinstance(v, list) and len(v) > 0: return json.dumps(v)
                    return t[s:e+1]
            except: pass
    return t


def normalise_difficulty(r):
    if not r: return "Medium"
    r = str(r).strip().lower()
    return {"easy":"Easy","medium":"Medium","moderate":"Medium","hard":"Hard","tough":"Hard","difficult":"Hard"}.get(r, "Medium")

def normalise_time(q):
    r = q.get("expected_time_seconds") or q.get("expected_time_to_solve") or q.get("expected_time") or 0
    if isinstance(r, (int, float)): return int(r)
    r = str(r).strip().lower()
    nums = re.findall(r'\d+', r)
    if not nums: return 0
    v = int(nums[0])
    return v * 60 if "min" in r else v

def normalise_answer(r):
    if not r: return ""
    r = str(r).strip().replace("(","").replace(")","")
    return {"a":"1","b":"2","c":"3","d":"4"}.get(r.lower(), r if r in ["1","2","3","4"] else r)

def clean_text(t):
    return " ".join(str(t).split()) if t else ""

def extract_paper_metadata(fn):
    ym = re.search(r'20[0-2][0-9]', fn)
    fu = fn.upper()
    return {
        "year": int(ym.group()) if ym else 0,
        "exam_name": "NEET" if "NEET" in fu else "JEE" if "JEE" in fu else "UNKNOWN",
        "phase": "Phase 2" if "PHASE-2" in fu or "PHASE 2" in fu else "Phase 1"
    }


def normalize_sections(sections):
    result = []
    for sec in sections:
        if isinstance(sec, str):
            result.append(sec)
        elif isinstance(sec, dict):
            name = sec.get("name") or sec.get("section") or sec.get("title") or ""
            if name:
                result.append(str(name))
            else:
                for v in sec.values():
                    if isinstance(v, str) and v in ["Physics", "Chemistry", "Biology"]:
                        result.append(v)
                        break
        else:
            result.append(str(sec))
    if not result:
        result = ["Physics", "Chemistry", "Biology"]
    print(f"  Normalized sections: {result}")
    return result


def renumber_questions(all_q, sections):
    print("\nRenumbering...")
    by_sec = defaultdict(list)
    for q in all_q: by_sec[q.get("section","")].append(q)
    for s in by_sec: by_sec[s].sort(key=lambda q: q.get("question_number",0))
    total_before = 0
    for sec in sections:
        if sec not in by_sec: continue
        qs = by_sec[sec]
        mn = min(q.get("question_number",0) for q in qs)
        if mn == 1 and total_before > 0:
            for i, q in enumerate(qs):
                on = q.get("question_number",0)
                nn = total_before + i + 1
                q["question_number"] = nn
                q["question_id"] = q.get("question_id","").replace(f"_{sec.lower()}_q{on}", f"_{sec.lower()}_q{nn}")
            print(f"  {sec}: renumbered to Q{total_before+1}-{total_before+len(qs)}")
        else:
            print(f"  {sec}: Q{mn}-{max(q.get('question_number',0) for q in qs)} ok")
        total_before += len(qs)
    return all_q


def attach_diagram_urls(questions, url_map):
    attached = 0
    for q in questions:
        key = f"{q.get('section','')}_{q.get('question_number',0)}"
        zones = url_map.get(key, {})
        qi = zones.get("question", [])
        q["question_diagram_urls"] = json.dumps(qi)
        q["solution_diagram_urls"] = json.dumps(zones.get("solution", []))
        for i in range(1,5):
            q[f"option_{i}_diagram_urls"] = json.dumps(zones.get(f"option_{i}", []))
        q["question_diagram_url"] = qi[0] if qi else None
        q["solution_diagram_url"] = (zones.get("solution") or [None])[0]
        for i in range(1,5):
            q[f"option_{i}_diagram_url"] = (zones.get(f"option_{i}") or [None])[0]
        if zones:
            attached += 1
            oc = sum(1 for i in range(1,5) if zones.get(f"option_{i}"))
            if oc: print(f"  {q.get('section','')} Q{q.get('question_number',0)}: {oc} option images")
    print(f"URLs attached to {attached} questions")
    return questions


def detect_pattern(pdf_bytes, client):
    print("Pass 1 — pattern detection...")
    default = {"sections":["Physics","Chemistry","Biology"],"question_number_format":"Question No. X","answer_marker":"Sol. (X)"}
    try:
        r = client.models.generate_content(model="gemini-2.5-flash", contents=[{"role":"user","parts":[
            {"text":"Analyze this NEET paper. Return ONLY JSON: {\"sections\":[\"Physics\",\"Chemistry\",\"Biology\"],\"question_number_format\":\"...\",\"answer_marker\":\"...\",\"has_diagrams\":true,\"has_formulas\":true,\"total_questions_estimate\":180}. IMPORTANT: sections must be a list of STRINGS."},
            {"inline_data":{"mime_type":"application/pdf","data":pdf_bytes}}]}])
        result = json.loads(clean_json_response(r.text, expect_object=True))
        if isinstance(result, dict) and result.get("sections"): return result
    except: pass
    return default


def clean_response_text(raw):
    t = raw.strip()
    if t.startswith("```"):
        t = "\n".join(l for l in t.split("\n") if not l.strip().startswith("```")).strip()
    s, e = t.find("["), t.rfind("]")
    if s == -1 or e <= s: s, e = t.find("{"), t.rfind("}")
    if s == -1: return t
    candidate = t[s:e+1]
    out, in_str, esc = [], False, False
    for ch in candidate:
        if esc: out.append(ch); esc = False
        elif ch == '\\': out.append(ch); esc = True
        elif ch == '"': in_str = not in_str; out.append(ch)
        elif in_str and ord(ch) < 32: out.append(' ')
        else: out.append(ch)
    return ''.join(out)


def extract_section(pdf_bytes, section, pattern, client, max_retries=2):
    print(f"Extracting {section}...")
    prompt = f"""
    Extract ALL {section} questions from this NEET question paper.
    Paper structure:
    - Question format: {pattern.get('question_number_format', 'Question No. X')}
    - Answer marker: {pattern.get('answer_marker', 'Sol. (X)')}
    - Options format: (1)(2)(3)(4)

    For EACH {section} question return a JSON object with these fields:
    {{
        "question_number": <integer>, "section": "{section}",
        "topic": "<topic>", "difficulty": "<Easy/Medium/Hard>",
        "expected_time_seconds": <integer>,
        "question_text": "<text, [DIAGRAM] if diagram>",
        "options": {{"1": "<text or [DIAGRAM]>", "2": "...", "3": "...", "4": "..."}},
        "has_question_diagram": <bool>, "has_option_diagram": <bool>,
        "has_solution_diagram": <bool>,
        "correct_answer": "<1/2/3/4>",
        "solution_text": "<solution>",
        "has_diagram": <bool>, "confidence": <0.0-1.0>
    }}
    CRITICAL: Return ONLY a JSON array. NO markdown. Keep solution_text on ONE LINE.
    """
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0: print(f"  Retry {attempt}...")
            r = client.models.generate_content(model="gemini-2.5-flash", contents=[{"role":"user","parts":[
                {"text": prompt}, {"inline_data":{"mime_type":"application/pdf","data":pdf_bytes}}]}])
            cleaned = clean_response_text(r.text)
            questions = json.loads(cleaned)
            if isinstance(questions, list) and len(questions) >= 10:
                print(f"  {len(questions)} {section} questions")
                return questions
            elif isinstance(questions, dict):
                for v in questions.values():
                    if isinstance(v, list) and len(v) >= 10: return v
            if isinstance(questions, list): print(f"  Only {len(questions)} — retrying")
        except json.JSONDecodeError as e:
            print(f"  JSON error attempt {attempt}: {e}")
        except Exception as e:
            print(f"  Error attempt {attempt}: {e}")
    return []


def load_to_bigquery(questions, paper_id, metadata):
    print(f"Loading {len(questions)} questions...")
    bq = bigquery.Client(project=PROJECT)
    rows = []
    for q in questions:
        try:
            rows.append({
                "question_id": q.get("question_id",""), "paper_id": paper_id,
                "year": metadata.get("year",0), "exam_name": metadata.get("exam_name","NEET"),
                "phase": metadata.get("phase","Phase 1"), "section": q.get("section",""),
                "question_number": int(q.get("question_number",0)),
                "question_text": clean_text(q.get("question_text","")),
                "option_1": clean_text(q.get("options",{}).get("1","")),
                "option_2": clean_text(q.get("options",{}).get("2","")),
                "option_3": clean_text(q.get("options",{}).get("3","")),
                "option_4": clean_text(q.get("options",{}).get("4","")),
                "correct_answer": normalise_answer(q.get("correct_answer","")),
                "solution": clean_text(q.get("solution_text","")),
                "subject": q.get("section",""), "topic": q.get("topic",""),
                "difficulty": normalise_difficulty(q.get("difficulty","")),
                "expected_time_seconds": normalise_time(q),
                "has_diagram": bool(q.get("has_diagram",False)),
                "has_question_diagram": bool(q.get("has_question_diagram",False)),
                "has_option_diagram": bool(q.get("has_option_diagram",False)),
                "has_solution_diagram": bool(q.get("has_solution_diagram",False)),
                "confidence": float(q.get("confidence",0.9)), "is_reviewed": False,
                **{f"{k}_diagram_url": q.get(f"{k}_diagram_url") for k in ["question","solution","option_1","option_2","option_3","option_4"]},
                **{f"{k}_diagram_urls": q.get(f"{k}_diagram_urls", json.dumps([])) for k in ["question","solution","option_1","option_2","option_3","option_4"]},
            })
        except Exception as e:
            print(f"Skip: {e}")

    from google.cloud.bigquery import LoadJobConfig, SourceFormat
    total = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        try:
            jc = LoadJobConfig(); jc.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
            jc.write_disposition = "WRITE_APPEND"; jc.autodetect = False
            job = bq.load_table_from_file(io.BytesIO("\n".join(json.dumps(r) for r in batch).encode()),
                                          f"{PROJECT}.{DATASET}.{TABLE}", job_config=jc)
            job.result()
            total += len(batch) if not job.errors else 0
        except Exception as e:
            print(f"Batch error: {e}")
    print(f"Loaded {total} rows")
    return total


def load_dim_papers(paper_id, fn, metadata, questions):
    bq = bigquery.Client(project=PROJECT)
    row = {"paper_id": paper_id, "source_file": fn, "exam_name": metadata.get("exam_name","NEET"),
           "year": metadata.get("year",0), "phase": metadata.get("phase","Phase 1"),
           "total_questions": len(questions),
           "physics_count": sum(1 for q in questions if q.get("section")=="Physics"),
           "chemistry_count": sum(1 for q in questions if q.get("section")=="Chemistry"),
           "biology_count": sum(1 for q in questions if q.get("section")=="Biology"),
           "status": "completed" if len(questions) >= 150 else "partial",
           "processed_at": datetime.utcnow().isoformat()}
    from google.cloud.bigquery import LoadJobConfig, SourceFormat
    try:
        jc = LoadJobConfig(); jc.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
        jc.write_disposition = "WRITE_APPEND"; jc.autodetect = False
        job = bq.load_table_from_file(io.BytesIO(json.dumps(row).encode()), f"{PROJECT}.{DATASET}.dim_papers", job_config=jc)
        job.result()
        print(f"dim_papers: status={row['status']}, total={row['total_questions']}")
    except Exception as e:
        print(f"dim_papers error: {e}")


def save_failed_questions(paper_id, rows):
    if not rows: return
    sc = storage.Client(project=PROJECT)
    sc.bucket(FAILED_BUCKET).blob(f"{paper_id}_failed_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json").upload_from_string(
        json.dumps({"paper_id": paper_id, "total_failed": len(rows), "failed_questions": rows}, ensure_ascii=False, indent=2))


def run_dbt_transformations(paper_id):
    print("Running dbt...")
    bq = bigquery.Client(project=PROJECT)
    P, D = PROJECT, DATASET
    for name, sql in [
        ("stg_questions", f"CREATE OR REPLACE TABLE `{P}.{D}.stg_questions` AS SELECT *, CASE WHEN question_text IS NULL OR question_text='' THEN 'missing_text' WHEN correct_answer NOT IN ('1','2','3','4') THEN 'invalid_answer' WHEN confidence<0.8 THEN 'low_confidence' ELSE 'ok' END AS quality_flag, CASE WHEN expected_time_seconds<=30 THEN 'quick' WHEN expected_time_seconds<=60 THEN 'standard' ELSE 'long' END AS time_bucket FROM `{P}.{D}.dim_questions`"),
        ("dim_questions_clean", f"CREATE OR REPLACE TABLE `{P}.{D}.dim_questions_clean` AS SELECT * FROM `{P}.{D}.stg_questions` WHERE quality_flag='ok' AND confidence>=0.8 AND question_text IS NOT NULL AND question_text!=''"),
        ("subject_summary", f"CREATE OR REPLACE TABLE `{P}.{D}.subject_summary` AS SELECT exam_name,year,section,topic,difficulty,COUNT(*) AS question_count,ROUND(AVG(confidence),2) AS avg_confidence,ROUND(AVG(expected_time_seconds),0) AS avg_time_seconds,SUM(CASE WHEN has_diagram THEN 1 ELSE 0 END) AS diagram_count FROM `{P}.{D}.dim_questions_clean` GROUP BY exam_name,year,section,topic,difficulty"),
    ]:
        try: bq.query(sql).result(); print(f"  ✓ {name}")
        except Exception as e: print(f"  ✗ {name}: {e}"); raise

    try:
        failed = [{"question_id":r.question_id,"section":r.section,"question_number":r.question_number,
                    "quality_flag":r.quality_flag,"confidence":r.confidence}
                   for r in bq.query(f"SELECT question_id,section,question_number,confidence,CASE WHEN question_text IS NULL OR question_text='' THEN 'missing_text' WHEN correct_answer NOT IN ('1','2','3','4') THEN 'invalid_answer' WHEN confidence<0.8 THEN 'low_confidence' ELSE 'ok' END AS quality_flag FROM `{P}.{D}.dim_questions` WHERE paper_id='{paper_id}' AND (question_text IS NULL OR question_text='' OR correct_answer NOT IN ('1','2','3','4') OR confidence<0.8)").result()]
        save_failed_questions(paper_id, failed)
    except Exception as e:
        print(f"  Failed export: {e}")
    print("dbt ✓")


@functions_framework.cloud_event
def process_pdf(cloud_event):
    """
    Pipeline v5 — FINAL:
      Physics:   PDF extractor only (all zones)
      Chemistry: DOCX dump (all images → bag) + PDF fallback (question + solution only)
      Biology:   DOCX zone detection only (no PDF fallback)
    """
    data = cloud_event.data
    bucket_name, file_name = data["bucket"], data["name"]
    print(f"New file: {file_name}")
    if not file_name.lower().endswith(".pdf"): return

    metadata = extract_paper_metadata(file_name)
    paper_id = re.sub(r'[^a-z0-9_]', '_', file_name.replace(".pdf","").replace(" ","_").lower())

    sc = storage.Client(project=PROJECT)
    if paper_already_exists(paper_id, sc): return

    pdf_bytes = sc.bucket(bucket_name).blob(file_name).download_as_bytes()
    print(f"PDF: {len(pdf_bytes)/1024/1024:.1f} MB")

    client = get_gemini_client()
    pattern = detect_pattern(pdf_bytes, client)
    raw_sections = pattern.get("sections", ["Physics","Chemistry","Biology"])
    sections = normalize_sections(raw_sections)

    all_q = []
    for sec in sections:
        qs = extract_section(pdf_bytes, sec, pattern, client)
        for q in qs:
            q["paper_id"] = paper_id
            q["question_id"] = f"{paper_id}_{sec.lower()}_q{q.get('question_number',0)}"
        all_q.extend(qs)
    print(f"Total: {len(all_q)} questions")

    all_q = renumber_questions(all_q, sections)

    # ══════════════════════════════════════════════════════════════
    # Step 7: FINAL DIAGRAM EXTRACTION STRATEGY
    #
    # Physics → PDF extractor for ALL zones (works well)
    #
    # Chemistry → HYBRID:
    #   - DOCX DUMP: all images per question → question bag
    #     (no zone detection, no junk filter, reviewer assigns manually)
    #   - PDF FALLBACK: question + solution zones only
    #     (clean single-image crops, no option crops to avoid junk)
    #
    # Biology → DOCX ZONE: element-order zone detection
    #   (works well for Biology, no PDF fallback needed)
    # ══════════════════════════════════════════════════════════════

    url_map = {}
    pdf_url_map = {}

    # ── Step 7A: PDF extractor ──
    # Physics: use ALL zones
    # Chemistry: use ONLY question + solution zones (no options)
    # Biology: skip entirely (DOCX handles it)
    try:
        print("\n── Step 7A: PDF extraction ──")
        pdf_url_map = extract_diagrams(pdf_bytes, paper_id,
                                       corrected_questions=all_q)

        physics_kept = 0
        chem_qs_kept = 0

        for key, zones in pdf_url_map.items():
            if key.startswith("Physics_"):
                # Physics: keep ALL zones from PDF
                url_map[key] = zones
                physics_kept += 1
            elif key.startswith("Chemistry_"):
                # Chemistry: keep ONLY question + solution from PDF
                filtered = {}
                if "question" in zones:
                    filtered["question"] = zones["question"]
                if "solution" in zones:
                    filtered["solution"] = zones["solution"]
                # DISCARD option_1, option_2, option_3, option_4 from PDF
                # (these are the junk text-contaminated crops)
                if filtered:
                    url_map[key] = filtered
                    chem_qs_kept += 1
            # Biology: skip PDF entirely

        print(f"  Physics: {physics_kept} questions (all zones)")
        print(f"  Chemistry: {chem_qs_kept} questions (question+solution only)")
        print(f"  Biology: skipped (DOCX handles it)")
        print(f"  Chemistry option crops DISCARDED (avoids junk)")
    except Exception as e:
        print(f"  PDF extraction failed: {e}")

    # ── Step 7B: DOCX extractor ──
    # Chemistry: DUMP mode — all images → question bag (for manual review)
    # Biology: ZONE mode — element-order zone detection
    docx_name = file_name.replace(".pdf", ".docx").replace(".PDF", ".docx")
    try:
        docx_blob = sc.bucket(bucket_name).blob(docx_name)
        if docx_blob.exists():
            print(f"\n── Step 7B: DOCX extraction ──")
            print(f"  DOCX found: {docx_name}")
            docx_bytes = docx_blob.download_as_bytes()
            docx_url_map = extract_diagrams_docx(docx_bytes, paper_id, all_q)

            for key, zones in docx_url_map.items():
                if key.startswith("Chemistry_"):
                    # Chemistry DOCX: merge into existing (PDF question+solution)
                    # DOCX images go to "question" bag for manual review
                    if key not in url_map:
                        url_map[key] = {}
                    for zone, urls in zones.items():
                        if zone == "question":
                            # Append DOCX question images to existing PDF question images
                            existing = url_map[key].get("question", [])
                            url_map[key]["question"] = existing + urls
                        elif zone == "solution":
                            # DOCX solution — use if PDF didn't have one
                            if "solution" not in url_map[key]:
                                url_map[key]["solution"] = urls
                        else:
                            # Any other zone from DOCX (shouldn't happen in DUMP mode)
                            url_map[key][zone] = urls

                elif key.startswith("Biology_"):
                    # Biology: use DOCX results directly (ZONE mode)
                    url_map[key] = zones

            # Summary
            chem_docx = sum(1 for k in docx_url_map if k.startswith("Chemistry_"))
            bio_docx = sum(1 for k in docx_url_map if k.startswith("Biology_"))
            print(f"\n  ── DOCX results ──")
            print(f"  Chemistry (DUMP): {chem_docx} questions → all images in question bag")
            print(f"  Biology (ZONE): {bio_docx} questions → zone-detected")

            # Detailed per-question summary for Chemistry
            for key in sorted(url_map.keys()):
                if key.startswith("Chemistry_"):
                    zones = url_map[key]
                    q_num = key.split("_")[-1]
                    q_count = len(zones.get("question", []))
                    sol_count = len(zones.get("solution", []))
                    opt_counts = [len(zones.get(f"option_{i}", [])) for i in range(1, 5)]
                    parts = []
                    if q_count: parts.append(f"bag={q_count}")
                    if sol_count: parts.append(f"sol={sol_count}")
                    if any(opt_counts): parts.append(f"opts={opt_counts}")
                    if parts:
                        print(f"  Chemistry Q{q_num}: {', '.join(parts)}")

        else:
            print(f"\n  ⚠ No DOCX found ({docx_name})")
            print(f"  Chemistry/Biology will have limited images")
    except Exception as e:
        print(f"  DOCX extraction failed: {e}")

    total_diagrams = len(url_map)
    print(f"\nFinal diagram count: {total_diagrams} questions with images")
    print(f"  Physics: {sum(1 for k in url_map if k.startswith('Physics_'))}")
    print(f"  Chemistry: {sum(1 for k in url_map if k.startswith('Chemistry_'))}")
    print(f"  Biology: {sum(1 for k in url_map if k.startswith('Biology_'))}")

    all_q = attach_diagram_urls(all_q, url_map)

    sc.bucket(RAW_JSON_BUCKET).blob(f"{paper_id}.json").upload_from_string(
        json.dumps({"paper_id":paper_id,"source_file":file_name,"year":metadata.get("year"),
                     "total_questions":len(all_q),"questions":all_q}, ensure_ascii=False))

    load_to_bigquery(all_q, paper_id, metadata)
    load_dim_papers(paper_id, file_name, metadata, all_q)
    run_dbt_transformations(paper_id)
    print(f"\n✓ Complete: {len(all_q)} questions from {file_name}")