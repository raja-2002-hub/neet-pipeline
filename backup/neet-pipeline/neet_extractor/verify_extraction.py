"""
verify_extraction.py — Quick verification with HTML reconstruction

Checks:
  Step 1a — Failed bucket contents
  Step 1b — Gemini flags vs extracted images  
  Step 1c — Orphan/broken diagram URLs
  Step 2  — HTML report with all questions + embedded images

No Gemini API calls — fast and quota-free.

Run from neet_extractor folder:
  python verify_extraction.py
"""

import base64
import json
import os
from datetime import datetime

from google.cloud import bigquery
from google.cloud import storage

PROJECT         = "project-3639c8e1-b432-4a18-99f"
DATASET         = "question_bank"
DIAGRAMS_BUCKET = f"{PROJECT}-diagrams"
FAILED_BUCKET   = f"{PROJECT}-failed"
PAPER_ID        = "2016_neet_solutions_phase_1_code_a_p_w"

bq_client      = bigquery.Client(project=PROJECT)
storage_client = storage.Client(project=PROJECT)


def parse_urls(val):
    """
    Parse diagram URL column from BigQuery.
    
    BigQuery stores JSON arrays, TO_JSON_STRING returns them as:
      "[]"                          → empty
      "[\"gs://...\"]"            → escaped JSON string  
      ["gs://..."]                  → plain JSON array
    """
    if not val or val in ('null', '[]', '"[]"'):
        return []
    v = val.strip()
    # Remove outer quotes if present: "[\"...\"]]" → [\"...\""]
    if v.startswith('"') and v.endswith('"'):
        v = v[1:-1]  # strip outer quotes
    # Unescape: \" → "
    v = v.replace('\\"', '"').replace('\"', '"')
    if v.startswith('['):
        try:
            result = json.loads(v)
            if isinstance(result, list):
                return [str(x) for x in result if x]
        except:
            pass
    return []


def fetch_questions():
    print("Fetching questions from BigQuery...")
    rows = list(bq_client.query(f"""
    SELECT
        dq.question_id, dq.section, dq.question_number, dq.topic, dq.difficulty,
        dq.question_text, dq.option_1, dq.option_2, dq.option_3, dq.option_4,
        dq.correct_answer, dq.solution,
        dq.has_diagram, dq.has_question_diagram,
        dq.has_option_diagram, dq.has_solution_diagram,
        dq.confidence,
        CASE
            WHEN dq.question_text IS NULL OR dq.question_text = '' THEN 'missing_text'
            WHEN dq.correct_answer NOT IN ('1','2','3','4')         THEN 'invalid_answer'
            WHEN dq.confidence < 0.8                               THEN 'low_confidence'
            ELSE 'ok'
        END AS quality_flag,
        TO_JSON_STRING(dq.question_diagram_urls) as question_urls,
        TO_JSON_STRING(dq.solution_diagram_urls) as solution_urls,
        TO_JSON_STRING(dq.option_1_diagram_urls) as opt1_urls,
        TO_JSON_STRING(dq.option_2_diagram_urls) as opt2_urls,
        TO_JSON_STRING(dq.option_3_diagram_urls) as opt3_urls,
        TO_JSON_STRING(dq.option_4_diagram_urls) as opt4_urls
    FROM `{PROJECT}.{DATASET}.dim_questions` dq
    WHERE dq.paper_id = '{PAPER_ID}'
    ORDER BY dq.section, dq.question_number
    """).result())
    print(f"  {len(rows)} questions fetched")
    # Debug: show raw URL values for first question with diagrams
    for row in rows:
        if 'chemistry' in (row.question_id or '').lower() and row.question_number == 137:
            print(f"  DEBUG Q137 solution_urls raw: {repr(row.solution_urls)}")
            print(f"  DEBUG Q137 question_urls raw: {repr(row.question_urls)}")
            break
    return rows


def fetch_gcs_files():
    print("Fetching GCS diagram files...")
    blobs = list(storage_client.bucket(DIAGRAMS_BUCKET).list_blobs(prefix=f"{PAPER_ID}/"))
    files = {f"gs://{DIAGRAMS_BUCKET}/{b.name}" for b in blobs}
    print(f"  {len(files)} files in GCS")
    return files


def fetch_failed_bucket():
    print("Fetching failed bucket...")
    blobs = list(storage_client.bucket(FAILED_BUCKET).list_blobs(prefix=PAPER_ID))
    results = []
    for blob in blobs:
        try:
            results.append(json.loads(blob.download_as_text()))
        except:
            pass
    print(f"  {len(results)} files in failed bucket")
    return results


def img_b64(gcs_url):
    try:
        path = gcs_url.replace(f"gs://{DIAGRAMS_BUCKET}/", "")
        data = storage_client.bucket(DIAGRAMS_BUCKET).blob(path).download_as_bytes()
        return base64.b64encode(data).decode("utf-8")
    except:
        return None


def check_failed_bucket(failed_data):
    print("\n== STEP 1A: Failed bucket ==")
    details = []
    for item in failed_data:
        total   = item.get("total_failed", 0)
        summary = item.get("failure_summary", {})
        qs      = item.get("failed_questions", [])
        details.append({"total": total, "summary": summary,
                        "questions": qs, "saved_at": item.get("saved_at", "?")})
        print(f"  saved_at={item.get('saved_at')} total={total} breakdown={summary}")
        for q in qs[:5]:
            print(f"    {q.get('section')} Q{q.get('question_number')} "
                  f"[{q.get('quality_flag')}] conf={q.get('confidence',0):.2f}")
    if not details:
        print("  No files in failed bucket")
    return details


def check_flag_mismatches(questions):
    print("\n== STEP 1B: Gemini flags vs extracted images ==")
    mismatches = []
    for row in questions:
        q_urls   = parse_urls(row.question_urls)
        s_urls   = parse_urls(row.solution_urls)
        all_opt  = (parse_urls(row.opt1_urls) + parse_urls(row.opt2_urls) +
                    parse_urls(row.opt3_urls) + parse_urls(row.opt4_urls))
        all_urls = q_urls + s_urls + all_opt
        has_url  = bool(all_urls)

        if row.has_diagram and not has_url:
            mismatches.append({"type": "flagged_no_image", "section": row.section,
                               "q": row.question_number,
                               "detail": "has_diagram=True but no image extracted"})
        if has_url and not row.has_diagram:
            mismatches.append({"type": "image_not_flagged", "section": row.section,
                               "q": row.question_number,
                               "detail": f"{len(all_urls)} images found but has_diagram=False"})
        if row.has_question_diagram and not q_urls:
            mismatches.append({"type": "q_flag_no_image", "section": row.section,
                               "q": row.question_number,
                               "detail": "has_question_diagram=True but no question URL"})
        if row.has_solution_diagram and not s_urls:
            mismatches.append({"type": "sol_flag_no_image", "section": row.section,
                               "q": row.question_number,
                               "detail": "has_solution_diagram=True but no solution URL"})

    if mismatches:
        print(f"  {len(mismatches)} mismatches:")
        for m in mismatches:
            print(f"    [{m['type']}] {m['section']} Q{m['q']}: {m['detail']}")
    else:
        print("  All flags match extracted images")
    return mismatches


def check_orphans(questions, gcs_files):
    print("\n== STEP 1C: Orphan and broken URLs ==")
    all_bq = set()
    for row in questions:
        for col in [row.question_urls, row.solution_urls,
                    row.opt1_urls, row.opt2_urls, row.opt3_urls, row.opt4_urls]:
            all_bq.update(parse_urls(col))

    orphans = sorted(gcs_files - all_bq)
    broken  = sorted(all_bq - gcs_files)

    print(f"  BQ references: {len(all_bq)} | GCS files: {len(gcs_files)}")
    print(f"  Orphans (GCS only): {len(orphans)}")
    print(f"  Broken  (BQ only):  {len(broken)}")
    for url in orphans:
        print(f"    ORPHAN: {url.split('/')[-1]}")
    for url in broken:
        print(f"    BROKEN: {url.split('/')[-1]}")
    return orphans, broken


def build_q_card(row):
    q_urls  = parse_urls(row.question_urls)
    s_urls  = parse_urls(row.solution_urls)
    opt_url = [parse_urls(getattr(row, f"opt{i}_urls")) for i in range(1, 5)]
    opts    = [row.option_1, row.option_2, row.option_3, row.option_4]

    def imgs(urls, zone):
        if not urls:
            return ""
        h = f'<div style="margin:4px 0;padding:6px;background:#f0f7ff;border-radius:4px;">'
        h += f'<div style="font-size:11px;color:#185FA5;margin-bottom:3px;">[{zone}]</div>'
        for url in urls:
            b64 = img_b64(url)
            if b64:
                h += (f'<img src="data:image/png;base64,{b64}" '
                      f'style="max-width:100%;max-height:250px;border:1px solid #ccc;'
                      f'border-radius:3px;margin:2px 0;" /><br>')
            else:
                h += f'<div style="color:#E24B4A;font-size:12px;">[load error: {url.split("/")[-1]}]</div>'
        return h + '</div>'

    opts_html = ""
    for i, (opt, o_urls) in enumerate(zip(opts, opt_url), 1):
        correct   = str(i) == str(row.correct_answer)
        bg        = "#E1F5EE" if correct else "#f9f9f9"
        border    = "2px solid #1D9E75" if correct else "1px solid #e0e0e0"
        tick      = ' <strong style="color:#1D9E75;">✓ Correct</strong>' if correct else ""
        opts_html += (f'<div style="background:{bg};border:{border};border-radius:5px;'
                      f'padding:7px 11px;margin:3px 0;font-size:13px;">'
                      f'<b>({i})</b> {opt or "<em>empty</em>"}{tick}'
                      f'{imgs(o_urls, f"option {i}")}</div>')

    fc   = {"ok": "#1D9E75", "low_confidence": "#BA7517",
            "missing_text": "#E24B4A", "invalid_answer": "#E24B4A"}
    dc   = {"Easy": "#1D9E75", "Medium": "#BA7517", "Hard": "#E24B4A"}
    conf = float(row.confidence or 0)
    cc   = "#1D9E75" if conf >= 0.9 else "#BA7517" if conf >= 0.8 else "#E24B4A"
    dbadge = (f'<span style="background:{dc.get(row.difficulty,"#888")};color:#fff;'
              f'padding:1px 6px;border-radius:4px;font-size:11px;">{row.difficulty}</span>')
    diag_badge = ('<span style="background:#E6F1FB;color:#185FA5;padding:1px 6px;'
                  'border-radius:4px;font-size:11px;margin-left:4px;">diagram</span>'
                  if row.has_diagram else "")

    return f"""
    <div style="border:1px solid #e0e0e0;border-radius:8px;padding:14px;margin:10px 0;background:#fff;">
        <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
            <div>
                <b>Q{row.question_number}</b>
                <span style="color:#888;font-size:12px;margin:0 6px;">|</span>
                <span style="font-size:12px;color:#555;">{row.topic or ""}</span>
                <span style="margin-left:6px;">{dbadge}</span>{diag_badge}
            </div>
            <div style="font-size:12px;">
                <span style="color:{fc.get(row.quality_flag,'#888')};font-weight:500;">{row.quality_flag}</span>
                <span style="color:{cc};margin-left:8px;">conf:{conf:.2f}</span>
            </div>
        </div>
        <div style="font-size:13px;line-height:1.7;margin-bottom:8px;">
            {row.question_text or "<em>empty</em>"}
        </div>
        {imgs(q_urls, "question diagram")}
        <div style="margin:8px 0;">{opts_html}</div>
        <div style="margin-top:10px;padding:8px 12px;background:#f8f8f8;
                    border-left:3px solid #ddd;border-radius:0 5px 5px 0;font-size:12px;">
            <b>Solution:</b>
            {(row.solution or "")[:600]}{'...' if row.solution and len(row.solution) > 600 else ""}
            {imgs(s_urls, "solution diagram")}
        </div>
    </div>"""


def generate_report(questions, failed_details, mismatches, orphans, broken):
    print("\nGenerating HTML report (downloading images from GCS)...")

    secs  = {"Physics": [], "Chemistry": [], "Biology": []}
    for q in questions:
        secs[q.section].append(q)

    ts    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = len(questions)
    ok_n  = sum(1 for q in questions if q.quality_flag == "ok")

    cards = f"""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0;">
        <div style="background:#f5f5f5;border-radius:8px;padding:14px;text-align:center;">
            <div style="font-size:26px;font-weight:600;">{total}</div>
            <div style="color:#888;font-size:12px;">Total questions</div>
        </div>
        <div style="background:{'#E1F5EE' if ok_n==total else '#FAEEDA'};border-radius:8px;padding:14px;text-align:center;">
            <div style="font-size:26px;font-weight:600;color:#{'0F6E56' if ok_n==total else '854F0B'};">{ok_n}/{total}</div>
            <div style="font-size:12px;color:#{'0F6E56' if ok_n==total else '854F0B'};">Quality OK</div>
        </div>
        <div style="background:{'#E1F5EE' if not mismatches else '#FCEBEB'};border-radius:8px;padding:14px;text-align:center;">
            <div style="font-size:26px;font-weight:600;color:#{'0F6E56' if not mismatches else 'A32D2D'};">{len(mismatches)}</div>
            <div style="font-size:12px;color:#{'0F6E56' if not mismatches else 'A32D2D'};">Flag mismatches</div>
        </div>
        <div style="background:{'#E1F5EE' if not orphans and not broken else '#FAEEDA'};border-radius:8px;padding:14px;text-align:center;">
            <div style="font-size:26px;font-weight:600;color:#{'0F6E56' if not orphans and not broken else '854F0B'};">{len(orphans)+len(broken)}</div>
            <div style="font-size:12px;color:#{'0F6E56' if not orphans and not broken else '854F0B'};">Orphan/Broken</div>
        </div>
    </div>"""

    # Checks tab
    checks_html = "<h2>Step 1 — Instant checks</h2>"

    checks_html += "<h3>1a. Failed bucket</h3>"
    if failed_details:
        for d in failed_details:
            checks_html += (f'<div style="background:#f5f5f5;border-radius:6px;padding:10px;'
                            f'margin:6px 0;font-size:13px;">'
                            f'<b>saved_at:</b> {d["saved_at"]} &nbsp;'
                            f'<b>total:</b> {d["total"]} &nbsp;'
                            f'<b>breakdown:</b> {d["summary"]}<br>')
            for q in d["questions"][:10]:
                checks_html += (f'<div style="margin-left:12px;padding:2px 0;">'
                                f'{q.get("section")} Q{q.get("question_number")} '
                                f'[{q.get("quality_flag")}] '
                                f'conf={q.get("confidence",0):.2f} — '
                                f'{(q.get("question_text_preview") or "")[:80]}</div>')
            checks_html += '</div>'
    else:
        checks_html += '<p style="color:#1D9E75;">No failed questions</p>'

    checks_html += "<h3>1b. Flag mismatches</h3>"
    if mismatches:
        for m in mismatches:
            checks_html += (f'<div style="color:#E24B4A;font-size:13px;padding:3px 0;">'
                            f'[{m["type"]}] {m["section"]} Q{m["q"]}: {m["detail"]}</div>')
    else:
        checks_html += '<p style="color:#1D9E75;font-weight:500;">All flags match extracted images ✓</p>'

    checks_html += "<h3>1c. Orphans and broken URLs</h3>"
    if not orphans and not broken:
        checks_html += '<p style="color:#1D9E75;">No orphans or broken URLs ✓</p>'
    else:
        checks_html += f'<p>Orphans: <b>{len(orphans)}</b> &nbsp; Broken: <b>{len(broken)}</b></p>'
        for url in orphans:
            checks_html += f'<div style="color:#BA7517;font-size:12px;">ORPHAN: {url.split("/")[-1]}</div>'
        for url in broken:
            checks_html += f'<div style="color:#E24B4A;font-size:12px;">BROKEN: {url.split("/")[-1]}</div>'

    def sec_html(sec_name):
        cards_html = ""
        for i, q in enumerate(secs[sec_name], 1):
            print(f"  Building {sec_name} Q{q.question_number} ({i}/{len(secs[sec_name])})...")
            cards_html += build_q_card(q)
        return cards_html

    print("Building Physics cards...")
    ph_html = sec_html("Physics")
    print("Building Chemistry cards...")
    ch_html = sec_html("Chemistry")
    print("Building Biology cards...")
    bi_html = sec_html("Biology")

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>NEET Verification — {PAPER_ID}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     max-width:1100px;margin:0 auto;padding:20px;background:#fafafa;color:#333;}}
h1{{font-size:21px;}} 
h2{{font-size:17px;border-bottom:1px solid #eee;padding-bottom:6px;margin-top:20px;}}
h3{{font-size:14px;color:#555;margin-top:14px;}}
.tb{{padding:7px 16px;border:1px solid #ddd;background:#fff;cursor:pointer;
     border-radius:5px;margin-right:5px;margin-bottom:6px;font-size:13px;}}
.tb.on{{background:#185FA5;color:#fff;border-color:#185FA5;}}
.tab{{display:none;}}.tab.on{{display:block;}}
</style></head><body>
<h1>NEET Extraction Verification Report</h1>
<p style="color:#888;font-size:12px;">{PAPER_ID} | {ts}</p>
{cards}
<div style="margin:14px 0;">
    <button class="tb on" onclick="show('checks')">Checks</button>
    <button class="tb" onclick="show('ph')">Physics ({len(secs['Physics'])})</button>
    <button class="tb" onclick="show('ch')">Chemistry ({len(secs['Chemistry'])})</button>
    <button class="tb" onclick="show('bi')">Biology ({len(secs['Biology'])})</button>
</div>
<div id="tab-checks" class="tab on">{checks_html}</div>
<div id="tab-ph" class="tab">{ph_html}</div>
<div id="tab-ch" class="tab">{ch_html}</div>
<div id="tab-bi" class="tab">{bi_html}</div>
<script>
function show(n){{
    document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
    document.querySelectorAll('.tb').forEach(b=>b.classList.remove('on'));
    document.getElementById('tab-'+n).classList.add('on');
    event.target.classList.add('on');
}}
</script>
</body></html>"""

    with open("verification_report.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("  Saved: verification_report.html")


if __name__ == "__main__":
    print("=" * 60)
    print("NEET EXTRACTION VERIFICATION (no Gemini check)")
    print("=" * 60)

    questions   = fetch_questions()
    gcs_files   = fetch_gcs_files()
    failed_data = fetch_failed_bucket()

    failed_details  = check_failed_bucket(failed_data)
    mismatches      = check_flag_mismatches(questions)
    orphans, broken = check_orphans(questions, gcs_files)

    generate_report(questions, failed_details, mismatches, orphans, broken)

    summary = f"""
{"="*60}
VERIFICATION COMPLETE
{"="*60}
  Total questions:    {len(questions)}
  Quality OK:         {sum(1 for q in questions if q.quality_flag == "ok")}
  Flag mismatches:    {len(mismatches)}
  Orphan images:      {len(orphans)}
  Broken URLs:        {len(broken)}
  Failed bucket files:{len(failed_data)}

  Open verification_report.html in browser
{"="*60}"""

    print(summary)
    with open("verification_summary.txt", "w") as f:
        f.write(summary)