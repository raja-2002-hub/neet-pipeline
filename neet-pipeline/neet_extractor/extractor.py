from google import genai
from dotenv import load_dotenv
import os
import json
import time
import pathlib

# Load API key
load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# ─────────────────────────────────────────
# HELPER: call Gemini with retry logic
# ─────────────────────────────────────────
def call_gemini_with_retry(prompt, pdf_bytes, max_retries=3):
    """
    Calls Gemini API with automatic retry on failure.
    Waits longer between each retry (exponential backoff).
    """
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[
                    {
                        "role": "user",
                        "parts": [
                            {"text": prompt},
                            {
                                "inline_data": {
                                    "mime_type": "application/pdf",
                                    "data": pdf_bytes
                                }
                            }
                        ]
                    }
                ]
            )
            return response.text.strip()

        except Exception as e:
            wait_time = (attempt + 1) * 30  # 30s, 60s, 90s
            print(f"  Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"  Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print("  All retries exhausted.")
                raise e


# ─────────────────────────────────────────
# HELPER: clean Gemini JSON response
# ─────────────────────────────────────────
def clean_json_response(raw_text):
    text = raw_text.strip()

    if "```json" in text:
        try:
            start = text.index("```json") + 7
            end = text.index("```", start)
            candidate = text[start:end].strip()
            json.loads(candidate)
            return candidate
        except:
            pass

    if "```" in text:
        try:
            parts = text.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("[") or part.startswith("{"):
                    json.loads(part)
                    return part
        except:
            pass

    for start_char, end_char in [("[", "]"), ("{", "}")]:
        if start_char in text:
            try:
                start = text.index(start_char)
                end = text.rindex(end_char) + 1
                candidate = text[start:end]
                json.loads(candidate)
                return candidate
            except:
                pass

    return text

# ─────────────────────────────────────────
# PASS 1: Detect paper pattern
# ─────────────────────────────────────────
def detect_paper_pattern(pdf_bytes):
    """
    Pass 1 — reads the PDF and detects its structure.
    Returns a pattern dict that guides Pass 2 extraction.
    """
    print("  Running Pass 1 — detecting paper pattern...")

    prompt = """
    Analyze this NEET question paper PDF carefully.
    Look at the first 5 pages to understand the structure.
    
    Return ONLY a valid JSON object with exactly these fields:
    {
        "pattern_type": "section_based",
        "sections": ["Physics", "Chemistry", "Biology"],
        "question_number_format": "exact format used e.g. Question No. X",
        "answer_marker": "exact format used e.g. Sol. (X)",
        "metadata_fields": ["Topic", "Concept", "Subject Concept", "Question Level"],
        "options_format": "how options are labeled e.g. (1)(2)(3)(4) or (A)(B)(C)(D)",
        "has_diagrams": true,
        "has_formulas": true,
        "total_questions_estimate": 180,
        "notes": "any other important structural observations"
    }
    
    Return ONLY the JSON object. No explanation. No markdown formatting.
    """

    raw = call_gemini_with_retry(prompt, pdf_bytes)
    cleaned = clean_json_response(raw)

    try:
        pattern = json.loads(cleaned)
        print(f"  Pattern detected successfully")
        return pattern
    except json.JSONDecodeError as e:
        print(f"  Warning: Could not parse pattern JSON: {e}")
        print(f"  Raw response: {raw[:200]}")
        # Return default pattern for NEET papers
        return {
            "pattern_type": "section_based",
            "sections": ["Physics", "Chemistry", "Biology"],
            "question_number_format": "Question No. X",
            "answer_marker": "Sol. (X)",
            "has_diagrams": True,
            "has_formulas": True,
            "total_questions_estimate": 180
        }


# ─────────────────────────────────────────
# PASS 2: Extract questions by section
# ─────────────────────────────────────────
def extract_section(pdf_bytes, section_name, pattern):
    """
    Extracts all questions from one section (Physics/Chemistry/Biology).
    Processing one section at a time reduces token load and improves accuracy.
    """
    print(f"  Extracting {section_name} questions...")

    prompt = f"""
    Extract ALL {section_name} questions from this NEET question paper.
    
    Paper structure:
    - Question format: {pattern.get('question_number_format', 'Question No. X')}
    - Answer marker: {pattern.get('answer_marker', 'Sol. (X)')}
    - Options format: {pattern.get('options_format', '(1)(2)(3)(4)')}
    
    For EACH {section_name} question, return a JSON object with:
    {{
        "question_number": <integer>,
        "section": "{section_name}",
        "topic": "<topic from metadata>",
        "concept": "<concept from metadata>",
        "subject_concept": "<subject concept from metadata>",
        "difficulty": "<Easy/Medium/Hard from metadata>",
        "expected_time_seconds": <integer from metadata>,
        "question_text": "<complete question text>",
        "options": {{
            "1": "<option 1 text>",
            "2": "<option 2 text>",
            "3": "<option 3 text>",
            "4": "<option 4 text>"
        }},
        "correct_answer": "<1/2/3/4>",
        "solution_text": "<complete solution/explanation>",
        "has_diagram": <true/false>,
        "diagram_description": "<describe diagram if present, empty string if none>",
        "confidence": <0.0 to 1.0 — your confidence in this extraction>
    }}
    
    Important rules:
    - Extract ONLY {section_name} questions, skip other sections
    - Include ALL questions, do not skip any
    - For formulas, write them in plain text e.g. MR^2/2
    - If a question has a diagram, set has_diagram to true and describe it
    - Set confidence below 0.8 if you are unsure about any field
    
    Return ONLY a JSON array of all {section_name} questions.
    No explanation. No markdown. Just the JSON array.
    """

    raw = call_gemini_with_retry(prompt, pdf_bytes)
    cleaned = clean_json_response(raw)

    try:
        questions = json.loads(cleaned)
        if not isinstance(questions, list):
            print(f"  Warning: Expected list, got {type(questions)}")
            return []
        print(f"  Extracted {len(questions)} {section_name} questions")
        return questions
    except json.JSONDecodeError as e:
        print(f"  Error parsing {section_name} JSON: {e}")
        print(f"  Raw response preview: {raw[:300]}")
        return []


# ─────────────────────────────────────────
# MAIN: Process the full paper
# ─────────────────────────────────────────
def process_paper(pdf_path, paper_id):
    """
    Main pipeline function.
    Runs Pass 1 (pattern detection) then Pass 2 (extraction by section).
    Saves progress after each section so crashes don't lose work.
    """

    print(f"\n{'='*50}")
    print(f"Processing: {pdf_path}")
    print(f"Paper ID: {paper_id}")
    print(f"{'='*50}\n")

    # Read PDF once and reuse bytes
    print("Reading PDF file...")
    pdf_bytes = open(pdf_path, "rb").read()
    pdf_size_mb = len(pdf_bytes) / (1024 * 1024)
    print(f"PDF size: {pdf_size_mb:.1f} MB")

    # Pass 1 — detect pattern
    pattern = detect_paper_pattern(pdf_bytes)
    print(f"Pattern: {json.dumps(pattern, indent=2)}\n")

    # Pass 2 — extract section by section
    all_questions = []
    sections = pattern.get("sections", ["Physics", "Chemistry", "Biology"])

    for section in sections:
        print(f"\nProcessing section: {section}")

        # Small delay between sections to avoid rate limiting
        if all_questions:  # Not first section
            print("  Waiting 10 seconds before next section...")
            time.sleep(10)

        section_questions = extract_section(pdf_bytes, section, pattern)

        # Tag each question with paper_id
        for q in section_questions:
            q["paper_id"] = paper_id
            q["question_id"] = f"{paper_id}_{section.lower()}_q{q.get('question_number', 0)}"

        all_questions.extend(section_questions)

        # Save progress after each section
        progress_file = f"{paper_id}_progress.json"
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump(all_questions, f, indent=2, ensure_ascii=False)
        print(f"  Progress saved: {len(all_questions)} questions so far")

    # Calculate confidence stats
    confidences = [q.get("confidence", 0.9) for q in all_questions]
    avg_confidence = sum(confidences) / len(confidences) if confidences else 0
    low_confidence = [q for q in all_questions if q.get("confidence", 1.0) < 0.8]
    has_diagrams = [q for q in all_questions if q.get("has_diagram", False)]

    print(f"\n{'='*50}")
    print(f"EXTRACTION COMPLETE")
    print(f"{'='*50}")
    print(f"Total questions: {len(all_questions)}")
    print(f"Average confidence: {avg_confidence:.2f}")
    print(f"Low confidence questions: {len(low_confidence)}")
    print(f"Questions with diagrams: {len(has_diagrams)}")
    print(f"{'='*50}\n")

    # Build final output
    output = {
        "paper_id": paper_id,
        "source_file": pdf_path,
        "pattern": pattern,
        "total_questions": len(all_questions),
        "avg_confidence": round(avg_confidence, 3),
        "low_confidence_count": len(low_confidence),
        "diagram_count": len(has_diagrams),
        "questions": all_questions
    }

    # Save final output
    output_file = f"{paper_id}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"Final output saved: {output_file}")
    return output


# ─────────────────────────────────────────
# RUN
# ─────────────────────────────────────────
if __name__ == "__main__":
    result = process_paper(
        pdf_path="2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf",
        paper_id="neet_2016_phase1"
    )
    print(f"\nDone. {result['total_questions']} questions extracted.")
    print(f"Check file: neet_2016_phase1.json")