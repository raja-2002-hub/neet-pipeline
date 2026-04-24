from google import genai
from dotenv import load_dotenv
import os
import json

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def clean_json_response(raw_text):
    text = raw_text.strip()
    
    # Find the first [ and last ] directly
    # This is the most reliable method
    if "[" in text:
        try:
            start = text.index("[")
            end = text.rindex("]") + 1
            candidate = text[start:end]
            json.loads(candidate)
            return candidate
        except:
            pass

    # Find first { and last }
    if "{" in text:
        try:
            start = text.index("{")
            end = text.rindex("}") + 1
            candidate = text[start:end]
            json.loads(candidate)
            return candidate
        except:
            pass

    return text

# Read PDF
print("Reading PDF...")
pdf_bytes = open("2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf", "rb").read()

prompt = """
Extract ALL Physics questions from this NEET question paper.

Paper structure:
- Question format: Question No. X
- Answer marker: Sol. (X)
- Options format: (1)(2)(3)(4)

For EACH Physics question return a JSON object with:
{
    "question_number": <integer>,
    "section": "Physics",
    "topic": "<topic from metadata>",
    "concept": "<concept from metadata>",
    "subject_concept": "<subject concept from metadata>",
    "difficulty": "<Easy/Medium/Hard>",
    "expected_time_seconds": <integer>,
    "question_text": "<complete question text>",
    "options": {
        "1": "<option 1 text>",
        "2": "<option 2 text>",
        "3": "<option 3 text>",
        "4": "<option 4 text>"
    },
    "correct_answer": "<1/2/3/4>",
    "solution_text": "<complete solution>",
    "has_diagram": <true/false>,
    "diagram_description": "<describe diagram if present, empty string if none>",
    "confidence": <0.0 to 1.0>
}

Extract ONLY Physics questions.
Return ONLY a JSON array.
No markdown. No explanation.
"""

print("Calling Gemini for Physics questions...")
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

raw = response.text.strip()
cleaned = clean_json_response(raw)

try:
    physics_questions = json.loads(cleaned)
    print(f"Extracted {len(physics_questions)} Physics questions")

    # Tag each question
    for q in physics_questions:
        q["paper_id"] = "neet_2016_phase1"
        q["question_id"] = f"neet_2016_phase1_physics_q{q.get('question_number', 0)}"

    # Load already saved Chemistry + Biology
    print("Loading existing Chemistry and Biology questions...")
    with open("neet_2016_phase1_progress.json", "r") as f:
        existing = json.load(f)
    print(f"Found {len(existing)} existing questions")

    # Combine all
    all_questions = physics_questions + existing
    print(f"Total combined: {len(all_questions)} questions")

    # Save final output
    output = {
        "paper_id": "neet_2016_phase1",
        "source_file": "2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf",
        "total_questions": len(all_questions),
        "questions": all_questions
    }

    with open("neet_2016_phase1.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"Done. Final file saved: neet_2016_phase1.json")
    print(f"Total questions: {len(all_questions)}")

except json.JSONDecodeError as e:
    print(f"Parse error: {e}")
    print(f"Raw response preview: {raw[:500]}")