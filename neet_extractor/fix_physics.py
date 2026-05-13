from google import genai
from dotenv import load_dotenv
import os
import json

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

pdf_bytes = open("2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf", "rb").read()

prompt = """
You are extracting Physics questions from a NEET 2016 question paper.

Physics section has questions numbered 1 to 45.
Each question has this structure in the paper:
- Question No. X (header)
- Single Correct Answer
- Topic, Concept, Subject Concept, Question Level, Expected time metadata
- The question text starting with the number
- Options labeled (1) (2) (3) (4)
- Sol. (X) giving the correct answer
- Solution explanation

Return a JSON array where each element is an object with EXACTLY these fields:
[
  {
    "question_number": 1,
    "section": "Physics",
    "topic": "topic name from metadata",
    "concept": "concept from metadata",
    "subject_concept": "subject concept from metadata",
    "difficulty": "Easy or Medium or Hard from Question Level",
    "expected_time_seconds": 45,
    "question_text": "complete question text here",
    "options": {
      "1": "option 1 text",
      "2": "option 2 text",
      "3": "option 3 text",
      "4": "option 4 text"
    },
    "correct_answer": "2",
    "solution_text": "complete solution text",
    "has_diagram": false,
    "diagram_description": "",
    "confidence": 0.95
  }
]

Rules:
- Extract ALL 45 Physics questions
- Each element MUST be a JSON object with those exact fields
- correct_answer must be just the number "1" "2" "3" or "4"
- has_diagram is true if the question or solution contains a figure or diagram
- confidence is your confidence 0.0 to 1.0 that you extracted correctly
- Return ONLY the JSON array starting with [ and ending with ]
- No markdown, no explanation, no code blocks
"""

print("Calling Gemini for Physics questions...")
response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents=[
        {
            "role": "user",
            "parts": [
                {"text": prompt},
                {"inline_data": {"mime_type": "application/pdf", "data": pdf_bytes}}
            ]
        }
    ]
)

raw = response.text.strip()
print(f"Response length: {len(raw)}")
print(f"First 100 chars: {repr(raw[:100])}")
print(f"Last 50 chars: {repr(raw[-50:])}")

# Parse it
start = raw.find("[")
end = raw.rfind("]")
if start != -1 and end != -1:
    clean = raw[start:end+1]
    try:
        questions = json.loads(clean)
        print(f"\nSuccessfully parsed {len(questions)} questions")
        print(f"First question: {questions[0]['question_text'][:80]}")
        print(f"Type of first element: {type(questions[0])}")
        
        # Tag and save
        for q in questions:
            q["paper_id"] = "neet_2016_phase1"
            q["question_id"] = f"neet_2016_phase1_physics_q{q.get('question_number', 0)}"

        # Load existing 135 questions
        with open("neet_2016_phase1_progress.json", "r", encoding="utf-8") as f:
            
            existing = json.load(f)

        all_questions = questions + existing
        print(f"Total combined: {len(all_questions)} questions")

        output = {
            "paper_id": "neet_2016_phase1",
            "source_file": "2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf",
            "total_questions": len(all_questions),
            "questions": all_questions
        }

        with open("neet_2016_phase1.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        print(f"Saved to neet_2016_phase1.json")

    except json.JSONDecodeError as e:
        print(f"Parse error: {e}")
        print(f"Problem area: {clean[max(0,e.pos-50):e.pos+50]}")
else:
    print("Could not find JSON array in response")
    print(raw[:300])