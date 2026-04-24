import json

# Load extracted JSON
with open("neet_2016_phase1.json", "r", encoding="utf-8") as f:
    data = json.load(f)

questions = data["questions"]

# Pick specific questions to verify manually against PDF
# These are spread across sections and difficulty levels
verify_list = [
    # Physics questions
    {"id": "neet_2016_phase1_physics_q1",  "expected_answer": "2"},
    {"id": "neet_2016_phase1_physics_q4",  "expected_answer": "3"},
    {"id": "neet_2016_phase1_physics_q9",  "expected_answer": "3"},
    {"id": "neet_2016_phase1_physics_q19", "expected_answer": "1"},
    {"id": "neet_2016_phase1_physics_q45", "expected_answer": "2"},

    # Chemistry questions
    {"id": "neet_2016_phase1_chemistry_q136", "expected_answer": "2"},
    {"id": "neet_2016_phase1_chemistry_q143", "expected_answer": "2"},
    {"id": "neet_2016_phase1_chemistry_q163", "expected_answer": "3"},
    {"id": "neet_2016_phase1_chemistry_q172", "expected_answer": "1"},
    {"id": "neet_2016_phase1_chemistry_q180", "expected_answer": "1"},

    # Biology questions
    {"id": "neet_2016_phase1_biology_q1",  "expected_answer": "3"},
    {"id": "neet_2016_phase1_biology_q6",  "expected_answer": "2"},
    {"id": "neet_2016_phase1_biology_q28", "expected_answer": "1"},
    {"id": "neet_2016_phase1_biology_q67", "expected_answer": "1"},
    {"id": "neet_2016_phase1_biology_q90", "expected_answer": "2"},
]

# Build lookup dict
question_lookup = {q["question_id"]: q for q in questions}

print("=" * 60)
print("VERIFICATION REPORT — NEET 2016 Phase 1")
print("=" * 60)

correct = 0
wrong = 0
missing = 0
results = []

for check in verify_list:
    qid = check["id"]
    expected = check["expected_answer"]

    if qid not in question_lookup:
        print(f"MISSING: {qid}")
        missing += 1
        continue

    q = question_lookup[qid]
    extracted_answer = str(q.get("correct_answer", "")).strip()
    extracted_text = q.get("question_text", "")[:80]

    status = "✅ CORRECT" if extracted_answer == expected else "❌ WRONG"
    if extracted_answer == expected:
        correct += 1
    else:
        wrong += 1

    results.append({
        "id": qid,
        "status": status,
        "expected": expected,
        "extracted": extracted_answer,
        "question_preview": extracted_text
    })

# Print results
for r in results:
    print(f"\n{r['status']}")
    print(f"  ID       : {r['id']}")
    print(f"  Expected : {r['expected']}")
    print(f"  Extracted: {r['extracted']}")
    print(f"  Question : {r['question_preview']}")

print("\n" + "=" * 60)
print(f"SUMMARY")
print(f"  Correct : {correct}/15")
print(f"  Wrong   : {wrong}/15")
print(f"  Missing : {missing}/15")
print(f"  Accuracy: {(correct/15)*100:.1f}%")
print("=" * 60)