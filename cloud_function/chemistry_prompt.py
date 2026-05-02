"""
chemistry_prompt.py

Section-specific Gemini prompts.
Chemistry gets SMILES-aware prompt → outputs SMILES for structures.
Physics/Biology get standard prompt → outputs [DIAGRAM] for diagrams.
"""

CHEMISTRY_PROMPT = """
Extract ALL Chemistry questions from this NEET question paper.

Paper structure:
- Question format: {question_format}
- Answer marker: {answer_marker}
- Options format: (1)(2)(3)(4)

For EACH Chemistry question return a JSON object with EXACTLY these fields:
{{
    "question_number": <integer>,
    "section": "Chemistry",
    "topic": "<topic name>",
    "concept": "<concept name>",
    "subject_concept": "<subject concept>",
    "difficulty": "<Easy/Medium/Hard>",
    "expected_time_seconds": <integer>,
    "question_text": "<complete question text>",
    "options": {{
        "1": <option - see format rules below>,
        "2": <option - see format rules below>,
        "3": <option - see format rules below>,
        "4": <option - see format rules below>
    }},
    "has_question_diagram": <true/false>,
    "has_option_diagram": <true/false>,
    "has_solution_diagram": <true/false>,
    "option_diagrams": {{
        "1": <true/false>, "2": <true/false>,
        "3": <true/false>, "4": <true/false>
    }},
    "correct_answer": "<1/2/3/4>",
    "solution_text": "<complete solution text>",
    "has_diagram": <true/false>,
    "diagram_description": "<describe all diagrams>",
    "confidence": <0.0 to 1.0>
}}

OPTION FORMAT RULES:
For each option use ONE of these formats:

1. TEXT option (no chemical structure):
   "1": "option text here"

2. CHEMICAL STRUCTURE option (molecule/compound diagram):
   "1": {{"type": "smiles", "smiles": "<SMILES>", "description": "<name>"}}

SMILES RULES:
- Provide VALID SMILES for the exact structure shown
- Common patterns:
  benzene: c1ccccc1 | cyclohexane: C1CCCCC1 | double bond: C=C
  triple bond: C#C | nitro: [N+](=O)[O-] | carboxylic acid: C(=O)O
  amide: C(=O)N | ester: C(=O)OC | amine: N | hydroxyl: O
  halides: F, Cl, Br, I | aromatic N: n (lowercase)
- Use / and \\ for E/Z stereochemistry when shown
- For polymers: show one repeat unit with terminal groups
- For carbocations: use [CH2+] or [CH3+]

EXAMPLES:
  biphenyl → {{"type": "smiles", "smiles": "c1ccc(-c2ccccc2)cc1", "description": "biphenyl"}}
  tert-nitro → {{"type": "smiles", "smiles": "CC(C)(C)[N+](=O)[O-]", "description": "2-methyl-2-nitropropane"}}
  ethanol text → "C₂H₅OH"

CRITICAL:
- Extract ONLY Chemistry questions
- Return ONLY a valid JSON array [ ... ]
- NO markdown, NO explanation
- Keep solution_text on ONE LINE
"""

STANDARD_PROMPT = """
Extract ALL {section} questions from this NEET question paper.

Paper structure:
- Question format: {question_format}
- Answer marker: {answer_marker}
- Options format: (1)(2)(3)(4)

For EACH {section} question return a JSON object with EXACTLY these fields:
{{
    "question_number": <integer>,
    "section": "{section}",
    "topic": "<topic name>",
    "concept": "<concept name>",
    "subject_concept": "<subject concept>",
    "difficulty": "<Easy/Medium/Hard>",
    "expected_time_seconds": <integer>,
    "question_text": "<complete question text, write [DIAGRAM] if diagram present>",
    "options": {{
        "1": "<option 1 text or exactly [DIAGRAM] if it is a diagram>",
        "2": "<option 2 text or exactly [DIAGRAM] if it is a diagram>",
        "3": "<option 3 text or exactly [DIAGRAM] if it is a diagram>",
        "4": "<option 4 text or exactly [DIAGRAM] if it is a diagram>"
    }},
    "has_question_diagram": <true/false>,
    "has_option_diagram": <true/false>,
    "has_solution_diagram": <true/false>,
    "option_diagrams": {{
        "1": <true/false>, "2": <true/false>,
        "3": <true/false>, "4": <true/false>
    }},
    "correct_answer": "<1/2/3/4>",
    "solution_text": "<complete solution text>",
    "has_diagram": <true/false>,
    "diagram_description": "<describe all diagrams>",
    "confidence": <0.0 to 1.0>
}}

CRITICAL:
- Extract ONLY {section} questions
- Return ONLY a valid JSON array [ ... ]
- NO markdown, NO explanation
- Keep solution_text on ONE LINE
"""


def get_extraction_prompt(section, pattern):
    """Returns section-specific prompt. Chemistry gets SMILES prompt."""
    qf = pattern.get('question_number_format', 'Question No. X')
    am = pattern.get('answer_marker', 'Sol. (X)')

    if section == "Chemistry":
        return CHEMISTRY_PROMPT.format(question_format=qf, answer_marker=am)
    else:
        return STANDARD_PROMPT.format(section=section, question_format=qf, answer_marker=am)