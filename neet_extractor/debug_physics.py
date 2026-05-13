# Save this as debug_physics.py
from google import genai
from dotenv import load_dotenv
import os

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

pdf_bytes = open("2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf", "rb").read()

prompt = """
Extract ONLY the first 3 Physics questions from this NEET paper.
Return a JSON array with 3 items only.
No markdown. No explanation. Just the JSON array starting with [
"""

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

# Print EXACT raw response — first 500 characters
raw = response.text
print("=== FIRST 100 CHARACTERS (showing exact bytes) ===")
print(repr(raw[:100]))
print()
print("=== FULL RESPONSE PREVIEW ===")
print(raw[:500])