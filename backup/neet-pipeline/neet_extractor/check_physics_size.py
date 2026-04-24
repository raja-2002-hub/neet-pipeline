# Save as check_physics_size.py
from google import genai
from dotenv import load_dotenv
import os

load_dotenv()
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

pdf_bytes = open("2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf", "rb").read()

prompt = """
Extract ALL 45 Physics questions from this NEET paper.
Return a JSON array.
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

raw = response.text
print(f"Response length: {len(raw)} characters")
print(f"First 100 chars: {repr(raw[:100])}")
print(f"Last 100 chars: {repr(raw[-100:])}")

# Check if it ends properly with ]
if raw.strip().endswith("]"):
    print("Response ends correctly with ]")
else:
    print("WARNING: Response does not end with ] — likely truncated")