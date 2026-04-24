from google import genai
from dotenv import load_dotenv
import os

# Load API key
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")

# Connect to Gemini
client = genai.Client(api_key=api_key)

# Send test message
response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents="Say exactly this: Gemini is working correctly"
)

print(response.text)