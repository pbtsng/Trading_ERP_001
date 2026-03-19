import os
import google.generativeai as genai

api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
if not api_key:
    raise SystemExit("Set GEMINI_API_KEY or GOOGLE_API_KEY before running this test.")

genai.configure(api_key=api_key)

model = genai.GenerativeModel("gemini-2.0-flash")
response = model.generate_content("Hello, explain ERP in one line")
print(response.text)
