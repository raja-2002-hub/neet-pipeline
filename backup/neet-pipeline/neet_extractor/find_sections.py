# Save as find_sections.py
import fitz
import re

doc = fitz.open("2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf")

for page_num in range(len(doc)):
    page = doc[page_num]
    text = page.get_text()
    
    # Look for section headers
    if re.search(r'\bChemistry\b', text) and page_num < 60:
        print(f"Page {page_num+1}: Contains 'Chemistry'")
    if re.search(r'\bBiology\b', text) and page_num < 80:
        print(f"Page {page_num+1}: Contains 'Biology'")

doc.close()