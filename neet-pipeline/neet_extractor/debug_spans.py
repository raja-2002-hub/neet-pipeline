import fitz

doc = fitz.open("2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf")

# Check pages 1, 2, 3 to see Sol. pattern
for page_num in range(3):
    page = doc[page_num]
    blocks = page.get_text("dict")["blocks"]
    print(f"\n--- Page {page_num+1} ---")
    for block in blocks:
        if block["type"] != 0:
            continue
        for line in block["lines"]:
            for span in line["spans"]:
                text = span["text"].strip()
                if text:
                    y = span["bbox"][1]
                    print(f"  y={round(y,2)} | {repr(text)}")

doc.close()