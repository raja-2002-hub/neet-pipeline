import fitz
import re

doc = fitz.open("2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf")

print("Checking Biology pages (53+) for images...")
for page_num in range(52, len(doc)):
    page = doc[page_num]
    images = page.get_images(full=True)
    if images:
        print(f"Page {page_num+1}: {len(images)} images found")
        for img in images:
            xref = img[0]
            base = doc.extract_image(xref)
            rects = page.get_image_rects(xref)
            if rects:
                rect = rects[0]
                print(f"  Size: {base['width']}x{base['height']} | y0={round(rect.y0,2)}")

doc.close()