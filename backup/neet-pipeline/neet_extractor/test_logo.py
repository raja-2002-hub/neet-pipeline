import fitz
doc = fitz.open("2016-NEET-Solutions-Phase-1-Code-A-P-W.pdf")
for page_num in range(3):
    page = doc[page_num]
    print(f"--- Page {page_num+1} ---")
    for img in page.get_images(full=True):
        xref = img[0]
        base = doc.extract_image(xref)
        rects = page.get_image_rects(xref)
        if rects:
            rect = rects[0]
            print(f"  Size: {base['width']}x{base['height']} | y0={round(rect.y0,2)}")
doc.close()