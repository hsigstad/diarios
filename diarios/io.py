from pathlib import Path
import pandas as pd
import pytesseract
from pdf2image import convert_from_path
from tempfile import TemporaryDirectory
from PIL import Image
from subprocess import run
import pypdf

pytesseract.pytesseract.tesseract_cmd = "/usr/bin/tesseract"


def read_files(infiles, OCR=True, file_col="infile", text_col="text"):
    df = pd.DataFrame({file_col: infiles})
    df[text_col] = df[file_col].apply(lambda f: read_file(f, OCR=OCR))
    return df


def read_file(infile, OCR=True, min_length=100, check_for_txt=True):
    path = Path(infile)
    suffix = path.suffix.lower()

    # 1) Try format-specific text extraction
    if suffix == ".pdf":
        text = extract_pdf_text(path)
    elif suffix == ".docx":
        text = extract_docx_text(path)
    elif suffix == ".doc":
        text = extract_doc_text(path)
    else:
        text = ""

    # 2) Fallback to OCR if needed
    if OCR and len(text) < min_length and suffix == ".pdf":
        if check_for_txt:
            txt_path = path.with_suffix(".txt")
            if txt_path.exists():
                text = txt_path.read_text(encoding="utf-8")
            else:
                text = ocr_file(path)
        else:
            text = ocr_file(path)

    return text


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        reader = pypdf.PdfReader(str(pdf_path))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as e:
        print("PDF extract error:", e, pdf_path)
        return ""


def extract_docx_text(docx_path: Path) -> str:
    try:
        import docx
        doc = docx.Document(str(docx_path))
        return "\n".join(p.text for p in doc.paragraphs)
    except Exception as e:
        print("DOCX extract error:", e, docx_path)
        return ""


def extract_doc_text(doc_path: Path) -> str:
    try:
        cd = run(["catdoc", str(doc_path)], capture_output=True, check=False)
        return cd.stdout.decode("utf-8", errors="ignore")
    except Exception as e:
        print("DOC extract error:", e, doc_path)
        return ""


def ocr_file(pdf_path: Path, save_as_txt=True) -> str:
    print("OCR:", pdf_path)
    image_file_list = []
    with TemporaryDirectory() as tempdir:
        pdf_pages = convert_from_path(str(pdf_path), dpi=300)
        print(len(pdf_pages), "pages")
        for i, page in enumerate(pdf_pages, start=1):
            img_path = Path(tempdir) / f"page_{i:03}.jpg"
            page.save(img_path, "JPEG")
            image_file_list.append(img_path)

        text = "\n".join(ocr_image(img_path) for img_path in image_file_list)

    if save_as_txt:
        pdf_path.with_suffix(".txt").write_text(text, encoding="utf-8")
    return text


def ocr_image(image_path: Path) -> str:
    print(image_path)
    img = Image.open(image_path)
    return pytesseract.image_to_string(
        img,
        lang="por",
        config="--oem 1 --psm 6",
    )


# import path
# import pandas as pd
# from glob import glob
# import pytesseract
# import textract
# from codecs import decode
# import os
# from subprocess import run
# from glob import glob
# from PIL import Image
# from tempfile import TemporaryDirectory
# from pdf2image import convert_from_path

# pytesseract.pytesseract.tesseract_cmd = (r'/usr/bin/tesseract')


# def read_files(infiles, OCR=True, file_col='infile', text_col='text'):
#     df = pd.DataFrame({file_col: infiles})
#     df[text_col] = df.infile.apply(read_file, OCR=OCR)
#     return df


# def read_file(infile, OCR=True, min_length=100, check_for_txt=True):
#     try:
#         text = decode(textract.process(infile), 'utf-8')
#         if OCR and len(text) < min_length:
#             if check_for_txt:
#                 try:
#                     with open(infile.replace('pdf', 'txt')) as f:
#                         text = f.read()
#                 except:
#                     text = ocr_file(infile)
#             else:
#                 text = ocr_file(infile)
#         return text
#     except textract.exceptions.ShellError as e:
#         if infile[-4:] in [".doc", "docx"]:
#             try: # some .doc files are really .docx
#                 text = textract.process(infile, extension='docx')
#                 return decode(text, 'utf-8')
#             except:
#                 try:
#                     cd = run(["catdoc", infile], capture_output=True)
#                     return decode(cd.stdout, 'utf-8')
#                 except:
#                     print("Unknown error:", infile)
#                     return ""
#         else:
#             print(e, infile)
#             return ""


# def ocr_file(PDF_file, save_as_txt=True):
#     print("OCR:", PDF_file)
#     image_file_list = []
#     with TemporaryDirectory() as tempdir:
#         pdf_pages = convert_from_path(PDF_file, 500) # 500 DPI
#         print(len(pdf_pages), "pages")
#         for page_enumeration, page in enumerate(pdf_pages, start=1):
#             filename = f"{tempdir}/page_{page_enumeration:03}.jpg"
#             page.save(filename, "JPEG")
#             image_file_list.append(filename)
#         text = '\n'.join(map(ocr_image, image_file_list))
#     if save_as_txt:
#         with open(PDF_file.replace('pdf', 'txt'), 'w') as f:
#             f.write(text)
#     return text


# def ocr_image(image_file):
#     print(image_file)
#     return str(((pytesseract.image_to_string(Image.open(image_file), lang="por"))))
