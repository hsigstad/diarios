import path
import pandas as pd
from glob import glob
import pytesseract
import textract
from codecs import decode
import os
from subprocess import run
from glob import glob
from PIL import Image
from tempfile import TemporaryDirectory
from pdf2image import convert_from_path

pytesseract.pytesseract.tesseract_cmd = (r'/usr/bin/tesseract')


def read_files(infiles, OCR=True, file_col='infile', text_col='text'):
    df = pd.DataFrame({file_col: infiles})
    df[text_col] = df.infile.apply(read_file, OCR=OCR)
    return df


def read_file(infile, OCR=True, min_length=100, check_for_txt=True):
    try:
        text = decode(textract.process(infile), 'utf-8')
        if OCR and len(text) < min_length:
            if check_for_txt:
                try:
                    with open(infile.replace('pdf', 'txt')) as f:
                        text = f.read()
                except:
                    text = ocr_file(infile)
            else:
                text = ocr_file(infile)
        return text
    except textract.exceptions.ShellError as e:
        if infile[-4:] in [".doc", "docx"]:
            try:
                cd = run(["catdoc", infile], capture_output=True)
                return decode(cd.stdout, 'utf-8')
            except:
                print("Unknown error:", infile)
                return ""
        else:
            print(e, infile)
            return ""


def ocr_file(PDF_file, save_as_txt=True):
    print("OCR:", PDF_file)
    image_file_list = []
    with TemporaryDirectory() as tempdir:
        pdf_pages = convert_from_path(PDF_file, 500) # 500 DPI
        print(len(pdf_pages), "pages")
        for page_enumeration, page in enumerate(pdf_pages, start=1):
            filename = f"{tempdir}/page_{page_enumeration:03}.jpg"
            page.save(filename, "JPEG")
            image_file_list.append(filename)
        text = '\n'.join(map(ocr_image, image_file_list))
    if save_as_txt:
        with open(PDF_file.replace('pdf', 'txt'), 'w') as f:
            f.write(text)
    return text


def ocr_image(image_file):
    print(image_file)
    return str(((pytesseract.image_to_string(Image.open(image_file), lang="por"))))
