# The contents of the file: /high_court_judgements/high_court_judgements/doc_processing.py

"""
PDF processing pipeline for large-scale Indian High Court judgments.

Behavior:
 - Walk S3 prefix under `data/pdf/...` (not data/tar).
 - For each PDF:
     - Try to extract text (PyPDF2).
     - Compute word-dictionary ratio (>= 0.6 default) using `wordfreq` if installed,
       otherwise fallback heuristic.
     - Detect language (langdetect) and accept only allowed languages (en, hi, te, mr by default).
     - If text is garbled (low dictionary ratio or language not allowed):
         - Run OCR with ocrmypdf (calls system binary), which will add a text layer.
         - Re-extract text and re-classify.
 - Upload the possibly-OCR'ed PDF back to S3. By default it writes to a configurable output bucket/prefix.
   Optionally overwrite the source key (dangerous; use --overwrite-source).
 - Parallelized using ThreadPoolExecutor.

Dependencies (system):
    - tesseract (and language packs you need: hin, tel, mar, eng)
    - ocrmypdf
    - poppler (if needed for some conversions)
Python packages:
    pip install boto3 PyPDF2 langdetect ocrmypdf wordfreq
    (wordfreq is optional; script will run without it)

Run:
    python3 pdf_processor_s3.py --source-bucket indian-high-court-judgments \
        --prefix "data/pdf/year=1950" --output-bucket my-processed-bucket \
        --workers 8 --min-word-ratio 0.6 --allowed-langs en,hi,te,mr
"""

import argparse
import boto3
import logging
import os
import tempfile
import shutil
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, List, Tuple
import threading

import PyPDF2 #type:ignore
from langdetect import detect_langs, DetectorFactory

try:
    from wordfreq import zipf_frequency #type:ignore
    HAVE_WORDFREQ = True
except ImportError:
    HAVE_WORDFREQ = False

DetectorFactory.seed = 0

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("pdf-processor")

S3 = boto3.client(
    's3'
)
DEFAULT_ALLOWED_LANGS = {"en", "hi", "te", "mr"}
SOURCE_BUCKET_NAME = "indian-high-court-judgments"  

log_lock = threading.Lock()
per_document_logs = []  # Will be flushed to a file at the end


def build_prefix(year: str, court_code: str) -> str:
    return f"data/pdf/year={year}/court={court_code}/"

def classify_doc_type(text: str) -> str:
    """
    Classifies court document into 'judgment', 'order', or 'unknown'.
    Uses extended keyword patterns, reasoning cues, procedural phrases,
    structural hints, and length-based heuristics.
    """

    text_upper = text.strip().upper()
    first_1000 = text_upper[:1000]
    word_count = len(text.split())

    # --- 1. Strong judgment keywords ---
    judgment_keywords = [
        r'\bJUDGMENT\b',
        r'\bCOMMON JUDGMENT\b',
        r'\bREASONS FOR JUDGMENT\b',
        r'\bFINAL JUDGMENT\b',
        r'\bSPEAKING JUDGMENT\b',
        r'\bCAV JUDGMENT\b',
        r'\bVERDICT\b',
        r'\bAWARD\b',
        r'\bDECISION\b',
        r'\bPRONOUNCED\b',
        r'\bREASONS\b'
    ]

    # --- 2. Strong order keywords ---
    order_keywords = [
        r'\bFINAL ORDER\b',
        r'\bINTERIM ORDER\b',
        r'\bORDER\b',
        r'\bSPEAKING ORDER\b',
        r'\bOPERATIVE ORDER\b',
        r'\bCAV ORDER\b',
        r'\bORDER SHEET\b',
        r'\bPROCEEDINGS\b',
        r'\bMINUTES OF ORDER\b',
        r'\bDIRECTION\b'
    ]

    judgment_keywords_multi = [
        # English
        r'\bJUDGMENT\b', r'\bFINAL JUDGMENT\b', r'\bCOMMON JUDGMENT\b',
        r'\bDECREE\b', r'\bVERDICT\b', r'\bAWARD\b', r'\bDECISION\b',
        r'\bCONVICTION\b', r'\bACQUITTAL\b',
        # Hindi
        r'निर्णय', r'फैसला', r'आदेश', r'याचिका स्वीकार', r'याचिका अस्वीकार',
        # Marathi
        r'निर्णय', r'निकाल', r'निवाडा', r'अपील मंजूर', r'अपील नामंजूर',
        # Bengali
        r'রায়', r'সিদ্ধান্ত', r'আবেদন মঞ্জুর', r'আবেদন খারিজ', r'আপিল গৃহীত', r'আপিল বাতিল',
        # Gujarati
        r'ચુકાદો', r'હુકમ', r'નિર્ણય', r'અપીલ સ્વીકાર', r'અપીલ નામંજૂર', r'અરજી મંજૂર', r'અરજી નામંજૂર'
        # --- Kannada ---
        "ತೀರ್ಪು", "ಸಾಮಾನ್ಯ ತೀರ್ಪು", "ಅಂತಿಮ ತೀರ್ಪು", "ಕಾರಣ ಸಮೇತ ತೀರ್ಪು", "ನಿರ್ಣಯ", "ಪ್ರಶಸ್ತಿ"
    ]

    order_keywords_multi = [
        # English
        r'\bORDER\b', r'\bFINAL ORDER\b', r'\bINTERIM ORDER\b', r'\bSTAY ORDER\b',
        r'\bISSUE NOTICE\b', r'\bADJOURNED\b',
        # Hindi
        r'अंतरिम आदेश', r'स्थगन आदेश', r'नोटिस जारी', r'अगली सुनवाई', r'स्थगित',
        # Marathi
        r'अंतरिम आदेश', r'स्थगित', r'पुढील तारखेस ठेवण्यात येईल',
        # Bengali
        r'অন্তর্বর্তী আদেশ', r'স্থগিতাদেশ', r'পরবর্তী শুনানি',
        # Gujarati
        r'અંતરિમ હુકમ', r'સ્થગિત', r'આગલી તારીખે મુકવામાં આવે'
        # --- Kannada ---
        r"ಅಂತಿಮ ಆದೇಶ", r"ಅಂತರಿಮ ಆದೇಶ", r"ಆದೇಶ", r"ಕಾರಣ ಸಮೇತ ಆದೇಶ", r"ವಿಧಾನ", r"ದಿಶಾನಿರ್ದೇಶ"
    ]

    for pat in judgment_keywords_multi:
        if re.search(pat, first_1000):
            return "judgment"
    for pat in order_keywords_multi:
        if re.search(pat, first_1000):
            return "order"

    # --- 3. Reasoning / finding cues ---
    reasoning_cues = [
        "HAVING HEARD", "HAVING CONSIDERED", "IT IS HELD", "FOR THE REASONS",
        "REASONS", "FINDINGS", "WE FIND", "THE COURT FINDS", "THE COURT HOLDS",
        "CONCLUSION", "IN VIEW OF THE ABOVE", "WE CONCLUDE", "IN THE RESULT",
        "THE SUIT IS DECREED", "PETITION IS ALLOWED", "PETITION IS DISMISSED",
        "APPEAL IS ALLOWED", "APPEAL IS DISMISSED", "CONVICTED", "ACQUITTED", "JUDGEMENT:",
        "SENTENCE", "RELIEF GRANTED", "ISSUE FRAMED", "POINT FOR DETERMINATION"
    ]
    reasoning_cues_multi = [
        # --- English ---
        "HAVING HEARD", "HAVING CONSIDERED", "IT IS HELD", "FOR THE REASONS",
        "REASONS", "FINDINGS", "WE FIND", "THE COURT FINDS", "THE COURT HOLDS",
        "CONCLUSION", "IN VIEW OF THE ABOVE", "WE CONCLUDE", "IN THE RESULT",
        "THE SUIT IS DECREED", "PETITION IS ALLOWED", "PETITION IS DISMISSED",
        "APPEAL IS ALLOWED", "APPEAL IS DISMISSED", "CONVICTED", "ACQUITTED",
        "JUDGEMENT:", "SENTENCE", "RELIEF GRANTED", "ISSUE FRAMED",
        "POINT FOR DETERMINATION",

        # --- Hindi ---
        "सुनवाई के बाद", "विचार करने के बाद", "यह निर्णय दिया जाता है", "कारणों से",
        "कारण", "निष्कर्ष", "हम पाते हैं", "न्यायालय पाता है", "न्यायालय मानता है",
        "उपरोक्त के दृष्टिगत", "हम निष्कर्ष निकालते हैं", "परिणामस्वरूप",
        "वाद डिक्री किया जाता है", "याचिका स्वीकार की जाती है", "याचिका अस्वीकृत की जाती है",
        "अपील स्वीकार की जाती है", "अपील अस्वीकृत की जाती है", "दोषी ठहराया गया", "बरी किया गया",
        "निर्णय:", "सजा", "राहत दी जाती है", "मुद्दा तय किया गया", "निर्धारण हेतु बिंदु",

        # --- Marathi ---
        "ऐकून घेतल्यानंतर", "विचार करून", "असे ठरविले", "कारणास्तव",
        "कारणे", "निष्कर्ष", "आम्ही आढळले", "न्यायालय आढळते", "न्यायालय ठरवते",
        "वरीलप्रमाणे", "आम्ही निष्कर्ष काढतो", "परिणामी",
        "दावा डिक्री केला", "याचिका मंजूर", "याचिका नामंजूर",
        "अपील मंजूर", "अपील फेटाळले", "दोषी ठरवले", "निर्दोष मुक्त",
        "निर्णय:", "शिक्षा", "दिलासा देण्यात आला", "मुद्दा ठरवला", "निर्धारणासाठी मुद्दा",

        # --- Bengali ---
        "শুনানি শেষে", "বিবেচনার পর", "এটি নির্ধারণ করা হয়", "কারণের জন্য",
        "কারণ", "উপসংহার", "আমরা পাই", "আদালত খুঁজে পেয়েছে", "আদালত সিদ্ধান্ত নিয়েছে",
        "উপরোক্ত কারণে", "আমরা সিদ্ধান্তে পৌঁছেছি", "ফলস্বরূপ",
        "মামলা ডিক্রি করা হলো", "আবেদন গৃহীত", "আবেদন খারিজ",
        "আপিল গৃহীত", "আপিল খারিজ", "দোষী সাব্যস্ত", "অভিযুক্ত খালাস",
        "রায়:", "শাস্তি", "রাহাত প্রদান", "ইস্যু নির্ধারণ", "বিবেচনার জন্য প্রশ্ন",

        # --- Gujarati ---
        "સંભળી લીધા પછી", "વિચાર કર્યા બાદ", "આ નિર્ણય આપવામાં આવે છે", "કારણસર",
        "કારણ", "નિષ્કર્ષ", "અમે માનીએ છીએ", "કોર્ટ શોધે છે", "કોર્ટ ઠરાવે છે",
        "ઉપરોક્તના દૃષ્ટિએ", "અમે નિષ્કર્ષ કાઢીએ છીએ", "પરિણામે",
        "દાવો ડિક્રી કરવામાં આવે છે", "અરજી મંજૂર કરવામાં આવે છે", "અરજી નામંજૂર કરવામાં આવે છે",
        "અપીલ સ્વીકારવામાં આવે છે", "અપીલ નામંજૂર કરવામાં આવે છે", "દોષિત ઠરાવવામાં આવ્યો",
        "બરી કરવામાં આવ્યો", "ચુકાદો:", "સજા", "રાહત આપવામાં આવી", "મુદ્દો નક્કી કરવામાં આવ્યો",
        "નિર્ધારણ માટે મુદ્દો"

        # --- Kannada ---
        "ಶ್ರವಣೆಯ ನಂತರ", "ವಿಚಾರಿಸಿದ ನಂತರ", "ಇದು ತೀರ್ಮಾನಿಸಲಾಗಿದೆ", "ಕಾರಣಗಳಿಂದ",
        "ಕಾರಣಗಳು", "ನಿರ್ಣಯ", "ನಾವು ಕಂಡುಹಿಡಿದಿದ್ದೇವೆ", "ನ್ಯಾಯಾಲಯ ಕಂಡುಹಿಡಿಯುತ್ತದೆ", "ನ್ಯಾಯಾಲಯ ತೀರ್ಮಾನಿಸುತ್ತದೆ",
        "ನ್ಯಾಯಾಲಯ ಅಭಿಪ್ರಾಯ ಹೊಂದಿದೆ", "ಮೇಲಿನ ದೃಷ್ಟಿಯಿಂದ", "ನಾವು ನಿರ್ಣಯಕ್ಕೆ ಬಂದಿದ್ದೇವೆ", "ಫಲವಾಗಿ",
        "ವಿವಾದವನ್ನು ಡಿಕ್ರಿ ಮಾಡಲಾಗಿದೆ", "ವಿನಂತಿಯನ್ನು ಅಂಗೀಕರಿಸಲಾಗಿದೆ", "ವಿನಂತಿಯನ್ನು ತಿರಸ್ಕರಿಸಲಾಗಿದೆ",
        "ಮನ್ನಣೆ ನೀಡಲಾಗಿದೆ", "ಅಪೀಲನ್ನು ತಿರಸ್ಕರಿಸಲಾಗಿದೆ", "ದೋಷಿ ಎಂದು ತೀರ್ಮಾನಿಸಲಾಗಿದೆ",
        "ವಿಮುಕ್ತಗೊಳಿಸಲಾಗಿದೆ", "ತೀರ್ಪು:", "ಶಿಕ್ಷೆ", "ಸಹಾಯ ನೀಡಲಾಗಿದೆ",
        "ಪ್ರಶ್ನೆಯನ್ನು ರೂಪಿಸಲಾಗಿದೆ", "ನಿರ್ಣಯಕ್ಕಾಗಿ ವಿಷಯ"
    ]

    if word_count > 1200 and any(cue in text_upper for cue in reasoning_cues_multi):
        return "judgment"

    # --- 4. Procedural / listing cues ---
    procedural_phrases = [
        "DIRECTED TO", "LET THIS MATTER APPEAR", "WARNING LIST", "DAILY CAUSE LIST",
        "ADJOURNED", "LIST THE MATTER", "CALL FOR RECORDS", "PRODUCE THE RECORDS",
        "STAND OVER TO", "FIXED FOR", "POSTED FOR", "PUT UP ON", "MENTIONED BEFORE",
        "PLACE BEFORE", "TAKEN ON BOARD", "REFERRED TO", "TO BE LISTED",
        "TO BE PLACED", "RE-NOTIFY", "NOTIFIED FOR", "HEARING ON", "FOR ORDERS",
        "NEXT DATE OF HEARING", "ON THE NEXT DATE", "DIRECTED THAT",
        "REGISTRAR TO", "NOTICE TO", "ISSUE NOTICE", "RETURNABLE ON", "ORDER:",
        "LISTED FOR", "FURTHER ORDERS", "FOR MENTIONING", "THE COURT MADE THE FOLLOWING ORDER"
    ]
    procedural_phrases_multi = [
        # --- English ---
        "DIRECTED TO", "LET THIS MATTER APPEAR", "WARNING LIST", "DAILY CAUSE LIST",
        "ADJOURNED", "LIST THE MATTER", "CALL FOR RECORDS", "PRODUCE THE RECORDS",
        "STAND OVER TO", "FIXED FOR", "POSTED FOR", "PUT UP ON", "MENTIONED BEFORE",
        "PLACE BEFORE", "TAKEN ON BOARD", "REFERRED TO", "TO BE LISTED",
        "TO BE PLACED", "RE-NOTIFY", "NOTIFIED FOR", "HEARING ON", "FOR ORDERS",
        "NEXT DATE OF HEARING", "ON THE NEXT DATE", "DIRECTED THAT",
        "REGISTRAR TO", "NOTICE TO", "ISSUE NOTICE", "RETURNABLE ON", "ORDER:",
        "LISTED FOR", "FURTHER ORDERS", "FOR MENTIONING",
        "THE COURT MADE THE FOLLOWING ORDER",

        # --- Hindi ---
        "निर्देश दिया गया", "यह मामला उपस्थित हो", "चेतावनी सूची", "दैनिक कारण सूची",
        "स्थगित", "मामला सूचीबद्ध किया जाए", "रिकॉर्ड मंगाए जाएं", "रिकॉर्ड प्रस्तुत करें",
        "अगली सुनवाई", "निर्धारित", "स्थगन आदेश", "सूचना जारी करें",
        "नोटिस दिया जाए", "नोटिस वापसी योग्य", "आदेश:", "आगे की कार्यवाही",

        # --- Marathi ---
        "निर्देश देण्यात आले", "ही बाब समोर आणण्यात यावी", "इशारा सूची", "दैनिक कारण सूची",
        "स्थगित", "मुद्दा सूचीबद्ध", "नोंदी मागविण्यात याव्यात", "नोंदी सादर करा",
        "पुढील तारखेस", "निश्चित", "अंतरिम आदेश", "सूचना जारी करा",
        "नोटीस देण्यात आली", "नोटीस परत करण्यायोग्य", "आदेश:", "पुढील कार्यवाही",

        # --- Bengali ---
        "নির্দেশ দেওয়া হলো", "এই বিষয় হাজির করা হবে", "সতর্ক তালিকা", "দৈনিক কার্যতালিকা",
        "স্থগিত", "বিষয় তালিকাভুক্ত", "রেকর্ড চাওয়া হয়েছে", "রেকর্ড জমা দিন",
        "পরবর্তী শুনানি", "নির্ধারিত", "অন্তর্বর্তী আদেশ", "নোটিশ জারি করুন",
        "নোটিশ দেওয়া হলো", "নোটিশ ফেরতযোগ্য", "আদেশ:", "পরবর্তী কার্যক্রম",

        # --- Gujarati ---
        "આદેશ આપવામાં આવ્યો", "આ મુદ્દો હાજર થાય", "ચેતવણી યાદી", "દૈનિક કારણ યાદી",
        "સ્થગિત", "મામલો યાદીમાં મૂકો", "રેકોર્ડ માંગવામાં આવે", "રેકોર્ડ રજૂ કરો",
        "આગલી સુનાવણી", "નિર્ધારિત", "અંતરિમ હુકમ", "નોટિસ જારી કરો",
        "નોટિસ આપવામાં આવી", "નોટિસ પરત કરી શકાય તેવી", "હુકમ:", "આગળની કાર્યવાહી"

         # --- Kannada ---
        "ನಿರ್ದೇಶನ ನೀಡಲಾಗಿದೆ", "ಈ ವಿಷಯ ಹಾಜರುಪಡಿಸಬೇಕು", "ಎಚ್ಚರಿಕೆ ಪಟ್ಟಿ", "ದೈನಂದಿನ ಕಾರಣ ಪಟ್ಟಿ",
        "ಸ್ಥಗಿತ", "ದಾಖಲೆಗಳನ್ನು ಕೋರಲಾಗಿದೆ", "ದಾಖಲೆಗಳನ್ನು ಸಲ್ಲಿಸಿ",
        "ಮುಂದಿನ ವಿಚಾರಣೆ", "ನೋಟಿಸ್ ಜಾರಿ ಮಾಡಿ", "ನೋಟಿಸ್ ಹಿಂತಿರುಗಿಸಬಹುದಾದ", "ಆದೇಶ:", "ಮುಂದಿನ ಕಾರ್ಯಚರಣೆ"
    ]
    if word_count < 1000 and any(phrase in text_upper for phrase in procedural_phrases_multi):
        return "order"

    # --- 5. Length-only heuristic ---
    if word_count < 300:
        return "order"

    # --- 6. Default fallback ---
    return "unknown"


def list_pdfs_in_prefix(bucket: str, prefix: str) -> List[str]:
    paginator = S3.get_paginator("list_objects_v2")
    print(f'Paginating through keys of the bucket: {bucket}', flush= True)
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".pdf"):
                keys.append(obj["Key"])
    logger.info(f"Found {len(keys)} PDFs in {bucket}/{prefix}")
    return keys


def download_s3_object(bucket: str, key: str, local_path: str) -> None:
    S3.download_file(bucket, key, local_path)


def upload_pdf_s3_object(bucket: str, key: str, local_path: str) -> None:
    with open(local_path, "rb") as f:
        S3.put_object(Bucket=bucket, Key=key, Body=f.read(), ContentType="application/pdf")

def upload_json_to_s3(bucket: str, key: str, local_path: str) -> None:
    """Upload a JSON log file with correct ContentType."""
    with open(local_path, "rb") as f:
        S3.put_object(Bucket=bucket, Key=key, Body=f.read(), ContentType="application/json")

def extract_text_from_pdf(path: str) -> str:
    try:
        text_parts = []
        with open(path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages:
                txt = page.extract_text()
                if txt:
                    text_parts.append(txt)
        return "\n".join(text_parts)
    except Exception:
        return ""


def compute_word_dictionary_ratio(text: str) -> float:
    tokens = []
    for raw in text.split():
        token = "".join(ch for ch in raw if ch.isalpha()).lower()
        if len(token) >= 2:
            tokens.append(token)
    if not tokens:
        return 0.0
    if HAVE_WORDFREQ:
        good = sum(1 for t in tokens if zipf_frequency(t, "en") >= 2.0)
        return good / len(tokens)
    vowels = set("aeiou")
    good = sum(1 for t in tokens if (len(t) >= 3 and any(c in vowels for c in t)))
    return good / len(tokens)


def detect_language(text: str) -> Tuple[Optional[str], float]:
    if not text.strip():
        return None, 0.0
    try:
        langs = detect_langs(text)
        if not langs:
            return None, 0.0
        top = langs[0]
        return top.lang, top.prob
    except Exception:
        return None, 0.0


def run_ocrmypdf(input_pdf: str, output_pdf: str, languages: str = "eng+hin+tel+mar") -> bool:
    cmd = ["ocrmypdf", "--quiet", "--redo-ocr", "-l", languages, input_pdf, output_pdf]
    try:
        subprocess.check_call(cmd)
        return True
    except Exception as e:
        logger.error(f"OCR failed: {e}")
        return False


def process_pdf(source_bucket: str, target_bucket: str, key: str, min_word_ratio: float, allowed_langs: set, ocr_languages: str):
    stats = {
        "key": key,
        "status": None,
        "word_ratio_before": None,
        "lang_before": None,
        "word_ratio_after": None,
        "lang_after": None,
        "did_ocr": False
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        local_pdf = os.path.join(tmpdir, "input.pdf")
        try:
            download_s3_object(source_bucket, key, local_pdf)
        except Exception as e:
            stats["status"] = f"download_failed: {e}"
            with log_lock:
                per_document_logs.append(stats)
            return

        text = extract_text_from_pdf(local_pdf)
        wr_before = compute_word_dictionary_ratio(text)
        lang_before, conf_before = detect_language(text)
        stats["word_ratio_before"] = wr_before
        stats["lang_before"] = (lang_before, conf_before)

        is_garbled = (wr_before < min_word_ratio) and (lang_before not in allowed_langs)
        if not text.strip():
            is_garbled = True

        if is_garbled:
            ocr_pdf = os.path.join(tmpdir, "ocr.pdf")
            if run_ocrmypdf(local_pdf, ocr_pdf, ocr_languages):
                shutil.move(ocr_pdf, local_pdf)
                stats["did_ocr"] = True
                text_after = extract_text_from_pdf(local_pdf)
                wr_after = compute_word_dictionary_ratio(text_after)
                lang_after, conf_after = detect_language(text_after)
                stats["word_ratio_after"] = wr_after
                stats["lang_after"] = (lang_after, conf_after)
                if wr_after >= min_word_ratio and lang_after in allowed_langs:
                    stats["status"] = "ok_after_ocr"
                else:
                    stats["status"] = "ocr_done_but_still_garbled"
            else:
                stats["status"] = "ocr_failed"
        else:
            stats["status"] = "ok_no_ocr"

        doc_type = classify_doc_type(text)
        stats["doc_type"] = doc_type
        try:
            if doc_type == "judgment":
                # Store as usual
                if stats['status'] in ["ok_no_ocr", "ok_after_ocr"]:
                    upload_pdf_s3_object(target_bucket, key, local_pdf)

            # elif doc_type == "order":
            #     order_key = f"orders/{key}"
            #     upload_s3_object("judgements-integration-sample-bucket", order_key, local_pdf)

            # elif doc_type == "unknown":
            #     unknown_key = f"unknown/{key}"
            #     upload_s3_object("judgements-integration-sample-bucket", unknown_key, local_pdf)

            # else:
            #     # fallback just in case classify_doc_type returns something unexpected
            #     fallback_key = f"misc/{key}"
            #     upload_s3_object("judgements-integration-sample-bucket", fallback_key, local_pdf)

        except Exception as e:
            stats["status"] = f"upload_failed: {e}"

    with log_lock:
        per_document_logs.append(stats)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-bucket", required=True)
    parser.add_argument("--year", required=True)
    parser.add_argument("--court-code", required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--min-word-ratio", type=float, default=0.6)
    parser.add_argument("--allowed-langs", default="en,hi,te,mr")
    parser.add_argument("--ocr-languages", default="eng+hin+tel+mar")
    parser.add_argument("--log-file", default="processing_log")
    args = parser.parse_args()

    allowed_langs = set(l.strip() for l in args.allowed_langs.split(","))
    prefix = build_prefix(args.year, args.court_code)
    keys = list_pdfs_in_prefix(SOURCE_BUCKET_NAME, prefix)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(process_pdf, SOURCE_BUCKET_NAME, args.target_bucket, key,
                            args.min_word_ratio, allowed_langs, args.ocr_languages)
            for key in keys
        ]
        for _ in as_completed(futures):
            pass

    import json
    with open(f'{args.log_file}_{args.year}.jsonl', "w", encoding="utf-8") as f:
        for record in per_document_logs:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    key = f'logs/year={args.year}/court={args.court_code}/{args.log_file}_{args.year}.jsonl'
    upload_json_to_s3("judgements-integration-sample-bucket", key, f'{args.log_file}_{args.year}.jsonl')
    logger.info(f"Logs written to {args.log_file}")


if __name__ == "__main__":
    main()