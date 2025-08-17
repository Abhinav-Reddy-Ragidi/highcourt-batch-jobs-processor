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

    for pat in judgment_keywords:
        if re.search(pat, first_1000):
            return "judgment"
    for pat in order_keywords:
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
    if word_count > 1200 and any(cue in text_upper for cue in reasoning_cues):
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
    if word_count < 1000 and any(phrase in text_upper for phrase in procedural_phrases):
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