import subprocess
import sys
from datetime import datetime

# Static params
TARGET_BUCKET = "indian-highcourt-judgments"
COURT_CODE = "36_29"

START_YEAR = 1993
END_YEAR = datetime.now().year  # or set manually, e.g., 2025

for year in range(START_YEAR, END_YEAR + 1):
    print(f"\n=== Processing year {year} ===\n")
    cmd = [
        sys.executable,  # ensures correct Python interpreter
        "doc_processing.py",
        "--target-bucket", TARGET_BUCKET,
        "--year", str(year),
        "--court-code", COURT_CODE
    ]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"⚠️  Processing failed for year {year}")