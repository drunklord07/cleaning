#!/usr/bin/env python3
import os
import re
import sys
import time
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from functools import lru_cache
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from collections import Counter

# ========= CONFIGURATION ========= #
INPUT_FOLDER = "input_logs"          # Folder with .txt inputs (non-recursive)
OUTPUT_FOLDER = "cleaned_output"     # Folder for cleaned outputs
CUSTID_FOLDER = "custid_matches"     # Folder for [CustomerId]-only matches
SUMMARY_FILE = "summary_report.txt"  # Saved in current working dir
RESUME_LOG = "resume_files.log"      # Checkpoint log in current working dir
MAX_WORKERS = 6                      # Parallel workers
ALLOWED_EXTS = (".txt",)             # Process only .txt files
# ================================= #

# ---------- REGEX PATTERNS ----------
PII_PATTERNS = {
    "MOBILE_REGEX": re.compile(r'(?<![A-Za-z0-9])(?:91)?[6-9]\d{9}(?![A-Za-z0-9])'),
    "AADHAAR_REGEX": re.compile(r'(?<![A-Za-z0-9])(\d{12})(?![A-Za-z0-9])'),
    "PAN_REGEX": re.compile(r'(?<![A-Za-z0-9])[A-Z]{5}\d{4}[A-Z](?![A-Za-z0-9])', re.IGNORECASE),
    "GSTIN_REGEX": re.compile(r'(?<![A-Za-z0-9])\d{2}[A-Z]{5}\d{4}[A-Z][1-9A-Z]Z[0-9A-Z](?![A-Za-z0-9])', re.IGNORECASE),
    "DL_REGEX": re.compile(r'(?<![A-Za-z0-9])[A-Z]{2}\d{2}\d{11}(?![A-Za-z0-9])', re.IGNORECASE),
    "VOTERID_REGEX": re.compile(r'(?<![A-Za-z0-9])[A-Z]{3}\d{7}(?![A-Za-z0-9])', re.IGNORECASE),
    "EMAIL_REGEX": re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE),
    "UPI_REGEX": re.compile(r'[A-Za-z0-9._-]{2,256}@[A-Za-z]{2,64}(?=$|[\s<>\[\]\{\}\(\),;_\-])'),
    "IP_REGEX": re.compile(r'(?<!\d)(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)(?!\d)'),
    "MAC_REGEX": re.compile(r'(?<![A-Fa-f0-9])(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}(?![A-Fa-f0-9])'),
    "CARD_REGEX": re.compile(
        r'(?<![A-Za-z0-9])('
        r'4\d{12}(?:\d{3})?'
        r'|5[1-5]\d{14}'
        r'|2(?:2[2-9]\d{12}|[3-6]\d{13}|7(?:[01]\d{12}|20\d{12}))'
        r'|3[47]\d{13}'
        r'|60\d{14}|65\d{14}|81\d{14}|508\d\d{12}'
        r')(?![A-Za-z0-9])'
    ),
    "COORD_REGEX": re.compile(
        r'(?<![A-Za-z0-9])'
        r'([+-]?(?:90\.(?:0+)?|[0-8]?\d\.\d+))'
        r'\s*,\s*'
        r'([+-]?(?:180\.(?:0+)?|1[0-7]\d\.\d+|[0-9]?\d\.\d+))'
        r'(?![A-Za-z0-9])'
    ),
}

# ---------- Aadhaar Verhoeff ----------
@lru_cache(maxsize=10000)
def is_valid_aadhaar(number: str) -> bool:
    if len(number) != 12 or not number.isdigit():
        return False
    mul = [
        [0,1,2,3,4,5,6,7,8,9],
        [1,2,3,4,0,6,7,8,9,5],
        [2,3,4,0,1,7,8,9,5,6],
        [3,4,0,1,2,8,9,5,6,7],
        [4,0,1,2,3,9,5,6,7,8],
        [5,9,8,7,6,0,4,3,2,1],
        [6,5,9,8,7,1,0,4,3,2],
        [7,6,5,9,8,2,1,0,4,3],
        [8,7,6,5,9,3,2,1,0,4],
        [9,8,7,6,5,4,3,2,1,0]
    ]
    perm = [
        [0,1,2,3,4,5,6,7,8,9],
        [1,5,7,6,2,8,3,0,9,4],
        [5,8,0,3,7,9,6,1,4,2],
        [8,9,1,6,0,4,3,5,2,7],
        [9,4,5,3,1,2,6,8,7,0],
        [4,2,8,6,5,7,3,9,0,1],
        [2,7,9,3,8,0,6,4,1,5],
        [7,0,4,6,9,1,3,2,5,8]
    ]
    inv = [0,4,3,2,1,5,6,7,8,9]
    c = 0
    for i, ch in enumerate(reversed(number)):
        c = mul[c][perm[i % 8][int(ch)]]
    return inv[c] == 0

# ---------- Card Luhn ----------
def _luhn_valid(num: str) -> bool:
    s, alt = 0, False
    for ch in reversed(num):
        if not ch.isdigit():
            return False
        n = ord(ch) - 48
        if alt:
            n *= 2
            if n > 9:
                n -= 9
        s += n
        alt = not alt
    return (s % 10) == 0

# ---------- KEYWORD PHRASES ----------
KEYWORD_PHRASES = {
    "ADDRESS_KEYWORD": ["address","full address","complete address","residential address","permanent address","add"],
    "NAME_KEYWORD": ["name","nam"],
    "DOB_KEYWORD": ["date of birth","dob","birthdate","born on"],
    "ACCOUNT_NUMBER_KEYWORD": ["account number","acc number","bank account","account no","a/c no"],
    "CUSTOMER_ID_KEYWORD": ["customer id","cust id","customer number","cust"],
    "SENSITIVE_HINTS_KEYWORD": ["national id","identity card","proof of identity","document number"],
    "INSURANCE_POLICY_KEYWORD": ["insurance number","policy number","insurance id","ins id"],
}

# ---------- Build keyword regex ----------
KEYWORD_REGEXES = {}
for k, phrases in KEYWORD_PHRASES.items():
    compiled = []
    for p in phrases:
        pat = r'(?<![A-Za-z0-9])' + re.escape(p).replace(r"\ ", r"[ _-]+") + r'(?![A-Za-z0-9])'
        compiled.append(re.compile(pat, re.IGNORECASE))
    KEYWORD_REGEXES[k] = compiled

# ---------- CustomerId regex ----------
CUSTID_RE = re.compile(r"\[CustomerId:(.*?)\]")
# ---------- Processing Function ----------
def process_file(file_path: str, custid_out) -> dict:
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_removed": 0,
        "lines_kept": 0,
        "custid_moved": 0,
        "custid_mobile_only": 0,
        "regex_counts": Counter(),
        "keyword_counts": Counter(),
        "custid_values": [],
        "error": None,
    }

    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f_in, \
             open(out_path, "w", encoding="utf-8") as f_out:

            for raw in f_in:
                local["lines_processed"] += 1
                line = raw.rstrip("\n")

                # --- Detect CustomerId ---
                cust_match = CUSTID_RE.search(line)
                custid = cust_match.group(1) if cust_match else None

                # --- Skip empty [CustomerId:] ---
                if custid == "":
                    custid = None

                # --- Regex matches ---
                regex_hit = False
                regex_names = []
                for name, rx in PII_PATTERNS.items():
                    for m in rx.finditer(line):
                        val = m.group(0)
                        if name == "AADHAAR_REGEX" and not is_valid_aadhaar(val):
                            continue
                        if name == "CARD_REGEX" and not _luhn_valid(val):
                            continue
                        regex_hit = True
                        regex_names.append(name)
                        local["regex_counts"][name] += 1

                # --- Keyword matches ---
                keyword_hit = False
                for k, regs in KEYWORD_REGEXES.items():
                    for rx in regs:
                        if rx.search(line):
                            keyword_hit = True
                            local["keyword_counts"][k] += 1

                # --- Apply rules ---
                if custid:
                    # Ignore MOBILE_REGEX if it only comes from inside [CustomerId:xxxx]
                    mobile_only = False
                    if regex_hit and not keyword_hit:
                        if regex_names == ["MOBILE_REGEX"]:
                            mobile_only = True
                            regex_hit = False  # ignore mobile match

                    if regex_hit or keyword_hit:
                        f_out.write(raw)  # keep full line
                        local["lines_kept"] += 1
                    else:
                        # truncate to [CustomerId:xxxx] ;path
                        semi_idx = line.find(";")
                        if semi_idx != -1:
                            truncated = f"[CustomerId:{custid}] {line[semi_idx:]}"
                        else:
                            truncated = f"[CustomerId:{custid}]"
                        custid_out.write(truncated + "\n")
                        local["custid_moved"] += 1
                        if mobile_only:
                            local["custid_mobile_only"] += 1
                        local["custid_values"].append(custid)
                else:
                    if regex_hit or keyword_hit:
                        f_out.write(raw)  # keep
                        local["lines_kept"] += 1
                    else:
                        local["lines_removed"] += 1

    except Exception as e:
        err = f"{local['file_name']}: {e.__class__.__name__}: {e}"
        local["error"] = err

    return local

# ---------- Resume Log ----------
def load_completed_set(log_path: str) -> set:
    completed = set()
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                name = line.strip()
                if name and not line.startswith("#"):
                    completed.add(name)
    return completed

def append_completed(log_path: str, file_name: str):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(file_name + "\n")

# ---------- Summary ----------
def write_summary(summary):
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Regex + Keyword Filter with CustomerId Salvage - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder: {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"CustId Folder: {os.path.abspath(CUSTID_FOLDER)}\n")
        f.write(f"Max Workers: {MAX_WORKERS}\n\n")

        f.write("=== Files Processed ===\n")
        f.write(f"Processed: {summary['files_scanned']}\n")
        f.write(f"Success:   {summary['files_success']}\n")
        f.write(f"Errors:    {summary['files_error']}\n\n")

        f.write("=== Lines ===\n")
        f.write(f"Total lines processed: {summary['total_lines_processed']}\n")
        f.write(f"Lines kept:            {summary['total_lines_kept']}\n")
        f.write(f"Lines removed:         {summary['total_lines_removed']}\n")
        f.write(f"CustId moved:          {summary['total_custid_moved']}\n")
        f.write(f"  of which mobile-only: {summary['custid_mobile_only']}\n\n")

        f.write("=== Regex Matches ===\n")
        for k,v in summary["regex_counts"].items():
            f.write(f"  {k}: {v}\n")
        f.write("\n=== Keyword Matches ===\n")
        for k,v in summary["keyword_counts"].items():
            f.write(f"  {k}: {v}\n")

        f.write("\n=== CustomerId Stats ===\n")
        total_custids = len(summary["custid_values"])
        unique_custids = len(set(summary["custid_values"]))
        dupes = total_custids - unique_custids
        f.write(f"Total moved CustomerIds: {total_custids}\n")
        f.write(f"Unique CustomerIds:      {unique_custids}\n")
        f.write(f"Duplicate count:         {dupes}\n")

        if summary["errors"]:
            f.write("\n=== Errors ===\n")
            for err in summary["errors"]:
                f.write(f"- {err}\n")

# ---------- Main ----------
def main():
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: INPUT_FOLDER does not exist: {INPUT_FOLDER}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(CUSTID_FOLDER, exist_ok=True)

    all_files = sorted(
        os.path.join(INPUT_FOLDER, f)
        for f in os.listdir(INPUT_FOLDER)
        if os.path.isfile(os.path.join(INPUT_FOLDER, f))
        and os.path.splitext(f)[1].lower() in ALLOWED_EXTS
    )

    if not all_files:
        print(f"No {ALLOWED_EXTS} files found in INPUT_FOLDER.", file=sys.stderr)
        return

    completed = load_completed_set(RESUME_LOG)
    pending_files = [fp for fp in all_files if os.path.basename(fp) not in completed]

    if not pending_files:
        print("All files already processed per resume log. Nothing to do.")
        return

    summary = {
        "files_scanned": 0,
        "files_success": 0,
        "files_error": 0,
        "total_lines_processed": 0,
        "total_lines_kept": 0,
        "total_lines_removed": 0,
        "total_custid_moved": 0,
        "custid_mobile_only": 0,
        "regex_counts": Counter(),
        "keyword_counts": Counter(),
        "custid_values": [],
        "errors": []
    }

    custid_out = open(os.path.join(CUSTID_FOLDER, "all_custid.txt"), "a", encoding="utf-8")

    overall_bar = tqdm(total=len(pending_files), desc="Overall", unit="file", leave=True)

    try:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(process_file, fp, custid_out): fp for fp in pending_files}
            for fut in as_completed(futures):
                file_path = futures[fut]
                base_name = os.path.basename(file_path)

                try:
                    res = fut.result()
                    summary["files_scanned"] += 1
                    summary["total_lines_processed"] += res["lines_processed"]
                    summary["total_lines_kept"] += res["lines_kept"]
                    summary["total_lines_removed"] += res["lines_removed"]
                    summary["total_custid_moved"] += res["custid_moved"]
                    summary["custid_mobile_only"] += res["custid_mobile_only"]
                    summary["regex_counts"].update(res["regex_counts"])
                    summary["keyword_counts"].update(res["keyword_counts"])
                    summary["custid_values"].extend(res["custid_values"])

                    if res["error"]:
                        summary["files_error"] += 1
                        summary["errors"].append(res["error"])
                    else:
                        summary["files_success"] += 1
                        append_completed(RESUME_LOG, base_name)
                except Exception as e:
                    summary["files_scanned"] += 1
                    summary["files_error"] += 1
                    summary["errors"].append(f"{base_name}: worker exception: {e}")
                overall_bar.update(1)

                if summary["files_scanned"] > 0:
                    elapsed = time.time() - overall_bar.start_t
                    avg = elapsed / summary["files_scanned"]
                    remaining = len(pending_files) - summary["files_scanned"]
                    eta_seconds = max(0, remaining * avg)
                    overall_bar.set_postfix_str(f"ETA: {str(timedelta(seconds=int(eta_seconds)))}")
    finally:
        overall_bar.close()
        custid_out.close()
        write_summary(summary)

if __name__ == "__main__":
    main()
