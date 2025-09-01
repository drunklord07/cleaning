#!/usr/bin/env python3
import os
import re
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from tqdm import tqdm
from collections import Counter

# ====== CONFIG ====== #
INPUT_FOLDER         = "input_logs"            # .txt inputs (non-recursive)
OUTPUT_CLEANED       = "output_cleaned_txt"    # non–mobile-only lines
OUTPUT_MOBILE_ONLY   = "output_mobile_only"    # mobile-only lines
SUMMARY_FILE         = "summary_report.txt"
RESUME_LOG           = "resume_files.log"
MAX_WORKERS          = 6
ALLOWED_EXTS         = (".txt",)               # *** only .txt ***
# ==================== #

# ---------- PII REGEX (same semantics as your reference) ----------
PII_PATTERNS = {
    "MOBILE_REGEX": re.compile(r'(?<![A-Za-z0-9])(?:91)?[6-9]\d{9}(?![A-Za-z0-9])'),
    "AADHAAR_REGEX": re.compile(r'(?<![A-Za-z0-9])(\d{12})(?![A-Za-z0-9])'),
    "PAN_REGEX": re.compile(r'(?<![A-Za-z0-9])[A-Z]{5}\d{4}[A-Z](?![A-Za-z0-9])', re.IGNORECASE),
    "GSTIN_REGEX": re.compile(r'(?<![A-Za-z0-9])\d{2}[A-Z]{5}\d{4}[A-Z][1-9A-Z]Z[0-9A-Z](?![A-Za-z0-9])', re.IGNORECASE),
    "DL_REGEX": re.compile(r'(?<![A-Za-z0-9])[A-Z]{2}\d{2}\d{11}(?![A-Za-z0-9])', re.IGNORECASE),
    "VOTERID_REGEX": re.compile(r'(?<![A-Za-z0-9])[A-Z]{3}\d{7}(?![A-Za-z0-9])', re.IGNORECASE),

    "EMAIL_REGEX": re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', re.IGNORECASE),
    "UPI_REGEX": re.compile(r'[A-Za-z0-9._-]{2,256}@[A-Za-z]{2,64}(?=$|[\s<>\[\]\{\}\(\),;_\-])'),

    "IP_REGEX": re.compile(
        r'(?<!\d)'
        r'(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}'
        r'(?:25[0-5]|2[0-4]\d|1?\d?\d)'
        r'(?!\d)'
    ),
    "MAC_REGEX": re.compile(r'(?<![A-Fa-f0-9])(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}(?![A-Fa-f0-9])'),

    "CARD_REGEX": re.compile(
        r'(?<![A-Za-z0-9])('
        r'4\d{12}(?:\d{3})?'                                 # Visa
        r'|5[1-5]\d{14}'                                     # Mastercard (old range)
        r'|2(?:2[2-9]\d{12}|[3-6]\d{13}|7(?:[01]\d{12}|20\d{12}))'  # Mastercard 2-series
        r'|3[47]\d{13}'                                      # Amex
        r'|60\d{14}|65\d{14}|81\d{14}|508\d\d{12}'           # Other BINs
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
def luhn_valid(num: str) -> bool:
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

# ---------- Keywords (same separator/adjacency rules as your script) ----------
KEYWORD_PHRASES = {
    "ADDRESS_KEYWORD": [
        "address", "full address", "complete address",
        "residential address", "permanent address", "add",
    ],
    "NAME_KEYWORD": ["name", "nam"],
    "DOB_KEYWORD": ["date of birth", "dob", "birthdate", "born on"],
    "ACCOUNT_NUMBER_KEYWORD": ["account number", "acc number", "bank account", "account no", "a/c no"],
    "CUSTOMER_ID_KEYWORD": ["customer id", "cust id", "customer number", "cust"],
    "SENSITIVE_HINTS_KEYWORD": ["national id", "identity card", "proof of identity", "document number"],
    "INSURANCE_POLICY_KEYWORD": ["insurance number", "policy number", "insurance id", "ins id"],
}

SEP = r"[ \t]*[-_][ \t]*|[ \t]+"
SEP_GROUP = f"(?:{SEP})"

def build_phrase_alt(phrase: str) -> str:
    tokens = [re.escape(tok) for tok in phrase.split()]
    if len(tokens) == 1:
        return tokens[0]
    separated = SEP_GROUP.join(tokens)  # date[sep]of[sep]birth
    contiguous = "".join(tokens)        # dateofbirth
    return f"(?:{separated}|{contiguous})"

def compile_keyword_patterns():
    patterns = {}
    for ktype, phrases in KEYWORD_PHRASES.items():
        alts = [build_phrase_alt(p) for p in phrases]
        core = "|".join(alts)
        patterns[ktype] = re.compile(rf"(?<![A-Za-z0-9])({core})(?![A-Za-z0-9])", re.IGNORECASE)
    return patterns

KEYWORD_PATTERNS = compile_keyword_patterns()

# ---------- Helpers ----------
MOBILE_RX = PII_PATTERNS["MOBILE_REGEX"]

OTHER_PII_KEYS = [k for k in PII_PATTERNS.keys() if k != "MOBILE_REGEX"]

def line_has_any_keyword(s: str) -> bool:
    for pat in KEYWORD_PATTERNS.values():
        if pat.search(s):
            return True
    return False

def line_has_other_pii(s: str) -> bool:
    for key in OTHER_PII_KEYS:
        pat = PII_PATTERNS[key]
        # Aadhaar must pass Verhoeff
        if key == "AADHAAR_REGEX":
            for m in pat.finditer(s):
                if is_valid_aadhaar(m.group(1)):
                    return True
            continue
        # Card must pass Luhn
        if key == "CARD_REGEX":
            for m in pat.finditer(s):
                if luhn_valid(m.group(1)):
                    return True
            continue
        # Others: any match is a hit
        if pat.search(s):
            return True
    return False

def classify_line(line: str) -> str:
    """
    Returns:
      "mobile_only" if >=1 mobile AND no other PII AND no keyword
      "clean"       otherwise (includes no-mobile lines)
    """
    if not MOBILE_RX.search(line):
        return "clean"
    if line_has_any_keyword(line):
        return "clean"
    if line_has_other_pii(line):
        return "clean"
    return "mobile_only"

# ---------- Worker ----------
def process_file(file_path: str) -> dict:
    local = {
        "file_name": os.path.basename(file_path),
        "lines_scanned": 0,
        "lines_mobile_only": 0,
        "lines_clean_kept": 0,
        "error": None,
        "input_was_blank": False,
    }

    base = os.path.basename(file_path)
    out_clean = os.path.join(OUTPUT_CLEANED, base)
    out_mobile = os.path.join(OUTPUT_MOBILE_ONLY, base)

    # Clean stale partials
    for p in (out_clean, out_mobile):
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f_in, \
             open(out_clean, "w", encoding="utf-8") as f_clean, \
             open(out_mobile, "w", encoding="utf-8") as f_mobile:

            any_line = False
            for raw in f_in:
                any_line = True
                local["lines_scanned"] += 1
                cls = classify_line(raw)
                if cls == "mobile_only":
                    f_mobile.write(raw)
                    local["lines_mobile_only"] += 1
                else:
                    f_clean.write(raw)
                    local["lines_clean_kept"] += 1

            if not any_line:
                local["input_was_blank"] = True

    except Exception as e:
        # remove partials
        for p in (out_clean, out_mobile):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        err = f"{local['file_name']}: {e.__class__.__name__}: {e}"
        err += "\n" + "".join(traceback.format_exception_only(type(e), e)).strip()
        local["error"] = err

    return local

# ---------- Resume ----------
def load_completed_set(log_path: str) -> set:
    done = set()
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                name = line.strip()
                if name and not name.startswith("#"):
                    done.add(name)
    return done

def append_completed(log_path: str, file_name: str):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(file_name + "\n")

# ---------- Summary ----------
def write_summary(summary):
    summary["end_ts"] = time.time()
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Mobile-Only Splitter (.txt) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder: {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Cleaned: {os.path.abspath(OUTPUT_CLEANED)}\n")
        f.write(f"Output Mobile-Only: {os.path.abspath(OUTPUT_MOBILE_ONLY)}\n")
        f.write(f"Max Workers: {summary['max_workers']}\n\n")

        f.write("=== Files ===\n")
        f.write(f"Processed: {summary['files_scanned']}\n")
        f.write(f"Success:   {summary['files_success']}\n")
        f.write(f"Errors:    {summary['files_error']}\n")
        f.write(f"Blank inputs: {len(summary['blank_input_files'])}\n\n")

        f.write("=== Lines (aggregate) ===\n")
        f.write(f"Total scanned:       {summary['total_lines_scanned']}\n")
        f.write(f"Mobile-only (shift): {summary['total_mobile_only']}\n")
        f.write(f"Kept in cleaned:     {summary['total_clean_kept']}\n")

        if summary["errors"]:
            f.write("\n=== Errors ===\n")
            for err in summary["errors"]:
                f.write(f"- {err}\n")

# ---------- Main ----------
def main():
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: INPUT_FOLDER does not exist: {INPUT_FOLDER}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(OUTPUT_CLEANED, exist_ok=True)
    os.makedirs(OUTPUT_MOBILE_ONLY, exist_ok=True)

    # Discover .txt files
    all_files = sorted(
        os.path.join(INPUT_FOLDER, f)
        for f in os.listdir(INPUT_FOLDER)
        if os.path.isfile(os.path.join(INPUT_FOLDER, f))
        and os.path.splitext(f)[1].lower() in ALLOWED_EXTS
    )

    if not all_files:
        print(f"No {ALLOWED_EXTS} files found in INPUT_FOLDER.", file=sys.stderr)
        # still write an empty-ish summary
        summary = {
            "files_scanned": 0, "files_success": 0, "files_error": 0,
            "blank_input_files": [], "errors": [],
            "total_lines_scanned": 0, "total_mobile_only": 0, "total_clean_kept": 0,
            "max_workers": MAX_WORKERS, "start_ts": time.time(), "end_ts": None,
        }
        write_summary(summary)
        return

    completed = load_completed_set(RESUME_LOG)
    pending_files = [fp for fp in all_files if os.path.basename(fp) not in completed]

    if not pending_files:
        print("All files already processed per resume log. Nothing to do.")
        summary = {
            "files_scanned": 0, "files_success": 0, "files_error": 0,
            "blank_input_files": [], "errors": [],
            "total_lines_scanned": 0, "total_mobile_only": 0, "total_clean_kept": 0,
            "max_workers": MAX_WORKERS, "start_ts": time.time(), "end_ts": None,
        }
        write_summary(summary)
        return

    summary = {
        "start_ts": time.time(),
        "end_ts": None,
        "max_workers": MAX_WORKERS,
        "files_scanned": 0,
        "files_success": 0,
        "files_error": 0,
        "blank_input_files": [],
        "errors": [],
        "total_lines_scanned": 0,
        "total_mobile_only": 0,
        "total_clean_kept": 0,
    }

    overall_bar = tqdm(total=len(pending_files), desc="Overall", unit="file", leave=True)

    try:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(process_file, fp): fp for fp in pending_files}

            for fut in as_completed(futures):
                file_path = futures[fut]
                base_name = os.path.basename(file_path)

                try:
                    res = fut.result()
                except Exception as e:
                    summary["files_scanned"] += 1
                    summary["files_error"] += 1
                    summary["errors"].append(f"{base_name}: worker exception: {e}")
                    overall_bar.update(1)
                    continue

                summary["files_scanned"] += 1
                summary["total_lines_scanned"] += res["lines_scanned"]
                summary["total_mobile_only"] += res["lines_mobile_only"]
                summary["total_clean_kept"] += res["lines_clean_kept"]

                if res["input_was_blank"]:
                    summary["blank_input_files"].append(res["file_name"])

                if res["error"]:
                    summary["files_error"] += 1
                    summary["errors"].append(res["error"])
                else:
                    summary["files_success"] += 1
                    append_completed(RESUME_LOG, base_name)

                overall_bar.update(1)

                # ETA
                elapsed = time.time() - summary["start_ts"]
                avg = elapsed / max(1, summary["files_scanned"])
                remaining = len(pending_files) - summary["files_scanned"]
                eta = max(0, int(remaining * avg))
                overall_bar.set_postfix_str(f"ETA: {str(timedelta(seconds=eta))}")

    finally:
        overall_bar.close()
        write_summary(summary)

if __name__ == "__main__":
    main()
