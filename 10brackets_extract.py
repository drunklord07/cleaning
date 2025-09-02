#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from tqdm import tqdm

# ====== CONFIGURATION ====== #
# INPUT for this step = output of your 4-case cleaner
INPUT_FOLDER = "cleaned_output"          # Folder with cleaned .txt inputs
OUTPUT_FOLDER = "rewritten_output"       # Rewritten outputs (same basenames)
FINAL_FOLDER = "brackets_final"          # Folder for extracted bracket lines
FINAL_FILE = "brackets_final_mobile.txt" # One combined file inside FINAL_FOLDER
SUMMARY_FILE = "summary_extract.txt"     # Saved in current working dir
RESUME_LOG = "resume_extract.log"        # Checkpoint log in current working dir
MAX_WORKERS = 6                          # Parallelism
ALLOWED_EXTS = (".txt",)                 # Process only .txt files

# OPTIONAL: original (pre-clean) input folder from the 4-case run
# If provided, we’ll compute Case 1–4 stats for comparison in the summary.
CASE_SOURCE_FOLDER = ""                  # e.g. "input_test" or absolute path; "" = disabled
# =========================== #

FINAL_PATH = os.path.join(FINAL_FOLDER, FINAL_FILE)

# ---------- tolerant helpers for CLEANED lines ----------
# First bracket token + remainder
RE_HEAD = re.compile(r'^\s*(\[[^\]]*?\])\s*(.*)$')

# Inside the bracket, extract key + digits (tolerant spacing)
RE_KEYVAL = re.compile(r'^\[\s*(CustomerNo|Mobile-No)\s*:\s*([0-9]+)\s*\]\s*$')

# ---------- ORIGINAL (pre-clean) case classification (optional) ----------
PREAMBLE_RE = re.compile(r'^\s*((?:\[[^\]]*\]\s*)+)(.*)$')
BRACKET_RE  = re.compile(r'\[[^\]]*\]')
MOBILE_REGEX = re.compile(r'(?<![A-Za-z0-9])(?:91)?[6-9]\d{9}(?![A-Za-z0-9])')

def classify_case_from_original(line: str) -> str:
    m = PREAMBLE_RE.match(line)
    if not m:
        return "other"
    preamble, _ = m.groups()
    tokens = BRACKET_RE.findall(preamble)
    cnt = len(tokens)
    joined = "".join(tokens)
    has_cust = "[CustomerNo:" in joined
    has_mob  = "[Mobile-No:" in joined
    if cnt == 10 and has_cust:
        return "case1"
    if cnt == 6 and has_mob:
        return "case2"
    if cnt == 9 and has_mob:
        return "case3"
    if cnt == 8 and has_mob:
        return "case4"
    return "other"

def original_key_is_nonempty(line: str) -> bool:
    m = PREAMBLE_RE.match(line)
    if not m:
        return False
    preamble = m.group(1)
    m1 = re.search(r'\[CustomerNo\s*:\s*([^\]]*)\]', preamble)
    m2 = re.search(r'\[Mobile-No\s*:\s*([^\]]*)\]', preamble)
    val = (m1.group(1) if m1 else (m2.group(1) if m2 else None))
    return bool(val and val.strip())

def original_body_has_mobile(line: str) -> bool:
    m = PREAMBLE_RE.match(line)
    if not m:
        return False
    _, rest = m.groups()
    if ";" not in rest:
        return False
    body, _ = rest.rsplit(";", 1)
    return bool(MOBILE_REGEX.search(body))

def scan_case_source_folder(folder: str):
    results = {f"case{i}": {"no_mobile": 0, "with_mobile": 0} for i in range(1,5)}
    if not folder or not os.path.isdir(folder):
        return None
    files = sorted(
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, f))
        and os.path.splitext(f)[1].lower() in ALLOWED_EXTS
    )
    for p in files:
        with open(p, "r", encoding="utf-8", errors="ignore") as fin:
            for raw in fin:
                s = raw.rstrip("\n")
                cid = classify_case_from_original(s)
                if cid == "other":
                    continue
                if not original_key_is_nonempty(s):
                    continue
                if original_body_has_mobile(s):
                    results[cid]["with_mobile"] += 1
                else:
                    results[cid]["no_mobile"] += 1
    return results

# ---------- extraction worker over CLEANED lines ----------
def process_file(file_path: str):
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_modified": 0,       # split count
        "lines_removed": 0,        # moved count
        "nonempty_no_mobile": 0,   # moved
        "nonempty_with_mobile": 0, # split
        "output_lines": 0,
        "bracket_lines": [],
        "error": None,
    }
    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f_in, \
             open(out_path, "w", encoding="utf-8") as f_out:

            for raw in f_in:
                local["lines_processed"] += 1
                line = raw.rstrip("\n")

                # Try to parse "[...]" head and remainder
                mhead = RE_HEAD.match(line)
                if not mhead:
                    # no bracket head → unchanged
                    f_out.write(line + "\n")
                    local["output_lines"] += 1
                    continue

                bracket, tail = mhead.groups()

                # Validate bracket is [CustomerNo:digits] or [Mobile-No:digits]
                if not RE_KEYVAL.match(bracket):
                    # head bracket isn't the kept key → unchanged
                    f_out.write(line + "\n")
                    local["output_lines"] += 1
                    continue

                # Must have a path separated by the LAST ';'
                if ";" not in tail:
                    # unexpected, keep unchanged
                    f_out.write(line + "\n")
                    local["output_lines"] += 1
                    continue

                body, path = tail.rsplit(";", 1)
                body, path = body.strip(), path.strip()

                if body == "":
                    # This is the "nonempty_no_mobile" shape → move as-is
                    local["nonempty_no_mobile"] += 1
                    local["lines_removed"] += 1
                    # exact original line to final file
                    local["bracket_lines"].append(f"{bracket};{path}")
                    continue
                else:
                    # "nonempty_with_mobile" shape → split
                    local["nonempty_with_mobile"] += 1
                    local["lines_modified"] += 1
                    # bracket+path to final
                    local["bracket_lines"].append(f"{bracket};{path}")
                    # body+path stays in rewritten output
                    f_out.write(f"{body};{path}\n")
                    local["output_lines"] += 1
                    continue

    except Exception as e:
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        local["error"] = f"{local['file_name']}: {type(e).__name__}: {e}"

    return local

# ---------- resume helpers ----------
def load_completed_set(log_path: str):
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

# ---------- summary ----------
def write_summary(summary, case_baseline):
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Bracket Extraction Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder: {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"Final Folder: {os.path.abspath(FINAL_FOLDER)}\n")
        f.write(f"Final File: {FINAL_PATH}\n")
        f.write(f"Max Workers: {summary['max_workers']}\n\n")

        # Case-wise (from ORIGINAL) if available
        f.write("=== Case-wise (from ORIGINAL input, if provided) ===\n")
        if case_baseline is None:
            f.write("N/A (set CASE_SOURCE_FOLDER to enable)\n\n")
        else:
            tot_w = tot_n = 0
            for i in range(1,5):
                w = case_baseline[f'case{i}']['with_mobile']
                n = case_baseline[f'case{i}']['no_mobile']
                tot_w += w; tot_n += n
                f.write(f"Case {i}: nonempty_with_mobile={w}, nonempty_no_mobile={n}\n")
            f.write(f"Case totals: with_mobile={tot_w}, no_mobile={tot_n}\n\n")

        f.write("=== Totals (this extraction run) ===\n")
        f.write(f"Files processed : {summary['files_scanned']}\n")
        f.write(f"Files success   : {summary['files_success']}\n")
        f.write(f"Files error     : {summary['files_error']}\n")
        f.write(f"Total lines processed : {summary['total_lines_processed']}\n")
        f.write(f"Lines moved (nonempty_no_mobile): {summary['nonempty_no_mobile']}\n")
        f.write(f"Lines split (nonempty_with_mobile): {summary['nonempty_with_mobile']}\n")
        f.write(f"Lines removed   : {summary['total_lines_removed']}\n")
        f.write(f"Lines modified  : {summary['total_lines_modified']}\n")
        f.write(f"Updated line count in output files: {summary['updated_line_count']}\n")
        f.write(f"Total lines written in {FINAL_FILE}: {summary['final_file_lines']}\n\n")

        # Consistency checks
        expected_output_lines = summary['total_lines_processed'] - summary['nonempty_no_mobile']
        check_a_ok = (summary['total_lines_processed'] ==
                      summary['updated_line_count'] + summary['nonempty_no_mobile'])
        expected_final_file_lines = summary['nonempty_no_mobile'] + summary['nonempty_with_mobile']
        check_b_ok = (summary['final_file_lines'] == expected_final_file_lines)
        lhs = summary['updated_line_count'] + summary['final_file_lines']
        rhs = summary['total_lines_processed'] + summary['nonempty_with_mobile']
        check_c_ok = (lhs == rhs)

        f.write("=== Consistency Checks ===\n")
        f.write(f"A) Original == Rewritten + Moved-only        : "
                f"{summary['total_lines_processed']} == {summary['updated_line_count']} + {summary['nonempty_no_mobile']}  "
                f"=> {'PASS' if check_a_ok else 'FAIL'}\n")
        f.write(f"   Expected Rewritten (computed)             : {expected_output_lines}\n")
        f.write(f"B) Final file lines == Moved + Split         : "
                f"{summary['final_file_lines']} == {summary['nonempty_no_mobile']} + {summary['nonempty_with_mobile']}  "
                f"=> {'PASS' if check_b_ok else 'FAIL'}\n")
        f.write(f"C) Rewritten + Final == Original + Splits    : "
                f"{lhs} == {rhs}  => {'PASS' if check_c_ok else 'FAIL'}\n\n")

        if summary["errors"]:
            f.write("=== Errors ===\n")
            for e in summary["errors"]:
                f.write(f"- {e}\n")

# ---------- main ----------
def main():
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: INPUT_FOLDER does not exist: {INPUT_FOLDER}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(FINAL_FOLDER, exist_ok=True)

    all_files = sorted(
        os.path.join(INPUT_FOLDER, f)
        for f in os.listdir(INPUT_FOLDER)
        if os.path.isfile(os.path.join(INPUT_FOLDER, f))
        and os.path.splitext(f)[1].lower() in ALLOWED_EXTS
    )
    if not all_files:
        print(f"No {ALLOWED_EXTS} files found in INPUT_FOLDER.", file=sys.stderr)
        return

    case_baseline = scan_case_source_folder(CASE_SOURCE_FOLDER) if CASE_SOURCE_FOLDER else None

    completed = set()
    if os.path.exists(RESUME_LOG):
        with open(RESUME_LOG, "r", encoding="utf-8") as f:
            for line in f:
                name = line.strip()
                if name and not line.startswith("#"):
                    completed.add(name)

    pending_files = [fp for fp in all_files if os.path.basename(fp) not in completed]
    if not pending_files:
        print("All files already processed per resume log. Nothing to do.")
        write_summary({
            "max_workers": MAX_WORKERS, "files_scanned": 0, "files_success": 0, "files_error": 0,
            "total_lines_processed": 0, "total_lines_removed": 0, "total_lines_modified": 0,
            "nonempty_no_mobile": 0, "nonempty_with_mobile": 0, "updated_line_count": 0,
            "final_file_lines": 0, "errors": []
        }, case_baseline)
        return

    summary = {
        "max_workers": MAX_WORKERS,
        "files_scanned": 0,
        "files_success": 0,
        "files_error": 0,
        "total_lines_processed": 0,
        "total_lines_removed": 0,
        "total_lines_modified": 0,
        "nonempty_no_mobile": 0,
        "nonempty_with_mobile": 0,
        "updated_line_count": 0,
        "final_file_lines": 0,
        "errors": []
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
                    summary["files_scanned"] += 1
                    summary["total_lines_processed"] += res["lines_processed"]
                    summary["total_lines_removed"] += res["lines_removed"]
                    summary["total_lines_modified"] += res["lines_modified"]
                    summary["nonempty_no_mobile"] += res["nonempty_no_mobile"]
                    summary["nonempty_with_mobile"] += res["nonempty_with_mobile"]
                    summary["updated_line_count"] += res["output_lines"]

                    if res["bracket_lines"]:
                        with open(FINAL_PATH, "a", encoding="utf-8") as f:
                            for l in res["bracket_lines"]:
                                f.write(l + "\n")
                                summary["final_file_lines"] += 1

                    with open(RESUME_LOG, "a", encoding="utf-8") as r:
                        r.write(base_name + "\n")

                    summary["files_success"] += 1
                except Exception as e:
                    summary["files_scanned"] += 1
                    summary["files_error"] += 1
                    summary["errors"].append(f"{base_name}: worker exception: {e}")
                overall_bar.update(1)
    finally:
        overall_bar.close()
        write_summary(summary, case_baseline)

if __name__ == "__main__":
    main()
