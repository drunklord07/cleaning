#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import time
import traceback
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from tqdm import tqdm

# ====== CONFIGURATION ====== #
INPUT_FOLDER = "cleaned_output"     # Folder with cleaned .txt inputs
OUTPUT_FOLDER = "rewritten_output"  # Rewritten outputs (same basenames)
FINAL_FOLDER = "brackets_final"     # Folder for extracted bracket lines
FINAL_FILE = "brackets_final_mobile.txt"  # One combined file inside FINAL_FOLDER
SUMMARY_FILE = "summary_extract.txt"      # Saved in current working dir
RESUME_LOG = "resume_extract.log"         # Checkpoint log in current working dir
MAX_WORKERS = 6                           # Parallelism
ALLOWED_EXTS = (".txt",)                  # Process only .txt files
# =========================== #

FINAL_PATH = os.path.join(FINAL_FOLDER, FINAL_FILE)

# Regex for general matching
NONEMPTY_NO_MOBILE_RE = re.compile(r'^\[(CustomerNo|Mobile-No):\s*\d+\]\s*;.+$')
NONEMPTY_WITH_MOBILE_RE = re.compile(r'^\[(CustomerNo|Mobile-No):\s*\d+\](.+)$')

def classify_case(line: str):
    """Classify line into case1–case4 based on bracket count and key."""
    brackets = re.findall(r'\[[^\]]*\]', line)
    count = len(brackets)
    if "[CustomerNo:" in line and count == 10:
        return "case1"
    if "[Mobile-No:" in line and count == 6:
        return "case2"
    if "[Mobile-No:" in line and count == 9:
        return "case3"
    if "[Mobile-No:" in line and count == 8:
        return "case4"
    return "other"

def process_file(file_path: str):
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_modified": 0,
        "lines_removed": 0,
        "nonempty_no_mobile": 0,
        "nonempty_with_mobile": 0,
        "output_lines": 0,
        "bracket_lines": [],
        "error": None,
        "cases": {f"case{i}": {"no_mobile": 0, "with_mobile": 0} for i in range(1,5)},
    }
    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f_in, \
             open(out_path, "w", encoding="utf-8") as f_out:

            for raw in f_in:
                local["lines_processed"] += 1
                line = raw.rstrip("\n")

                case = classify_case(line)

                # nonempty_no_mobile
                if NONEMPTY_NO_MOBILE_RE.match(line):
                    local["nonempty_no_mobile"] += 1
                    local["lines_removed"] += 1
                    local["bracket_lines"].append(line)
                    if case.startswith("case"):
                        local["cases"][case]["no_mobile"] += 1
                    continue

                # nonempty_with_mobile
                m = NONEMPTY_WITH_MOBILE_RE.match(line)
                if m and ";" in line:
                    key = m.group(0).split("]")[0] + "]"
                    rest = line[len(key):]
                    if ";" not in rest:
                        f_out.write(line + "\n")
                        local["output_lines"] += 1
                        continue
                    body, path = rest.rsplit(";", 1)
                    body, path = body.strip(), path.strip()

                    local["bracket_lines"].append(f"{key};{path}")
                    local["nonempty_with_mobile"] += 1
                    if case.startswith("case"):
                        local["cases"][case]["with_mobile"] += 1

                    new_line = f"{body};{path}"
                    f_out.write(new_line + "\n")

                    local["lines_modified"] += 1
                    local["output_lines"] += 1
                    continue

                # Everything else unchanged
                f_out.write(line + "\n")
                local["output_lines"] += 1

    except Exception as e:
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        local["error"] = f"{local['file_name']}: {type(e).__name__}: {e}"
    return local

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

def write_summary(summary):
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Bracket Extraction Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder: {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"Final Folder: {os.path.abspath(FINAL_FOLDER)}\n")
        f.write(f"Final File: {FINAL_PATH}\n")
        f.write(f"Max Workers: {summary['max_workers']}\n\n")

        # Per-case stats
        for i in range(1,5):
            f.write(f"=== Case {i} ===\n")
            f.write(f"nonempty_with_mobile: {summary['cases'][f'case{i}']['with_mobile']}\n")
            f.write(f"nonempty_no_mobile : {summary['cases'][f'case{i}']['no_mobile']}\n\n")

        f.write("=== Totals ===\n")
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
        f.write(f"   Expected Final File Lines (computed)      : {expected_final_file_lines}\n")

        f.write(f"C) Rewritten + Final == Original + Splits    : "
                f"{lhs} == {rhs}  => {'PASS' if check_c_ok else 'FAIL'}\n\n")

        if summary["errors"]:
            f.write("=== Errors ===\n")
            for e in summary["errors"]:
                f.write(f"- {e}\n")

def main():
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: INPUT_FOLDER does not exist: {INPUT_FOLDER}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(FINAL_FOLDER, exist_ok=True)

    all_files = sorted(
        os.path.join(INPUT_FOLDER, f)
        for f in os.listdir(INPUT_FOLDER)
        if os.path.isfile(os.path.join(INPUT_FOLDER, f)) and os.path.splitext(f)[1].lower() in ALLOWED_EXTS
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
        "errors": [],
        "cases": {f"case{i}": {"no_mobile": 0, "with_mobile": 0} for i in range(1,5)},
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

                    for i in range(1,5):
                        summary["cases"][f"case{i}"]["no_mobile"] += res["cases"][f"case{i}"]["no_mobile"]
                        summary["cases"][f"case{i}"]["with_mobile"] += res["cases"][f"case{i}"]["with_mobile"]

                    # Append bracket lines to final file
                    if res["bracket_lines"]:
                        with open(FINAL_PATH, "a", encoding="utf-8") as f:
                            for l in res["bracket_lines"]:
                                f.write(l + "\n")
                                summary["final_file_lines"] += 1

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
                elapsed = time.time() - summary["files_scanned"]
                avg = elapsed / max(1, summary["files_scanned"])
                remaining = len(pending_files) - summary["files_scanned"]
                eta = max(0, int(remaining * avg))
                overall_bar.set_postfix_str(f"ETA: {str(timedelta(seconds=eta))}")
    finally:
        overall_bar.close()
        write_summary(summary)

if __name__ == "__main__":
    main()
