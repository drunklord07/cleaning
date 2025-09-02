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
INPUT_FOLDER = "input_logs"         # Folder with .txt inputs (non-recursive)
OUTPUT_FOLDER = "cleaned_output"    # Outputs as .txt (same basenames)
SUMMARY_FILE = "summary_report.txt" # Saved in current working dir
RESUME_LOG = "resume_files.log"     # Checkpoint log in current working dir
MAX_WORKERS = 6                     # Parallelism
ALLOWED_EXTS = (".txt",)            # Process only .txt files

CASE_SENSITIVE = True               # Key matches respect case
EMIT_SINGLE_SPACE = True            # Normalize join spacing around kept items
# =========================== #

MOBILE_REGEX = re.compile(r'(?<![A-Za-z0-9])(?:91)?[6-9]\d{9}(?![A-Za-z0-9])')

def extract_tokens_and_body(line: str):
    """Split line into tokens (leading brackets) and the rest (body+path)."""
    m = re.match(r'^\s*((?:\[[^\]]*\]\s*)+)(.*)$', line)
    if not m:
        return [], line
    preamble, body = m.groups()
    tokens = re.findall(r'\[[^\]]*\]', preamble)
    return tokens, body

def process_case(line: str, expected_brackets: int, key_name: str, case_id: str):
    """
    Generic case handler.
    expected_brackets: exact number of preamble brackets
    key_name: "CustomerNo" or "Mobile-No"
    case_id: "case1", "case2", "case3", "case4"
    """
    tokens, rest = extract_tokens_and_body(line)
    if len(tokens) != expected_brackets:
        return line, "unchanged", case_id

    key_tokens = [t for t in tokens if t.startswith(f"[{key_name}")]
    if not key_tokens:
        return line, "unchanged", case_id

    # Extract key value
    key_val = ""
    if ":" in key_tokens[0]:
        key_val = key_tokens[0][1:-1].split(":", 1)[1].strip()

    # Split body and path
    if ";" in rest:
        body, path = rest.split(";", 1)
        body, path = body.strip(), path.strip()
    else:
        body, path = rest.strip(), ""

    has_mobile = bool(MOBILE_REGEX.search(body))

    if key_val:  # non-empty
        if has_mobile:
            new_line = f"[{key_name}:{key_val}]{' ' if EMIT_SINGLE_SPACE and body else ''}{body};{path}"
            return new_line, "nonempty_with_mobile", case_id
        else:
            new_line = f"[{key_name}:{key_val}];{path}"
            return new_line, "nonempty_no_mobile", case_id
    else:  # empty key
        if has_mobile:
            new_line = f"{body};{path}"
            return new_line, "empty_with_mobile", case_id
        else:
            return None, "empty_no_mobile", case_id  # dropped

def process_line(line: str):
    """Dispatcher for line processing across cases."""
    # Case 1: 10 brackets + CustomerNo
    out, status, cid = process_case(line, 10, "CustomerNo", "case1")
    if status != "unchanged":
        return out, f"{cid}_{status}"

    # Case 2: 6 brackets + Mobile-No
    out, status, cid = process_case(line, 6, "Mobile-No", "case2")
    if status != "unchanged":
        return out, f"{cid}_{status}"

    # Case 3: 9 brackets + Mobile-No
    out, status, cid = process_case(line, 9, "Mobile-No", "case3")
    if status != "unchanged":
        return out, f"{cid}_{status}"

    # Case 4: 8 brackets + Mobile-No
    out, status, cid = process_case(line, 8, "Mobile-No", "case4")
    if status != "unchanged":
        return out, f"{cid}_{status}"

    # Unchanged line
    return line, "unchanged"

def process_file(file_path: str):
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_modified": 0,
        "lines_removed": 0,
        "unchanged": 0,
        "case_counts": {
            "case1": {k: 0 for k in ["nonempty_with_mobile","nonempty_no_mobile","empty_with_mobile","empty_no_mobile"]},
            "case2": {k: 0 for k in ["nonempty_with_mobile","nonempty_no_mobile","empty_with_mobile","empty_no_mobile"]},
            "case3": {k: 0 for k in ["nonempty_with_mobile","nonempty_no_mobile","empty_with_mobile","empty_no_mobile"]},
            "case4": {k: 0 for k in ["nonempty_with_mobile","nonempty_no_mobile","empty_with_mobile","empty_no_mobile"]},
        },
        "dropped_lines": [],
        "error": None,
        "output_lines": 0,
    }
    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f_in, \
             open(out_path, "w", encoding="utf-8") as f_out:
            for raw in f_in:
                local["lines_processed"] += 1
                new_line, status = process_line(raw.strip("\n"))

                if status.startswith("case"):
                    cid, key = status.split("_", 1)
                    if key in local["case_counts"][cid]:
                        local["case_counts"][cid][key] += 1

                if new_line is None:
                    local["lines_removed"] += 1
                    local["dropped_lines"].append(raw.strip())
                else:
                    if new_line != raw.strip():
                        local["lines_modified"] += 1
                    local["output_lines"] += 1
                    f_out.write(new_line + "\n")

                if status == "unchanged":
                    local["unchanged"] += 1

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
        f.write(f"Log Cleaning Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder: {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"Max Workers: {summary['max_workers']}\n\n")

        for cid in ["case1","case2","case3","case4"]:
            label = {
                "case1": "Case 1: CustomerNo (10 brackets)",
                "case2": "Case 2: Mobile-No (6 brackets)",
                "case3": "Case 3: Mobile-No (9 brackets)",
                "case4": "Case 4: Mobile-No (8 brackets)",
            }[cid]
            f.write(f"=== {label} ===\n")
            for k,v in summary["case_counts"][cid].items():
                f.write(f"{k}: {v}\n")
            f.write("\n")

        f.write("=== Totals ===\n")
        f.write(f"Files processed : {summary['files_scanned']}\n")
        f.write(f"Files success   : {summary['files_success']}\n")
        f.write(f"Files error     : {summary['files_error']}\n")
        f.write(f"Total lines     : {summary['total_lines_processed']}\n")
        f.write(f"Lines modified  : {summary['total_lines_modified']}\n")
        f.write(f"Lines removed   : {summary['total_lines_removed']}\n")
        f.write(f"Lines unchanged : {summary['unchanged']}\n")
        f.write(f"Final output lines: {summary['final_output_lines']}\n\n")

        if summary["dropped_lines"]:
            f.write("=== Dropped Lines ===\n")
            for l in summary["dropped_lines"]:
                f.write(l + "\n")

        if summary["errors"]:
            f.write("\n=== Errors ===\n")
            for e in summary["errors"]:
                f.write(f"- {e}\n")

def main():
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: INPUT_FOLDER does not exist: {INPUT_FOLDER}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
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
        "total_lines_modified": 0,
        "total_lines_removed": 0,
        "unchanged": 0,
        "final_output_lines": 0,
        "case_counts": {
            "case1": {k:0 for k in ["nonempty_with_mobile","nonempty_no_mobile","empty_with_mobile","empty_no_mobile"]},
            "case2": {k:0 for k in ["nonempty_with_mobile","nonempty_no_mobile","empty_with_mobile","empty_no_mobile"]},
            "case3": {k:0 for k in ["nonempty_with_mobile","nonempty_no_mobile","empty_with_mobile","empty_no_mobile"]},
            "case4": {k:0 for k in ["nonempty_with_mobile","nonempty_no_mobile","empty_with_mobile","empty_no_mobile"]},
        },
        "dropped_lines": [],
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
                    summary["total_lines_modified"] += res["lines_modified"]
                    summary["total_lines_removed"] += res["lines_removed"]
                    summary["unchanged"] += res["unchanged"]
                    summary["final_output_lines"] += res["output_lines"]
                    for cid in ["case1","case2","case3","case4"]:
                        for k,v in res["case_counts"][cid].items():
                            summary["case_counts"][cid][k] += v
                    summary["dropped_lines"].extend(res["dropped_lines"])
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
