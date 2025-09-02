#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import traceback
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from tqdm import tqdm

# ====== CONFIGURATION ====== #
INPUT_FOLDER = "input_logs"             # Folder with .txt inputs (non-recursive)
OUTPUT_FOLDER = "cleaned_output"        # Outputs as .txt (same basenames)
SUMMARY_FILE = "summary_dashclean.txt"  # Saved in current working dir
RESUME_LOG = "resume_dashclean.log"     # Checkpoint log in current working dir
MAX_WORKERS = 6                         # Parallelism
ALLOWED_EXTS = (".txt",)                # Process only .txt files
# =========================== #

def transform_line(line: str) -> (str, bool):
    """
    If ' - ' occurs (exact space-dash-space), cut at the LAST occurrence.
    Remove everything to the left including ' - '.
    Returns (new_line, changed_flag).
    """
    idx = line.rfind(" - ")
    if idx != -1:
        return line[idx+3:], True
    return line, False

def process_file(file_path: str):
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_with_dash": 0,
        "lines_without_dash": 0,
        "lines_changed": 0,
        "lines_unchanged": 0,
        "output_lines": 0,
        "error": None,
    }
    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f_in, \
             open(out_path, "w", encoding="utf-8") as f_out:

            for raw in f_in:
                local["lines_processed"] += 1
                has_nl = raw.endswith("\n")
                line = raw[:-1] if has_nl else raw

                new_line, changed = transform_line(line)
                if changed:
                    local["lines_with_dash"] += 1
                    local["lines_changed"] += 1
                else:
                    local["lines_without_dash"] += 1
                    local["lines_unchanged"] += 1

                f_out.write(new_line + ("\n" if has_nl else ""))
                local["output_lines"] += 1

    except Exception as e:
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        err = f"{local['file_name']}: {type(e).__name__}: {e}"
        err += "\n" + "".join(traceback.format_exception_only(type(e), e)).strip()
        local["error"] = err

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
        f.write(f"Dash Cleaner Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder : {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"Max Workers  : {summary['max_workers']}\n\n")

        f.write("=== Files ===\n")
        f.write(f"Total input files  : {summary['files_scanned']}\n")
        f.write(f"Files success      : {summary['files_success']}\n")
        f.write(f"Files error        : {summary['files_error']}\n")
        f.write(f"Total output files : {summary['files_success']}\n\n")

        f.write("=== Lines ===\n")
        f.write(f"Total lines processed : {summary['total_lines_processed']}\n")
        f.write(f"Lines with ' - '      : {summary['total_lines_with_dash']}\n")
        f.write(f"Lines without ' - '   : {summary['total_lines_without_dash']}\n")
        f.write(f"Lines changed         : {summary['total_lines_changed']}\n")
        f.write(f"Lines unchanged       : {summary['total_lines_unchanged']}\n")
        f.write(f"Total output lines    : {summary['output_lines']}\n\n")

        if summary["errors"]:
            f.write("=== Errors ===\n")
            for err in summary["errors"]:
                f.write(f"- {err}\n")

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
        "total_lines_with_dash": 0,
        "total_lines_without_dash": 0,
        "total_lines_changed": 0,
        "total_lines_unchanged": 0,
        "output_lines": 0,
        "errors": []
    }

    overall_bar = tqdm(total=len(pending_files), desc="Overall", unit="file", leave=True)
    start_ts = time.time()

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
                    summary["total_lines_with_dash"] += res["lines_with_dash"]
                    summary["total_lines_without_dash"] += res["lines_without_dash"]
                    summary["total_lines_changed"] += res["lines_changed"]
                    summary["total_lines_unchanged"] += res["lines_unchanged"]
                    summary["output_lines"] += res["output_lines"]

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
                elapsed = time.time() - start_ts
                avg = elapsed / max(1, summary["files_scanned"])
                remaining = len(pending_files) - summary["files_scanned"]
                eta = max(0, int(remaining * avg))
                overall_bar.set_postfix_str(f"ETA: {timedelta(seconds=eta)}")

    finally:
        overall_bar.close()
        write_summary(summary)

if __name__ == "__main__":
    main()
