#!/usr/bin/env python3
import os
import sys
import time
import re
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from tqdm import tqdm

# ====== CONFIGURATION ====== #
INPUT_FOLDER  = "input_logs"          # Folder with .txt inputs (non-recursive)
OUTPUT_FOLDER = "redacted_output"     # Redacted outputs (same basenames)
SUMMARY_FILE  = "summary_report.txt"  # Saved in current working dir
RESUME_LOG    = "resume_files.log"    # Checkpoint log
MAX_WORKERS   = 6                     # Parallelism
ALLOWED_EXTS  = (".txt",)             # *** Only .txt ***
REPLACEMENT   = "<mobile_regex>"      # what to insert for each mobile match
# =========================== #

# Mobile regex EXACTLY as provided
MOBILE_REGEX = re.compile(r'(?<![A-Za-z0-9])(?:91)?[6-9]\d{9}(?![A-Za-z0-9])')

def process_file(file_path: str) -> dict:
    """
    Replace all mobile matches with REPLACEMENT and write to OUTPUT_FOLDER.
    """
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_changed": 0,
        "replacements": 0,
        "error": None,
        "input_was_blank": False,
    }

    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    # Clean any stale partial from a previous failed attempt
    try:
        if os.path.exists(out_path):
            os.remove(out_path)
    except Exception:
        pass

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f_in, \
             open(out_path, "w", encoding="utf-8") as f_out:

            any_line = False
            for raw in f_in:
                any_line = True
                local["lines_processed"] += 1

                new_line, nsubs = MOBILE_REGEX.subn(REPLACEMENT, raw)
                if nsubs > 0:
                    local["lines_changed"] += 1
                    local["replacements"] += nsubs
                f_out.write(new_line)

            if not any_line:
                local["input_was_blank"] = True

    except Exception as e:
        # Remove partial output so the file is retried next run
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        err = f"{local['file_name']}: {e.__class__.__name__}: {e}"
        err += "\n" + "".join(traceback.format_exception_only(type(e), e)).strip()
        local["error"] = err

    return local

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

def write_summary(summary):
    summary["end_ts"] = time.time()
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Mobile Redaction Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder:  {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"Max Workers:   {summary['max_workers']}\n")
        f.write(f"Replacement:   {REPLACEMENT}\n\n")

        f.write("=== Files ===\n")
        f.write(f"Processed: {summary['files_scanned']}\n")
        f.write(f"Success:   {summary['files_success']}\n")
        f.write(f"Errors:    {summary['files_error']}\n")
        f.write(f"Blank inputs: {len(summary['blank_input_files'])}\n\n")

        f.write("=== Lines / Replacements (aggregate) ===\n")
        f.write(f"Total lines processed: {summary['total_lines_processed']}\n")
        f.write(f"Lines changed:         {summary['total_lines_changed']}\n")
        f.write(f"Total replacements:    {summary['total_replacements']}\n")

        if summary["errors"]:
            f.write("\n=== Errors ===\n")
            for err in summary["errors"]:
                f.write(f"- {err}\n")

def main():
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: INPUT_FOLDER does not exist: {INPUT_FOLDER}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Discover inputs (.txt only)
    all_files = sorted(
        os.path.join(INPUT_FOLDER, f)
        for f in os.listdir(INPUT_FOLDER)
        if os.path.isfile(os.path.join(INPUT_FOLDER, f))
        and os.path.splitext(f)[1].lower() in ALLOWED_EXTS
    )

    if not all_files:
        print(f"No {ALLOWED_EXTS} files found in INPUT_FOLDER.", file=sys.stderr)
        summary = {
            "files_scanned": 0, "files_success": 0, "files_error": 0,
            "blank_input_files": [], "errors": [],
            "total_lines_processed": 0, "total_lines_changed": 0, "total_replacements": 0,
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
            "total_lines_processed": 0, "total_lines_changed": 0, "total_replacements": 0,
            "max_workers": MAX_WORKERS, "start_ts": time.time(), "end_ts": None,
        }
        write_summary(summary)
        return

    summary = {
        "start_ts": time.time(), "end_ts": None, "max_workers": MAX_WORKERS,
        "files_scanned": 0, "files_success": 0, "files_error": 0,
        "blank_input_files": [], "errors": [],
        "total_lines_processed": 0, "total_lines_changed": 0, "total_replacements": 0,
    }

    overall_bar = tqdm(total=len(pending_files), desc="Redacting", unit="file", leave=True)

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
                summary["total_lines_processed"] += res["lines_processed"]
                summary["total_lines_changed"] += res["lines_changed"]
                summary["total_replacements"] += res["replacements"]

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
