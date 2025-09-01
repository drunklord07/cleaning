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
INPUT_FOLDER   = "input_logs"           # Folder with .txt inputs (non-recursive)
OUTPUT_FOLDER  = "mobile_only_output"   # Filtered outputs (same basenames)
SUMMARY_FILE   = "summary_report.txt"   # Saved in current working dir
RESUME_LOG     = "resume_files.log"     # Checkpoint log
MAX_WORKERS    = 6                      # Parallelism
ALLOWED_EXTS   = (".txt",)              # *** Only .txt ***
FIELD_NAME     = "CustomerId"           # Field label to append; change if needed
EMIT_ONE_SPACE = True                   # Space between kept bracket and ';' tail
# =========================== #

# Mobile regex EXACTLY as requested
MOBILE_REGEX = re.compile(r'(?<![A-Za-z0-9])(?:91)?[6-9]\d{9}(?![A-Za-z0-9])')

# [CustomerId: ...] (case-sensitive "CustomerId")
CUST_RE = re.compile(r"\[CustomerId:(.*?)\]")

def sanitize_line(line_wo_nl: str, mobile_match: re.Match) -> str | None:
    """
    Apply CustomerId/semicolon sanitization and append metadata.
    Returns final line (without trailing newline) or None to drop the line.
    """
    # Step A: locate first CustomerId bracket (if any)
    cust_m = CUST_RE.search(line_wo_nl)
    cust_token = cust_m.group(0) if cust_m else None

    # Step B: find first semicolon
    semi_idx = line_wo_nl.find(";")

    # Step C: build sanitized base per rules
    if cust_token:
        if semi_idx >= 0:
            tail = line_wo_nl[semi_idx:]  # keep from ';' inclusive
            base = cust_token + ((" " + tail.lstrip()) if EMIT_ONE_SPACE else tail)
        else:
            base = cust_token  # no ';' → just the bracket
    else:
        if semi_idx >= 0:
            base = line_wo_nl[semi_idx:]  # keep from ';' inclusive
        else:
            return None  # neither bracket nor ';' → drop the line

    # Step D: append metadata: ; <FIELD_NAME> ; mobile_regex ; <match_value>
    match_value = mobile_match.group(0)
    final_line = f"{base} ; {FIELD_NAME} ; mobile_regex ; {match_value}"
    return final_line

def process_file(file_path: str) -> dict:
    """
    Keep only lines containing a mobile number; sanitize and append metadata per rules.
    """
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_kept": 0,
        "lines_removed": 0,
        "kept_with_bracket": 0,
        "kept_without_bracket": 0,
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
                line = raw.rstrip("\n")

                m = MOBILE_REGEX.search(line)
                if not m:
                    local["lines_removed"] += 1
                    continue

                sanitized = sanitize_line(line, m)
                if sanitized is None:
                    # Had mobile, but neither bracket nor ';' → drop per rule
                    local["lines_removed"] += 1
                    continue

                # Count bracket presence for telemetry
                if CUST_RE.search(line):
                    local["kept_with_bracket"] += 1
                else:
                    local["kept_without_bracket"] += 1

                f_out.write(sanitized + "\n")
                local["lines_kept"] += 1

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
        f.write(f"Mobile-only Filter + CustomerId/Tail + Metadata - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder: {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"Max Workers: {summary['max_workers']}\n")
        f.write(f"Allowed Exts: {ALLOWED_EXTS}\n")
        f.write(f"Field name appended: {FIELD_NAME}\n\n")

        f.write("=== Files ===\n")
        f.write(f"Processed: {summary['files_scanned']}\n")
        f.write(f"Success:   {summary['files_success']}\n")
        f.write(f"Errors:    {summary['files_error']}\n")
        f.write(f"Blank inputs: {len(summary['blank_input_files'])}\n")
        f.write(f"Zero-kept outputs: {len(summary['zero_kept_files'])}\n\n")

        f.write("=== Lines (aggregate) ===\n")
        f.write(f"Total scanned:         {summary['total_lines_processed']}\n")
        f.write(f"Total kept:            {summary['total_lines_kept']}\n")
        f.write(f"Total removed:         {summary['total_lines_removed']}\n")
        f.write(f"Kept with bracket:     {summary['kept_with_bracket']}\n")
        f.write(f"Kept without bracket:  {summary['kept_without_bracket']}\n")

        if summary["errors"]:
            f.write("\n=== Errors ===\n")
            for err in summary["errors"]:
                f.write(f"- {err}\n")

def main():
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: INPUT_FOLDER does not exist: {INPUT_FOLDER}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Discover inputs (.txt only), stable order
    all_files = sorted(
        os.path.join(INPUT_FOLDER, f)
        for f in os.listdir(INPUT_FOLDER)
        if os.path.isfile(os.path.join(INPUT_FOLDER, f))
        and os.path.splitext(f)[1].lower() in ALLOWED_EXTS
    )

    if not all_files:
        print(f"No {ALLOWED_EXTS} files found in INPUT_FOLDER.", file=sys.stderr)
        # still write a summary shell
        summary = {
            "files_scanned": 0, "files_success": 0, "files_error": 0,
            "blank_input_files": [], "zero_kept_files": [], "errors": [],
            "total_lines_processed": 0, "total_lines_kept": 0, "total_lines_removed": 0,
            "kept_with_bracket": 0, "kept_without_bracket": 0,
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
            "blank_input_files": [], "zero_kept_files": [], "errors": [],
            "total_lines_processed": 0, "total_lines_kept": 0, "total_lines_removed": 0,
            "kept_with_bracket": 0, "kept_without_bracket": 0,
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
        "zero_kept_files": [],
        "errors": [],
        "total_lines_processed": 0,
        "total_lines_kept": 0,
        "total_lines_removed": 0,
        "kept_with_bracket": 0,
        "kept_without_bracket": 0,
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
                summary["total_lines_processed"] += res["lines_processed"]
                summary["total_lines_kept"] += res["lines_kept"]
                summary["total_lines_removed"] += res["lines_removed"]
                summary["kept_with_bracket"] += res["kept_with_bracket"]
                summary["kept_without_bracket"] += res["kept_without_bracket"]

                if res["input_was_blank"]:
                    summary["blank_input_files"].append(res["file_name"])
                if res["lines_processed"] > 0 and res["lines_kept"] == 0:
                    summary["zero_kept_files"].append(res["file_name"])

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
