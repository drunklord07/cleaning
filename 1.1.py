import os
import re
import sys
import time
import gzip
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from tqdm import tqdm

# ====== CONFIGURATION ====== #
INPUT_FOLDER = "input_logs"         # Folder with .gz inputs (non-recursive)
OUTPUT_FOLDER = "filtered_output"   # Filtered outputs as .gz (same basenames)
SUMMARY_FILE = "summary_report.txt" # Saved in current working dir
RESUME_LOG   = "resume_files.log"   # Checkpoint log in current working dir
MAX_WORKERS  = 6                    # Use 6–8 for optimal performance
GZIP_LEVEL   = 6                    # Compression level for outputs
# =========================== #

# ---- Main pattern: header (>=5 bracketed fields) + optional '-' + "<### Request URI/URL:" + [CustomerId:...]
LOG_PATTERN = re.compile(
    r'^(?:\[[^]]+\]\s*){5,}\s*-?\s*<###\s*Request\s+ur[il]\s*:\s*.*?'
    r'\[(?:CustomerId|CustomerID|Customer\s*Id)\s*[:=]\s*([^\]]+)\]',
    re.IGNORECASE
)

# ---- Lightweight probes to diagnose non-matches
HEADER_PROBE   = re.compile(r'^(?:\[[^]]+\]\s*){5,}')
URI_PROBE      = re.compile(r'<###\s*Request\s+ur[il]\s*:', re.IGNORECASE)
ID_TOKEN_PROBE = re.compile(r'\[(?:CustomerId|CustomerID|Customer\s*Id)\s*[:=]', re.IGNORECASE)
ID_VALUE_PROBE = re.compile(r'\[(?:CustomerId|CustomerID|Customer\s*Id)\s*[:=]\s*([^\]]*)\]', re.IGNORECASE)

def process_file(file_path: str) -> dict:
    """
    Runs in a separate process. Reads .gz and writes .gz.
    If a line matches the target format with a non-empty CustomerId, writes:
        CustomerId:<value>;path
    Otherwise, writes the original line (log_content;path) to preserve data.
    Returns per-file stats and diagnostic counters.
    """
    local = {
        "file_name": os.path.basename(file_path),
        "lines_scanned": 0,
        "lines_with_id": 0,        # matched and replaced with CustomerId:<val>;path
        "lines_passthrough": 0,    # kept as original because no usable CustomerId found
        "error": None,

        # Diagnostics (why a line didn't match):
        "no_header_prefix": 0,     # missing >=5 bracketed fields at start
        "no_uri_marker": 0,        # missing "<### Request URI/URL:"
        "no_id_token": 0,          # missing "[CustomerId...]" token
        "id_empty": 0,             # has token but empty value
    }

    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    # Clean any stale partial output (so a failed file can be retried)
    try:
        if os.path.exists(out_path):
            os.remove(out_path)
    except Exception:
        pass

    try:
        with gzip.open(file_path, "rt", encoding="utf-8", errors="ignore") as f_in, \
             gzip.open(out_path, "wt", encoding="utf-8", compresslevel=GZIP_LEVEL) as f_out:

            for line in f_in:
                local["lines_scanned"] += 1
                raw_line = line.rstrip("\n")

                # Split "log_content ; path" (path is the last ';'-separated field)
                if ";" in raw_line:
                    log_content, path = raw_line.rsplit(";", 1)
                    log_content = log_content.rstrip()
                    path = path.strip()
                else:
                    log_content, path = raw_line, "UNKNOWN_PATH"

                m = LOG_PATTERN.search(log_content)
                if m:
                    customer_id = m.group(1).strip()
                    if customer_id:
                        # Write normalized output
                        f_out.write(f"CustomerId:{customer_id};{path}\n")
                        local["lines_with_id"] += 1
                    else:
                        # Empty ID -> pass through original; diagnose
                        local["id_empty"] += 1
                        f_out.write(f"{log_content} ; {path}\n")
                        local["lines_passthrough"] += 1
                else:
                    # Didn't match main pattern -> diagnose and pass through original
                    if not HEADER_PROBE.search(log_content):
                        local["no_header_prefix"] += 1
                    elif not URI_PROBE.search(log_content):
                        local["no_uri_marker"] += 1
                    elif not ID_TOKEN_PROBE.search(log_content):
                        local["no_id_token"] += 1
                    else:
                        # token present but likely empty or malformed value
                        valm = ID_VALUE_PROBE.search(log_content)
                        if valm and not valm.group(1).strip():
                            local["id_empty"] += 1
                        else:
                            # Unknown mismatch, treat as pass-through
                            pass
                    f_out.write(f"{log_content} ; {path}\n")
                    local["lines_passthrough"] += 1

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
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Log Filtering Summary Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder:  {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"Max Workers:   {summary['max_workers']}\n")
        f.write(f"GZIP Level:    {summary['gzip_level']}\n\n")

        f.write("=== Files Processed ===\n")
        f.write(f"Processed: {summary['files_scanned']}\n")
        f.write(f"Success:   {summary['files_success']}\n")
        f.write(f"Errors:    {summary['files_error']}\n\n")

        f.write("=== Line Counts ===\n")
        f.write(f"Total lines scanned:  {summary['total_lines_scanned']}\n")
        f.write(f"Lines with CustomerId:{summary['total_lines_with_id']}\n")
        f.write(f"Lines passthrough:    {summary['total_lines_passthrough']}\n\n")

        f.write("=== Non-match Diagnostics ===\n")
        f.write(f"Missing header prefix (>=5 [..]): {summary['no_header_prefix']}\n")
        f.write(f"Missing '<### Request URI/URL:' : {summary['no_uri_marker']}\n")
        f.write(f"Missing '[CustomerId ... ]'     : {summary['no_id_token']}\n")
        f.write(f"Empty CustomerId value          : {summary['id_empty']}\n\n")

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
        if f.endswith(".gz") and os.path.isfile(os.path.join(INPUT_FOLDER, f))
    )

    if not all_files:
        print("No .gz files found in INPUT_FOLDER.", file=sys.stderr)
        # still write an empty summary shell
        summary = {
            "max_workers": MAX_WORKERS,
            "gzip_level": GZIP_LEVEL,
            "files_scanned": 0,
            "files_success": 0,
            "files_error": 0,
            "total_lines_scanned": 0,
            "total_lines_with_id": 0,
            "total_lines_passthrough": 0,
            "no_header_prefix": 0,
            "no_uri_marker": 0,
            "no_id_token": 0,
            "id_empty": 0,
            "errors": [],
        }
        write_summary(summary)
        return

    completed = load_completed_set(RESUME_LOG)
    pending_files = [fp for fp in all_files if os.path.basename(fp) not in completed]

    if not pending_files:
        print("All files already processed per resume log. Nothing to do.")
        # still write a summary shell
        summary = {
            "max_workers": MAX_WORKERS,
            "gzip_level": GZIP_LEVEL,
            "files_scanned": 0,
            "files_success": 0,
            "files_error": 0,
            "total_lines_scanned": 0,
            "total_lines_with_id": 0,
            "total_lines_passthrough": 0,
            "no_header_prefix": 0,
            "no_uri_marker": 0,
            "no_id_token": 0,
            "id_empty": 0,
            "errors": [],
        }
        write_summary(summary)
        return

    summary = {
        "max_workers": MAX_WORKERS,
        "gzip_level": GZIP_LEVEL,
        "files_scanned": 0,
        "files_success": 0,
        "files_error": 0,
        "total_lines_scanned": 0,
        "total_lines_with_id": 0,
        "total_lines_passthrough": 0,
        "no_header_prefix": 0,
        "no_uri_marker": 0,
        "no_id_token": 0,
        "id_empty": 0,
        "errors": [],
    }

    overall_bar = tqdm(total=len(pending_files), desc="Overall", unit="file", leave=True)

    try:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(process_file, fp): fp for fp in pending_files}

            for fut in as_completed(futures):
                file_path = futures[fut]
                base_name = os.path.basename(file_path)

                try:
                    local = fut.result()
                    summary["files_scanned"] += 1
                    summary["total_lines_scanned"]    += local["lines_scanned"]
                    summary["total_lines_with_id"]    += local["lines_with_id"]
                    summary["total_lines_passthrough"]+= local["lines_passthrough"]
                    summary["no_header_prefix"]       += local["no_header_prefix"]
                    summary["no_uri_marker"]          += local["no_uri_marker"]
                    summary["no_id_token"]            += local["no_id_token"]
                    summary["id_empty"]               += local["id_empty"]

                    if local["error"]:
                        summary["files_error"] += 1
                        summary["errors"].append(local["error"])
                    else:
                        summary["files_success"] += 1
                        append_completed(RESUME_LOG, base_name)

                except Exception as e:
                    summary["files_scanned"] += 1
                    summary["files_error"] += 1
                    summary["errors"].append(f"{base_name}: worker exception: {e}")

                overall_bar.update(1)

                # Optional ETA display
                if summary["files_scanned"] > 0:
                    elapsed = time.time() - (time.time() - overall_bar.start_t) if hasattr(overall_bar, 'start_t') else None
                    start_ts = getattr(overall_bar, 'start_ts', None)

    finally:
        overall_bar.close()
        write_summary(summary)

if __name__ == "__main__":
    main()
