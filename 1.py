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
INPUT_FOLDER = "input_logs"      # Folder with .gz inputs (non-recursive)
OUTPUT_FOLDER = "filtered_output" # Filtered outputs as .gz (same basenames)
SUMMARY_FILE = "summary_report.txt"  # Saved in current working dir
RESUME_LOG = "resume_files.log"  # Checkpoint log in current working dir
MAX_WORKERS = 6                  # Use 6–8 for optimal performance
GZIP_LEVEL = 6                   # Increased compression level
# =========================== #

# This regex identifies the specific log format and captures the CustomerId
# It now requires the "‹### Request uri : " string to be present.
LOG_PATTERN = re.compile(
    r'^(?:\[[^]]+\]\s*){7}-\s*‹### Request uri\s*:\s*.*?(?:\[CustomerId:([^]]*)\]).*?$',
    re.DOTALL
)

def process_file(file_path: str) -> dict:
    """
    Runs in a separate process. Filters lines and writes to a new .gz file.
    Output format: CustomerId:value;path or nothing if line is dropped.
    """
    local = {
        "file_name": os.path.basename(file_path),
        "lines_scanned": 0,
        "lines_kept": 0,
        "lines_removed": 0,
        "error": None,
    }
    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    # Clean any stale partial from a previous failed attempt
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

                # Split the log line and the path
                if ";" in raw_line:
                    log_content, path = raw_line.rsplit(";", 1)
                    log_content = log_content.rstrip()
                    path = path.strip()
                else:
                    log_content, path = raw_line, "UNKNOWN_PATH"
                
                match = LOG_PATTERN.search(log_content)

                if match:
                    customer_id = match.group(1).strip()
                    if customer_id:
                        # Keep the line, extract the CustomerId
                        f_out.write(f"CustomerId:{customer_id};{path}\n")
                        local["lines_kept"] += 1
                    else:
                        # No CustomerId found, remove the line
                        local["lines_removed"] += 1
                else:
                    # Line doesn't match the required format, remove it
                    local["lines_removed"] += 1

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
    """Loads a set of completed files from the resume log."""
    completed = set()
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                name = line.strip()
                if name and not line.startswith("#"):
                    completed.add(name)
    return completed

def append_completed(log_path: str, file_name: str):
    """Appends a completed file name to the resume log."""
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(file_name + "\n")

def write_summary(summary_data):
    """Writes the summary report to a file."""
    summary_data["end_ts"] = time.time()
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Log Filtering Summary Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder: {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"Max Workers: {summary_data['max_workers']}\n\n")

        f.write("=== Files Processed ===\n")
        f.write(f"Processed: {summary_data['files_scanned']}\n")
        f.write(f"Success:   {summary_data['files_success']}\n")
        f.write(f"Errors:    {summary_data['files_error']}\n\n")

        f.write("=== Line Counts ===\n")
        f.write(f"Total lines scanned: {summary_data['total_lines_scanned']}\n")
        f.write(f"Lines kept:          {summary_data['total_lines_kept']}\n")
        f.write(f"Lines removed:       {summary_data['total_lines_removed']}\n\n")

        if summary_data["errors"]:
            f.write("=== Errors ===\n")
            for err in summary_data["errors"]:
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
        return

    completed = load_completed_set(RESUME_LOG)
    pending_files = [fp for fp in all_files if os.path.basename(fp) not in completed]

    if not pending_files:
        print("All files already processed per resume log. Nothing to do.")
        return

    summary = {
        "start_ts": time.time(),
        "end_ts": None,
        "max_workers": MAX_WORKERS,
        "files_scanned": 0,
        "files_success": 0,
        "files_error": 0,
        "total_lines_scanned": 0,
        "total_lines_kept": 0,
        "total_lines_removed": 0,
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
                    local_result = fut.result()
                    summary["files_scanned"] += 1
                    summary["total_lines_scanned"] += local_result["lines_scanned"]
                    summary["total_lines_kept"] += local_result["lines_kept"]
                    summary["total_lines_removed"] += local_result["lines_removed"]
                    
                    if local_result["error"]:
                        summary["files_error"] += 1
                        summary["errors"].append(local_result["error"])
                    else:
                        summary["files_success"] += 1
                        append_completed(RESUME_LOG, base_name)
                    
                except Exception as e:
                    summary["files_scanned"] += 1
                    summary["files_error"] += 1
                    summary["errors"].append(f"{base_name}: worker exception: {e}")

                overall_bar.update(1)

                # Calculate and display ETA
                if summary["files_scanned"] > 0:
                    elapsed_time = time.time() - summary["start_ts"]
                    avg_time_per_file = elapsed_time / summary["files_scanned"]
                    remaining_files = len(pending_files) - summary["files_scanned"]
                    eta_seconds = remaining_files * avg_time_per_file
                    eta_delta = timedelta(seconds=int(eta_seconds))
                    overall_bar.set_postfix_str(f"ETA: {str(eta_delta)}")

    finally:
        overall_bar.close()
        write_summary(summary)

if __name__ == "__main__":
    main()
