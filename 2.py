import os
import sys
import time
import gzip
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from tqdm import tqdm
from threading import Lock

# ====== CONFIGURATION ====== #
INPUT_FOLDER = "input_logs"       # Folder with .gz inputs (non-recursive)
OUTPUT_FOLDER = "cleaned_output"  # Cleaned outputs as .gz (same basenames)
SUMMARY_FILE = "summary_report.txt"
RESUME_LOG = "resume_files.log"
DEBUG_LOG = "debug.log"
MAX_WORKERS = 6
GZIP_LEVEL = 6
# =========================== #

FRAGMENTS_TO_REMOVE = [
    "channel-RETP, ",
    "useragent-null, ",
    "deviceid-null, ",
    "customerHandleNumber=null, ",
    "apptype-RET, ",
    "customerHandleType=null, ",
    "imeiInfo-null, ",
    "processorInfo-null, ",
    "deviceInfo-null, ",
    "Version-null, ",
    "browserType=null, ",
    "latitude=null, ",
    "longitude-null, ",
    "callerEntity-null, ",
    "mobileNumber=null, ",
    "ipAddress=null, ",
    "@sApilevel=null, ",
    "appVersion-null, ",
    "Exnid-null, ",
    "fesessionId-null, ",
    "jwtayload-null, ",
    "requestip-null, ",
    "prmnumber-null, ",
    "vprRequestAppType=null, ",
    "lastRequestShopPhoto=null"
]

# Lock for thread-safe debug logging
debug_lock = Lock()

def sniff_text_encoding_gz(path: str) -> str:
    """Quick BOM sniff for gzipped text. Falls back to utf-8."""
    with gzip.open(path, "rb") as f:
        head = f.read(4)
    if head.startswith(b"\xff\xfe"):
        return "utf-16-le"
    if head.startswith(b"\xfe\xff"):
        return "utf-16-be"
    if head.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    return "utf-8"

def log_debug(file_name, encoding, samples_in, samples_out, lines_processed, lines_written):
    """Thread-safe debug logger for each file."""
    with debug_lock:
        with open(DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n=== DEBUG for {file_name} ===\n")
            f.write(f"Encoding: {encoding}\n")
            f.write(f"Lines processed: {lines_processed}, Lines written: {lines_written}\n")
            f.write("Sample input lines:\n")
            for ln in samples_in:
                f.write("  IN : " + repr(ln[:200]) + "\n")
            f.write("Sample cleaned lines:\n")
            for ln in samples_out:
                f.write("  OUT: " + repr(ln[:200]) + "\n")
            f.write("="*40 + "\n")

def process_file(file_path: str) -> dict:
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_written": 0,
        "encoding": None,
        "error": None,
    }
    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    try:
        if os.path.exists(out_path):
            os.remove(out_path)
    except Exception:
        pass

    samples_in, samples_out = [], []

    try:
        enc = sniff_text_encoding_gz(file_path)
        local["encoding"] = enc

        with gzip.open(file_path, "rt", encoding=enc, errors="replace") as f_in, \
             gzip.open(out_path, "wt", encoding="utf-8", compresslevel=GZIP_LEVEL) as f_out:

            for line in f_in:
                local["lines_processed"] += 1
                if len(samples_in) < 5:
                    samples_in.append(line)

                cleaned = line
                for frag in FRAGMENTS_TO_REMOVE:
                    cleaned = cleaned.replace(frag, "")

                if cleaned.strip():
                    f_out.write(cleaned)
                    local["lines_written"] += 1
                    if len(samples_out) < 5:
                        samples_out.append(cleaned)
                else:
                    f_out.write("\n")
                    if len(samples_out) < 5:
                        samples_out.append("\\n")

        # Debug dump
        log_debug(local["file_name"], enc, samples_in, samples_out,
                  local["lines_processed"], local["lines_written"])

        if local["lines_processed"] > 0 and local["lines_written"] == 0:
            local["error"] = f"{local['file_name']} → 0 non-empty lines written"

    except Exception as e:
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

def write_summary(summary_data):
    summary_data["end_ts"] = time.time()
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Log Cleaning Summary Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder:  {os.path.abspath(summary_data['input_folder'])}\n")
        f.write(f"Output Folder: {os.path.abspath(summary_data['output_folder'])}\n")
        f.write(f"Max Workers:   {summary_data['max_workers']}\n\n")

        f.write("=== Files ===\n")
        f.write(f"Processed: {summary_data['files_scanned']}\n")
        f.write(f"Success:   {summary_data['files_success']}\n")
        f.write(f"Errors:    {summary_data['files_error']}\n\n")

        f.write("=== Lines ===\n")
        f.write(f"Total lines processed: {summary_data['total_lines_processed']}\n")
        f.write(f"Total lines written:   {summary_data['total_lines_written']}\n\n")

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
        "input_folder": INPUT_FOLDER,
        "output_folder": OUTPUT_FOLDER,
        "start_ts": time.time(),
        "end_ts": None,
        "max_workers": MAX_WORKERS,
        "files_scanned": 0,
        "files_success": 0,
        "files_error": 0,
        "total_lines_processed": 0,
        "total_lines_written": 0,
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
                    local = fut.result()
                    summary["files_scanned"] += 1
                    summary["total_lines_processed"] += local["lines_processed"]
                    summary["total_lines_written"] += local["lines_written"]

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

                if summary["files_scanned"] > 0:
                    elapsed = time.time() - summary["start_ts"]
                    avg_per = elapsed / summary["files_scanned"]
                    remain = len(pending_files) - summary["files_scanned"]
                    eta_seconds = int(remain * avg_per)
                    overall_bar.set_postfix_str(f"ETA: {timedelta(seconds=eta_seconds)}")

    finally:
        overall_bar.close()
        write_summary(summary)

if __name__ == "__main__":
    main()
