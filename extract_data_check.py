#!/usr/bin/env python3
import os
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm

# ========= CONFIGURATION ========= #
INPUT_FOLDER = "input_logs"          # Folder with .txt inputs (non-recursive)
OUTPUT_FOLDER = "cleaned_output"     # Folder for cleaned outputs
SUMMARY_FILE = "summary_report.txt"  # Saved in current working dir
MAX_WORKERS = 6                      # Parallel workers
ALLOWED_EXTS = (".txt",)             # Process only .txt files
# ================================= #

summary = {
    "files_scanned": 0,
    "total_lines": 0,
    "lines_written": 0,
    "errors": []
}

def ensure_folders():
    Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

def process_file(file_path: Path) -> dict:
    """
    Process a single .txt file:
    - Keeps only the last 4 fields (from the right).
    - Writes cleaned lines to output file.
    """
    local_stats = {"lines_in": 0, "lines_out": 0, "errors": None}

    try:
        output_path = Path(OUTPUT_FOLDER) / file_path.name
        with file_path.open("r", encoding="utf-8", errors="ignore") as fin, \
             output_path.open("w", encoding="utf-8") as fout:

            for line in fin:
                line = line.strip()
                if not line:
                    continue
                local_stats["lines_in"] += 1

                parts = [p.strip() for p in line.split(";")]
                if len(parts) >= 5:
                    # Keep last 4 fields from the right
                    cleaned_parts = parts[-4:]
                    cleaned = " ; ".join(cleaned_parts)
                    fout.write(cleaned + "\n")
                    local_stats["lines_out"] += 1
                else:
                    # Skip malformed lines (<5 fields)
                    continue

    except Exception as e:
        local_stats["errors"] = f"{file_path}: {e}"
    return local_stats

def write_summary():
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write("Summary Report\n")
        f.write("====================\n")
        f.write(f"Input Folder: {INPUT_FOLDER}\n")
        f.write(f"Output Folder: {OUTPUT_FOLDER}\n\n")
        f.write(f"Total Files Scanned: {summary['files_scanned']}\n")
        f.write(f"Total Lines Read: {summary['total_lines']}\n")
        f.write(f"Total Lines Written: {summary['lines_written']}\n\n")

        if summary["errors"]:
            f.write("Errors:\n")
            for err in summary["errors"]:
                f.write(f"- {err}\n")
        else:
            f.write("No errors encountered.\n")

def main():
    ensure_folders()
    input_dir = Path(INPUT_FOLDER)
    files = [f for f in input_dir.glob("*") if f.suffix in ALLOWED_EXTS]

    if not files:
        print("No .txt files found in input folder.")
        return

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_file, f): f for f in files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
            result = future.result()
            summary["files_scanned"] += 1
            summary["total_lines"] += result["lines_in"]
            summary["lines_written"] += result["lines_out"]
            if result["errors"]:
                summary["errors"].append(result["errors"])

    write_summary()
    print("Processing complete. See summary_report.txt for details.")

if __name__ == "__main__":
    main()
