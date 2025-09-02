#!/usr/bin/env python3
import os
import re
import sys
import shutil
from datetime import datetime

# ========= CONFIG =========
INPUT_FOLDER  = "input_txt"           # read-only .txt files (non-recursive)
EXTRACTED_DIR = "extracted"           # where matching lines go (chunked)
ORIGINAL_DIR  = "new_original"        # where non-matching lines go (chunked)
SUMMARY_FILE  = "summary_report.txt"

ALLOWED_EXTS  = (".txt",)
MAX_LINES_PER_FILE = 10_000           # chunk size per output file

# Matching behavior
KEYWORDS = [
    "there are so many people",       # add more phrases here
]
USE_REGEX        = False              # False = literal substrings, True = regex patterns
CASE_INSENSITIVE = True               # applies to both literal and regex
CLEAN_OUTPUTS_AT_START = True         # wipe EXTRACTED_DIR and ORIGINAL_DIR at start
# ==========================

def compile_keywords():
    if USE_REGEX:
        flags = re.IGNORECASE if CASE_INSENSITIVE else 0
        return [re.compile(p, flags) for p in KEYWORDS]
    # literals
    return [k.lower() if CASE_INSENSITIVE else k for k in KEYWORDS]

KW_OBJS = compile_keywords()

def line_matches(s: str) -> bool:
    if not KW_OBJS:
        return False
    if USE_REGEX:
        return any(rx.search(s) for rx in KW_OBJS)
    hay = s.lower() if CASE_INSENSITIVE else s
    return any(kw in hay for kw in KW_OBJS)

class ChunkWriter:
    """Opens chunk files like prefix_00001.txt and writes ≤ max_lines each."""
    def __init__(self, folder: str, prefix: str, max_lines: int):
        self.folder = folder
        self.prefix = prefix
        self.max_lines = max_lines
        self.file_idx = 0
        self.line_count_in_current = 0
        self.handle = None
        self.total_lines_written = 0
        self.total_files_created = 0
        os.makedirs(folder, exist_ok=True)

    def _open_new(self):
        if self.handle:
            self.handle.flush()
            self.handle.close()
        self.file_idx += 1
        name = f"{self.prefix}_{self.file_idx:05d}.txt"
        path = os.path.join(self.folder, name)
        self.handle = open(path, "w", encoding="utf-8")
        self.line_count_in_current = 0
        self.total_files_created += 1

    def write(self, line: str):
        if self.handle is None or self.line_count_in_current >= self.max_lines:
            self._open_new()
        self.handle.write(line)
        self.line_count_in_current += 1
        self.total_lines_written += 1

    def close(self):
        if self.handle:
            self.handle.flush()
            self.handle.close()
            self.handle = None

def clean_outputs():
    if CLEAN_OUTPUTS_AT_START:
        for d in (EXTRACTED_DIR, ORIGINAL_DIR):
            if os.path.isdir(d):
                shutil.rmtree(d, ignore_errors=True)
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    os.makedirs(ORIGINAL_DIR, exist_ok=True)

def discover_inputs():
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: INPUT_FOLDER does not exist: {INPUT_FOLDER}", file=sys.stderr)
        sys.exit(1)
    files = sorted(
        os.path.join(INPUT_FOLDER, fn)
        for fn in os.listdir(INPUT_FOLDER)
        if os.path.isfile(os.path.join(INPUT_FOLDER, fn)) and os.path.splitext(fn)[1].lower() in ALLOWED_EXTS
    )
    if not files:
        print(f"No {ALLOWED_EXTS} files in {INPUT_FOLDER}", file=sys.stderr)
    return files

def write_summary(summary: dict):
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Simple Keyword Split (.txt) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input:         {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Extracted dir: {os.path.abspath(EXTRACTED_DIR)}\n")
        f.write(f"Original dir:  {os.path.abspath(ORIGINAL_DIR)}\n")
        f.write(f"Max lines/file: {MAX_LINES_PER_FILE}\n")
        f.write(f"Mode: {'REGEX' if USE_REGEX else 'LITERAL'} | Case-insensitive: {CASE_INSENSITIVE}\n")
        f.write("Keywords:\n")
        for k in KEYWORDS:
            f.write(f"  - {k}\n")
        f.write("\n=== Totals ===\n")
        for k in ("files_scanned","lines_scanned","lines_extracted","lines_nonmatch",
                  "extracted_files","original_files"):
            f.write(f"{k.replace('_',' ').title()}: {summary[k]}\n")

def main():
    clean_outputs()
    inputs = discover_inputs()

    extracted_writer = ChunkWriter(EXTRACTED_DIR, "matches", MAX_LINES_PER_FILE)
    original_writer  = ChunkWriter(ORIGINAL_DIR,  "original", MAX_LINES_PER_FILE)

    summary = {
        "files_scanned": 0,
        "lines_scanned": 0,
        "lines_extracted": 0,
        "lines_nonmatch": 0,
        "extracted_files": 0,
        "original_files": 0,
    }

    for path in inputs:
        summary["files_scanned"] += 1
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                summary["lines_scanned"] += 1
                if line_matches(line):
                    extracted_writer.write(line)
                    summary["lines_extracted"] += 1
                else:
                    original_writer.write(line)
                    summary["lines_nonmatch"] += 1

    extracted_writer.close()
    original_writer.close()

    summary["extracted_files"] = extracted_writer.total_files_created
    summary["original_files"]  = original_writer.total_files_created

    write_summary(summary)
    print("Done.")
    print(f"Extracted lines: {summary['lines_extracted']} into {summary['extracted_files']} file(s)")
    print(f"Non-matching  : {summary['lines_nonmatch']} into {summary['original_files']} file(s)")
    print(f"Summary: {os.path.abspath(SUMMARY_FILE)}")

if __name__ == "__main__":
    main()
