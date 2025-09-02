#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# ========= CONFIGURATION ========= #
INPUT_FOLDER = "input_logs"          # Folder with .txt inputs (non-recursive)
OUTPUT_FOLDER = "cleaned_output"     # Folder for chunked outputs
SUMMARY_FILE = "summary_report.txt"  # Saved in current working dir
RESUME_LOG = "resume_files.log"      # Checkpoint log in current working dir
MAX_WORKERS = 6                      # Parallel workers
ALLOWED_EXTS = (".txt",)             # Process only .txt files
CHUNK_SIZE = 1000                    # Lines per chunk
# ================================= #

# ---- Global counters ----
total_lines = 0
chunks_created = 0
errors = []
processed_files = []


def process_file(file_path: Path):
    """
    Read one file and return all its lines.
    This runs in a worker process.
    """
    try:
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        return (str(file_path), lines, None)
    except Exception as e:
        return (str(file_path), [], f"{type(e).__name__}: {e}")


def write_chunk(lines, chunk_idx, output_dir):
    """Write one chunk to disk."""
    chunk_path = output_dir / f"chunks{chunk_idx:04}.txt"
    with chunk_path.open("w", encoding="utf-8") as f:
        f.writelines(lines)
    return chunk_path


def main():
    global total_lines, chunks_created, processed_files, errors

    start_ts = time.time()
    input_dir = Path(INPUT_FOLDER)
    output_dir = Path(OUTPUT_FOLDER)

    if not input_dir.is_dir():
        print(f"[ERROR] Input directory not found: {input_dir}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Gather files
    input_files = [p for p in input_dir.iterdir() if p.suffix in ALLOWED_EXTS]
    total_files = len(input_files)
    print(f"[INFO] Found {total_files} input files.")

    # ---- Phase 1: Parallel read ----
    all_lines = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_file, p): p for p in input_files}
        for i, fut in enumerate(as_completed(futures), 1):
            file_path, lines, err = fut.result()
            if err:
                print(f"[ERROR] {file_path} -> {err}")
                errors.append(f"{file_path} -> {err}")
            else:
                print(f"[READ] {file_path} ({len(lines)} lines)")
                all_lines.extend(lines)
                processed_files.append(file_path)
                total_lines += len(lines)
            if i % 10 == 0 or i == total_files:
                print(f"[STATUS] Reading {i}/{total_files} done...")

    # ---- Phase 2: Chunk writing ----
    buffer = []
    chunk_idx = 1
    for line in all_lines:
        buffer.append(line)
        if len(buffer) == CHUNK_SIZE:
            write_chunk(buffer, chunk_idx, output_dir)
            chunks_created += 1
            chunk_idx += 1
            buffer = []

    # Write leftovers
    if buffer:
        write_chunk(buffer, chunk_idx, output_dir)
        chunks_created += 1

    elapsed = time.time() - start_ts

    # ---- Write summary ----
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write("Chunking Summary\n")
        f.write("================\n")
        f.write(f"Input folder   : {input_dir.resolve()}\n")
        f.write(f"Output folder  : {output_dir.resolve()}\n")
        f.write(f"Total files    : {total_files}\n")
        f.write(f"Total lines    : {total_lines}\n")
        f.write(f"Chunks created : {chunks_created}\n")
        f.write(f"Elapsed (sec)  : {elapsed:.2f}\n")
        if errors:
            f.write("\nErrors:\n")
            for e in errors:
                f.write(f"- {e}\n")

    # ---- Resume log ----
    with open(RESUME_LOG, "w", encoding="utf-8") as f:
        for pf in processed_files:
            f.write(f"{pf}\n")

    print("------------------------------------")
    print(f"[DONE] Files: {total_files} | Lines: {total_lines:,} | Chunks: {chunks_created:,}")
    print(f"[DONE] Summary saved to {SUMMARY_FILE}")
    print(f"[DONE] Resume log saved to {RESUME_LOG}")
    print(f"[DONE] Elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted by user. Re-run to continue.")
        sys.exit(130)
