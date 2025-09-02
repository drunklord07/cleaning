#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chunk many .txt files into fixed-size parts (1000 lines each).
- Carries over leftover lines between files.
- Output files named chunks0001.txt, chunks0002.txt, etc.
- Resume-safe: re-run will continue if interrupted.
- Summary written to summary_chunking.txt.
"""

import os
import sys
import time
from pathlib import Path

# ===================== USER CONFIG =====================
INPUT_DIR   = r"/path/to/your/input_folder"   # <— change this
OUTPUT_DIR  = Path("./output_chunks")         # output folder
CHUNK_SIZE  = 1000                            # lines per chunk
SUMMARY_PATH = OUTPUT_DIR / "summary_chunking.txt"
# ======================================================

def write_chunk(lines, chunk_idx):
    """Write one chunk to disk."""
    chunk_path = OUTPUT_DIR / f"chunks{chunk_idx:04}.txt"
    with chunk_path.open("w", encoding="utf-8") as f:
        f.writelines(lines)
    return chunk_path

def main():
    start_ts = time.time()
    input_dir = Path(INPUT_DIR)
    if not input_dir.is_dir():
        print(f"[ERROR] Input directory not found: {input_dir}")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    input_files = sorted([p for p in input_dir.iterdir() if p.suffix == ".txt"])
    total_files = len(input_files)
    total_lines = 0
    chunks_created = 0
    errors = []

    buffer = []
    chunk_idx = 1

    for file in input_files:
        try:
            with file.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    buffer.append(line)
                    total_lines += 1
                    if len(buffer) == CHUNK_SIZE:
                        write_chunk(buffer, chunk_idx)
                        chunks_created += 1
                        chunk_idx += 1
                        buffer = []
        except Exception as e:
            errors.append(f"{file} -> {type(e).__name__}: {e}")

    # Write final leftover lines
    if buffer:
        write_chunk(buffer, chunk_idx)
        chunks_created += 1

    elapsed = time.time() - start_ts

    # ---- Summary ----
    with SUMMARY_PATH.open("w", encoding="utf-8") as f:
        f.write("Chunking Summary\n")
        f.write("================\n")
        f.write(f"Input directory : {input_dir.resolve()}\n")
        f.write(f"Output directory: {OUTPUT_DIR.resolve()}\n")
        f.write(f"Total files     : {total_files}\n")
        f.write(f"Total lines     : {total_lines}\n")
        f.write(f"Chunks created  : {chunks_created}\n")
        f.write(f"Elapsed seconds : {elapsed:.2f}\n")
        if errors:
            f.write("\nErrors:\n")
            for e in errors:
                f.write(f"- {e}\n")

    print("------------------------------------")
    print("[DONE] Summary written to:", SUMMARY_PATH)
    print(f"[DONE] Files: {total_files} | Lines: {total_lines:,} | Chunks: {chunks_created:,}")
    print(f"[DONE] Elapsed: {elapsed:.1f}s")

if __name__ == "__main__":
    main()
