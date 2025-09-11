#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import random
import traceback
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm

# ========= CONFIGURATION ========= #
INPUT_FOLDER = "input_logs"          # Folder with .txt inputs
OUTPUT_FOLDER = "field_examples"     # Folder for outputs
SUMMARY_FILE = "summary_report.txt"  # Saved in current working dir
CHUNK_SIZE = 10000                   # Max lines per output file
MAX_WORKERS = 6                      # Parallel workers
ALLOWED_EXTS = (".txt",)             # Process only .txt files
# ================================= #

summary = {
    "files_scanned": 0,
    "total_lines": 0,
    "unique_fields": 0,
    "examples_written": 0,
    "errors": [],
    "per_field": {}  # field -> {count, examples}
}

def ensure_folders():
    Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

def parse_line(line: str):
    """
    Parse a line from the right:
    log line ; path ; field ; regex type ; regex match
    Returns: (path, field, regex_type, regex_match)
    """
    parts = [p.strip() for p in line.strip().split(";")]
    if len(parts) < 5:
        return None
    regex_match = parts[-1]
    regex_type = parts[-2]
    field = parts[-3]
    path = parts[-4]
    return path, field, regex_type, regex_match

def process_file(file_path: Path) -> dict:
    local_counts = defaultdict(int)
    local_examples = defaultdict(list)
    local_stats = {"lines_in": 0, "errors": None}

    try:
        with file_path.open("r", encoding="utf-8", errors="ignore") as fin:
            for line in fin:
                if not line.strip():
                    continue
                local_stats["lines_in"] += 1
                parsed = parse_line(line)
                if not parsed:
                    continue
                path, field, regex_type, regex_match = parsed
                local_counts[field] += 1
                local_examples[field].append((path, field, regex_type, regex_match))

    except Exception as e:
        local_stats["errors"] = f"{file_path}: {e}"

    return {"counts": dict(local_counts), "examples": dict(local_examples), "stats": local_stats}

def merge_results(results):
    global_counts = defaultdict(int)
    global_examples = defaultdict(list)

    for result in results:
        for field, cnt in result["counts"].items():
            global_counts[field] += cnt
        for field, exs in result["examples"].items():
            global_examples[field].extend(exs)

    return global_counts, global_examples

def select_examples(global_examples):
    chosen = {}
    for field, exs in global_examples.items():
        # Shuffle to randomize
        random.shuffle(exs)

        # Prefer unique paths
        seen_paths = set()
        picked = []
        for ex in exs:
            if ex[0] not in seen_paths:
                picked.append(ex)
                seen_paths.add(ex[0])
            if len(picked) == 3:
                break

        # If fewer than 3 and still examples left, fill randomly
        if len(picked) < 3:
            remaining = [ex for ex in exs if ex not in picked]
            random.shuffle(remaining)
            picked.extend(remaining[: 3 - len(picked)])

        chosen[field] = picked
    return chosen

def write_output(chosen_examples, global_counts):
    lines_written = 0
    file_index = 1
    current_count = 0
    output_file = None

    def new_output_file(idx):
        return Path(OUTPUT_FOLDER) / f"output_{idx:04d}.txt"

    output_path = new_output_file(file_index)
    fout = output_path.open("w", encoding="utf-8")

    for field in sorted(chosen_examples.keys()):
        count = global_counts[field]
        fout.write(f"{field} ({count})\n")
        lines_written += 1
        current_count += 1

        for ex in chosen_examples[field]:
            line = " ; ".join(ex)
            fout.write(line + "\n")
            lines_written += 1
            current_count += 1

        fout.write("\n")
        lines_written += 1
        current_count += 1

        # Rotate file if reached chunk size
        if current_count >= CHUNK_SIZE:
            fout.close()
            file_index += 1
            output_path = new_output_file(file_index)
            fout = output_path.open("w", encoding="utf-8")
            current_count = 0

    fout.close()
    return lines_written, file_index

def write_summary():
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write("Summary Report\n")
        f.write("====================\n")
        f.write(f"Input Folder: {INPUT_FOLDER}\n")
        f.write(f"Output Folder: {OUTPUT_FOLDER}\n\n")
        f.write(f"Total Files Scanned: {summary['files_scanned']}\n")
        f.write(f"Total Lines Read: {summary['total_lines']}\n")
        f.write(f"Total Unique Fields: {summary['unique_fields']}\n")
        f.write(f"Total Examples Written: {summary['examples_written']}\n\n")

        f.write("Per-field Counts:\n")
        for field, data in sorted(summary["per_field"].items()):
            f.write(f"- {field}: {data['count']} occurrences, {data['examples']} examples written\n")

        f.write("\n")
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

    results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_file, f): f for f in files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
            result = future.result()
            results.append(result)
            summary["files_scanned"] += 1
            summary["total_lines"] += result["stats"]["lines_in"]
            if result["stats"]["errors"]:
                summary["errors"].append(result["stats"]["errors"])

    global_counts, global_examples = merge_results(results)
    chosen_examples = select_examples(global_examples)

    lines_written, num_files = write_output(chosen_examples, global_counts)

    summary["unique_fields"] = len(global_counts)
    summary["examples_written"] = lines_written
    for field in global_counts:
        summary["per_field"][field] = {
            "count": global_counts[field],
            "examples": len(chosen_examples[field])
        }

    write_summary()
    print(f"Processing complete. {num_files} output files written. See summary_report.txt for details.")

if __name__ == "__main__":
    main()
