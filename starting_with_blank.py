#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from tqdm import tqdm

# ===== CONFIGURATION ===== #
INPUT_FOLDER = "mobile_only_output"     # folder with input .txt files
OUTPUT_FOLDER = "cleaned_output"        # folder to write cleaned files
SUMMARY_FILE = "cleanup_summary.txt"    # summary report
ALLOWED_EXTS = (".txt",)
# ========================= #

def process_file(file_path: Path) -> dict:
    stats = {
        "file": file_path.name,
        "total": 0,
        "modified": 0,
        "space": 0,
        "dash": 0,
        "dash_space": 0,
        "specials": []  # collect offending lines
    }

    out_path = Path(OUTPUT_FOLDER) / file_path.name

    with file_path.open("r", encoding="utf-8", errors="ignore") as f_in, \
         out_path.open("w", encoding="utf-8") as f_out:

        for line in f_in:
            stats["total"] += 1
            original = line.rstrip("\n")

            # Case 1: line starts with space
            if original.startswith(" "):
                new_line = original.lstrip(" ")
                stats["modified"] += 1
                stats["space"] += 1

            # Case 2: line starts with "- "
            elif original.startswith("- "):
                new_line = original[2:]
                stats["modified"] += 1
                stats["dash_space"] += 1

            # Case 3: line starts with "-" but not "- "
            elif original.startswith("-"):
                new_line = original[1:]
                stats["modified"] += 1
                stats["dash"] += 1

            # Case 4: other leading special char
            elif original and not original[0].isalnum():
                new_line = original
                stats["specials"].append(original)

            else:
                new_line = original

            f_out.write(new_line + "\n")

    return stats

def write_summary(all_stats: list):
    total_lines = sum(s["total"] for s in all_stats)
    total_modified = sum(s["modified"] for s in all_stats)
    space = sum(s["space"] for s in all_stats)
    dash = sum(s["dash"] for s in all_stats)
    dash_space = sum(s["dash_space"] for s in all_stats)
    specials = sum(len(s["specials"]) for s in all_stats)

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write("=== Cleanup Summary ===\n\n")
        f.write(f"Total lines processed: {total_lines}\n")
        f.write(f"Total lines modified:  {total_modified}\n")
        f.write(f"  - Starting with space:      {space}\n")
        f.write(f"  - Starting with dash only:  {dash}\n")
        f.write(f"  - Starting with '- ':       {dash_space}\n")
        f.write(f"Other special leading chars:  {specials}\n\n")

        if specials > 0:
            f.write("=== Offending lines (other specials) ===\n")
            for s in all_stats:
                if s["specials"]:
                    f.write(f"\nFile: {s['file']}\n")
                    for line in s["specials"]:
                        f.write(f"  {line}\n")

def main():
    input_folder = Path(INPUT_FOLDER)
    output_folder = Path(OUTPUT_FOLDER)
    output_folder.mkdir(parents=True, exist_ok=True)

    files = sorted([f for f in input_folder.iterdir()
                    if f.is_file() and f.suffix.lower() in ALLOWED_EXTS])

    if not files:
        print(f"No {ALLOWED_EXTS} files found in {INPUT_FOLDER}")
        return

    all_stats = []
    for file_path in tqdm(files, desc="Processing files", unit="file"):
        stats = process_file(file_path)
        all_stats.append(stats)

    write_summary(all_stats)
    print(f"\nSummary written to {SUMMARY_FILE}")

if __name__ == "__main__":
    main()
