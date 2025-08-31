#!/usr/bin/env python3
import argparse
import csv
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Tuple, List

MAX_WORKERS = 6

def count_lines_in_file(path: Path) -> Tuple[str, int]:
    """
    Count the number of lines in a text file.
    Uses text mode so a final line without a trailing newline is still counted.
    """
    count = 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for _ in f:
                count += 1
    except Exception:
        # Return -1 to indicate an error; caller can handle/report it.
        return (str(path), -1)
    return (str(path), count)

def find_txt_files(root: Path, recursive: bool = True) -> List[Path]:
    return sorted(root.rglob("*.txt") if recursive else root.glob("*.txt"))

def main():
    parser = argparse.ArgumentParser(
        description="Count lines in all .txt files within a folder (in parallel)."
    )
    parser.add_argument("folder", type=Path, help="Folder containing .txt files")
    parser.add_argument("--out", type=Path, default=None,
                        help="Optional CSV output path (filename,lines)")
    parser.add_argument("--no-recursive", action="store_true",
                        help="Only look at top-level (no subfolders)")
    args = parser.parse_args()

    if not args.folder.exists() or not args.folder.is_dir():
        print(f"Error: '{args.folder}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    files = find_txt_files(args.folder, recursive=not args.no_recursive)
    if not files:
        print("No .txt files found. Nothing to do.")
        sys.exit(0)

    print(f"Found {len(files)} .txt files. Counting lines with {MAX_WORKERS} workers...")

    results: List[Tuple[str, int]] = []
    errors = 0

    # Use ProcessPoolExecutor for parallel counting
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(count_lines_in_file, p): p for p in files}
        for fut in as_completed(future_map):
            fname, line_count = fut.result()
            results.append((fname, line_count))
            if line_count < 0:
                errors += 1

    # Sort results by filename for stable output
    results.sort(key=lambda x: x[0])

    # Write CSV if requested; otherwise, print tabular to stdout
    if args.out:
        with args.out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["filename", "lines"])
            writer.writerows(results)
        print(f"Wrote CSV: {args.out}")
    else:
        # Print a simple table to stdout
        print("\nfilename,lines")
        for fname, line_count in results:
            print(f"{fname},{line_count}")
        print()  # spacing before summary

    # ---- FINAL SUMMARY (printed at the very end) ----
    total_lines = sum(c for _, c in results if c >= 0)
    readable_files = sum(1 for _, c in results if c >= 0)
    print("Summary:")
    print(f"  Files found      : {len(files)}")
    print(f"  Readable files   : {readable_files}")
    print(f"  Unreadable files : {errors}")
    print(f"  Total lines (readable): {total_lines}")

if __name__ == "__main__":
    main()
