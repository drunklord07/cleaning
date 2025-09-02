#!/usr/bin/env python3
import os
import re
import sys
import time
import shutil
import tempfile
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from tqdm import tqdm

# ================= CONFIG =================
INPUT_FOLDER              = "input_txt"          # non-recursive .txt inputs (read-only)
OUTPUT_ROOT               = "output_run"         # all outputs live inside this root

WORKING_COPY_FOLDER       = os.path.join(OUTPUT_ROOT, "working_copy")       # processed copy of inputs
OUTPUT_MATCHES_FOLDER     = os.path.join(OUTPUT_ROOT, "extracted_matches")  # aggregated keyword lines (chunked)
STAGING_FOLDER            = os.path.join(OUTPUT_ROOT, "_staging_matches")   # per-file staging before chunking
SUMMARY_FILE              = os.path.join(OUTPUT_ROOT, "summary_report.txt")
RESUME_LOG                = os.path.join(OUTPUT_ROOT, "resume_files.log")

MAX_WORKERS               = 6
ALLOWED_EXTS              = (".txt",)

# Matching behavior
KEYWORDS = [
    "there are so many people",  # add more phrases here
]
USE_REGEX         = False        # False=literals, True=regex patterns
CASE_INSENSITIVE  = True         # applies to literals and regex

# Limits
MAX_LINES_PER_MATCH_FILE = 10_000  # chunk size for extracted output files
MAX_LINES_PER_WORK_FILE  = 10_000  # enforce this on WORKING_COPY_FOLDER after extraction

# Clean output root at start (safe because we never touch INPUT_FOLDER)
CLEAN_OUTPUT_ROOT_AT_START = True
# =========================================

# ---------- Keyword prep ----------
def _compile_keywords():
    if USE_REGEX:
        flags = re.IGNORECASE if CASE_INSENSITIVE else 0
        return [re.compile(p, flags) for p in KEYWORDS]
    else:
        return [k.lower() if CASE_INSENSITIVE else k for k in KEYWORDS]

KW_OBJS = _compile_keywords()

def line_has_keyword(line: str) -> bool:
    if not KW_OBJS:
        return False
    if USE_REGEX:
        return any(rx.search(line) for rx in KW_OBJS)
    hay = line.lower() if CASE_INSENSITIVE else line
    return any(kw in hay for kw in KW_OBJS)

# ---------- Filesystem helpers ----------
def atomic_rewrite(src_path: str, kept_lines_iter):
    """Write kept_lines_iter to a temp file in the same dir, then os.replace() over src_path."""
    folder = os.path.dirname(src_path) or "."
    os.makedirs(folder, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=folder, encoding="utf-8") as fout:
        tmp_path = fout.name
        for s in kept_lines_iter:
            fout.write(s)
        fout.flush()
        os.fsync(fout.fileno())
    os.replace(tmp_path, src_path)  # atomic on same FS

def sync_input_to_working_copy():
    """Create a fresh working copy of INPUT_FOLDER (non-recursive, .txt only)."""
    if CLEAN_OUTPUT_ROOT_AT_START and os.path.isdir(OUTPUT_ROOT):
        shutil.rmtree(OUTPUT_ROOT, ignore_errors=True)

    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    os.makedirs(WORKING_COPY_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_MATCHES_FOLDER, exist_ok=True)
    os.makedirs(STAGING_FOLDER, exist_ok=True)

    copied = 0
    for name in sorted(os.listdir(INPUT_FOLDER)):
        src = os.path.join(INPUT_FOLDER, name)
        if not os.path.isfile(src):
            continue
        if os.path.splitext(name)[1].lower() not in ALLOWED_EXTS:
            continue
        dst = os.path.join(WORKING_COPY_FOLDER, name)
        shutil.copy2(src, dst)
        copied += 1
    return copied

# ---------- Worker (operates ONLY on working copy) ----------
def process_file(file_path: str) -> dict:
    """
    For a single .txt in WORKING_COPY_FOLDER:
      - Extract lines with keywords -> write to a staging file.
      - Keep non-matching lines -> rewrite working copy via atomic replace.
    """
    local = {
        "file_name": os.path.basename(file_path),
        "lines_scanned": 0,
        "lines_extracted": 0,
        "lines_kept": 0,
        "staging_path": None,
        "error": None,
        "input_was_blank": False,
    }

    base = os.path.basename(file_path)
    staging_path = os.path.join(STAGING_FOLDER, f"{base}.staging.txt")

    # Clean stale staging from prior attempts
    try:
        if os.path.exists(staging_path):
            os.remove(staging_path)
    except Exception:
        pass

    try:
        os.makedirs(os.path.dirname(staging_path) or ".", exist_ok=True)
        with open(file_path, "r", encoding="utf-8", errors="ignore") as fin, \
             open(staging_path, "w", encoding="utf-8") as fstage:
            any_line = False
            kept = []
            for line in fin:
                any_line = True
                local["lines_scanned"] += 1
                if line_has_keyword(line):
                    fstage.write(line)
                    local["lines_extracted"] += 1
                else:
                    kept.append(line)
                    local["lines_kept"] += 1

            if not any_line:
                local["input_was_blank"] = True

        # Rewrite working copy with kept lines (even if zero)
        atomic_rewrite(file_path, kept)

        # If nothing was extracted, remove empty staging and unset path
        if local["lines_extracted"] == 0:
            try:
                os.remove(staging_path)
            except FileNotFoundError:
                pass
            staging_path = None

        local["staging_path"] = staging_path

    except Exception as e:
        # Clean partial staging
        try:
            if os.path.exists(staging_path):
                os.remove(staging_path)
        except Exception:
            pass
        local["error"] = f"{local['file_name']}: {e.__class__.__name__}: {e}\n" + \
                         "".join(traceback.format_exception_only(type(e), e)).strip()

    return local

# ---------- Merge staging into chunked outputs ----------
def chunk_staging_files(staging_paths, out_folder, base_prefix, max_lines_per_file):
    """
    Read staging files in deterministic order (by basename), write to matches_00001.txt, etc.
    Remove staging files after merging.
    """
    if not staging_paths:
        return 0, 0  # num_out_files, num_out_lines

    os.makedirs(out_folder, exist_ok=True)
    # Sort by source file basename for deterministic order
    staging_paths = sorted(staging_paths, key=lambda p: os.path.basename(p))

    out_index = 1
    out_line_count = 0
    total_written = 0
    total_files = 0
    out_handle = None

    def open_new():
        nonlocal out_index, out_line_count, out_handle, total_files
        if out_handle:
            out_handle.flush()
            out_handle.close()
        out_path = os.path.join(out_folder, f"{base_prefix}_{out_index:05d}.txt")
        out_handle = open(out_path, "w", encoding="utf-8")
        out_line_count = 0
        total_files += 1

    def ensure_capacity():
        nonlocal out_line_count
        if out_handle is None or out_line_count >= max_lines_per_file:
            open_new()

    try:
        for spath in staging_paths:
            with open(spath, "r", encoding="utf-8", errors="ignore") as fin:
                for line in fin:
                    ensure_capacity()
                    out_handle.write(line)
                    out_line_count += 1
                    total_written += 1
            # remove staging after successful merge
            try:
                os.remove(spath)
            except FileNotFoundError:
                pass
    finally:
        if out_handle:
            out_handle.flush()
            out_handle.close()

    return total_files, total_written

# ---------- Enforce max-line policy on WORKING_COPY_FOLDER ----------
def enforce_max_lines_in_working_copy(max_lines=MAX_LINES_PER_WORK_FILE):
    files_split = 0
    parts_created = 0

    for name in sorted(os.listdir(WORKING_COPY_FOLDER)):
        if not name.lower().endswith(".txt"):
            continue
        path = os.path.join(WORKING_COPY_FOLDER, name)
        if not os.path.isfile(path):
            continue

        # Count lines
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                count = sum(1 for _ in f)
        except Exception:
            continue

        if count <= max_lines:
            continue

        # Split into parts
        files_split += 1
        base, ext = os.path.splitext(name)
        part_idx = 1
        lines_in_part = 0
        out = None
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fin:
                for line in fin:
                    if out is None or lines_in_part >= max_lines:
                        if out:
                            out.flush(); out.close()
                        out_name = f"{base}.part{part_idx:03d}{ext}"
                        out_path = os.path.join(WORKING_COPY_FOLDER, out_name)
                        out = open(out_path, "w", encoding="utf-8")
                        lines_in_part = 0
                        part_idx += 1
                        parts_created += 1
                    out.write(line)
                    lines_in_part += 1
        finally:
            if out:
                out.flush(); out.close()
        # Remove original after successful split
        try:
            os.remove(path)
        except Exception:
            pass

    return files_split, parts_created

# ---------- Resume helpers ----------
def load_completed_set(log_path: str) -> set:
    done = set()
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                nm = line.strip()
                if nm and not nm.startswith("#"):
                    done.add(nm)
    return done

def append_completed(log_path: str, file_name: str):
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(file_name + "\n")

# ---------- Summary ----------
def write_summary(summary):
    os.makedirs(os.path.dirname(SUMMARY_FILE) or ".", exist_ok=True)
    summary["end_ts"] = time.time()
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Keyword Extract & Split (.txt, working copy) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder (read-only): {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Root:              {os.path.abspath(OUTPUT_ROOT)}\n")
        f.write(f"Working Copy:             {os.path.abspath(WORKING_COPY_FOLDER)}\n")
        f.write(f"Matches Output:           {os.path.abspath(OUTPUT_MATCHES_FOLDER)}\n")
        f.write(f"Staging Folder:           {os.path.abspath(STAGING_FOLDER)}\n")
        f.write(f"Max Workers:              {summary['max_workers']}\n")
        f.write(f"Keywords mode:            {'REGEX' if USE_REGEX else 'LITERAL'} | Case-insensitive: {CASE_INSENSITIVE}\n")
        f.write("Keywords:\n")
        for k in KEYWORDS:
            f.write(f"  - {k}\n")
        f.write("\n")

        f.write("=== Files ===\n")
        f.write(f"Copied from input:  {summary['files_copied']}\n")
        f.write(f"Processed:          {summary['files_scanned']}\n")
        f.write(f"Success:            {summary['files_success']}\n")
        f.write(f"Errors:             {summary['files_error']}\n")
        f.write(f"Blank inputs:       {len(summary['blank_input_files'])}\n\n")

        f.write("=== Lines (aggregate) ===\n")
        f.write(f"Total scanned:      {summary['total_lines_scanned']}\n")
        f.write(f"Total extracted:    {summary['total_lines_extracted']}\n")
        f.write(f"Total kept:         {summary['total_lines_kept']}\n\n")

        f.write("=== Matches Output ===\n")
        f.write(f"Chunk files created: {summary['match_files_created']}\n")
        f.write(f"Lines written out:   {summary['match_lines_written']}\n\n")

        f.write("=== Working Copy Post-Check ===\n")
        f.write(f"Files split (> {MAX_LINES_PER_WORK_FILE}): {summary['files_split']}\n")
        f.write(f"Parts created:       {summary['parts_created']}\n")

        if summary["errors"]:
            f.write("\n=== Errors ===\n")
            for e in summary["errors"]:
                f.write(f"- {e}\n")

# ---------- Main ----------
def main():
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: INPUT_FOLDER does not exist: {INPUT_FOLDER}", file=sys.stderr)
        sys.exit(1)

    # Build fresh OUTPUT_ROOT & working copy
    files_copied = sync_input_to_working_copy()
    if files_copied == 0:
        print(f"No {ALLOWED_EXTS} files found in INPUT_FOLDER.", file=sys.stderr)
        summary = {
            "start_ts": time.time(), "end_ts": None, "max_workers": MAX_WORKERS,
            "files_copied": 0, "files_scanned": 0, "files_success": 0, "files_error": 0,
            "blank_input_files": [], "errors": [],
            "total_lines_scanned": 0, "total_lines_extracted": 0, "total_lines_kept": 0,
            "match_files_created": 0, "match_lines_written": 0,
            "files_split": 0, "parts_created": 0,
        }
        write_summary(summary)
        return

    # Discover .txt files in WORKING_COPY_FOLDER
    all_files = sorted(
        os.path.join(WORKING_COPY_FOLDER, fn)
        for fn in os.listdir(WORKING_COPY_FOLDER)
        if os.path.isfile(os.path.join(WORKING_COPY_FOLDER, fn))
        and os.path.splitext(fn)[1].lower() in ALLOWED_EXTS
    )

    completed = load_completed_set(RESUME_LOG)
    pending = [fp for fp in all_files if os.path.basename(fp) not in completed]
    if not pending:
        print("All files already processed per resume log. Nothing to do.")
        files_split, parts_created = enforce_max_lines_in_working_copy()
        summary = {
            "start_ts": time.time(), "end_ts": None, "max_workers": MAX_WORKERS,
            "files_copied": files_copied, "files_scanned": 0, "files_success": 0, "files_error": 0,
            "blank_input_files": [], "errors": [],
            "total_lines_scanned": 0, "total_lines_extracted": 0, "total_lines_kept": 0,
            "match_files_created": 0, "match_lines_written": 0,
            "files_split": files_split, "parts_created": parts_created,
        }
        write_summary(summary)
        return

    summary = {
        "start_ts": time.time(), "end_ts": None, "max_workers": MAX_WORKERS,
        "files_copied": files_copied,
        "files_scanned": 0, "files_success": 0, "files_error": 0,
        "blank_input_files": [], "errors": [],
        "total_lines_scanned": 0, "total_lines_extracted": 0, "total_lines_kept": 0,
        "match_files_created": 0, "match_lines_written": 0,
        "files_split": 0, "parts_created": 0,
    }

    staging_collected = []  # staging paths with extracted lines

    overall = tqdm(total=len(pending), desc="Extract+Rewrite (working copy)", unit="file", leave=True)
    try:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(process_file, fp): fp for fp in pending}

            for fut in as_completed(futures):
                src = futures[fut]
                base = os.path.basename(src)
                try:
                    res = fut.result()
                except Exception as e:
                    summary["files_scanned"] += 1
                    summary["files_error"] += 1
                    summary["errors"].append(f"{base}: worker exception: {e}")
                    overall.update(1)
                    continue

                summary["files_scanned"] += 1
                summary["total_lines_scanned"] += res["lines_scanned"]
                summary["total_lines_extracted"] += res["lines_extracted"]
                summary["total_lines_kept"] += res["lines_kept"]

                if res["input_was_blank"]:
                    summary["blank_input_files"].append(res["file_name"])

                if res["error"]:
                    summary["files_error"] += 1
                    summary["errors"].append(res["error"])
                else:
                    summary["files_success"] += 1
                    append_completed(RESUME_LOG, base)
                    if res["staging_path"]:
                        staging_collected.append(res["staging_path"])

                overall.update(1)
    finally:
        overall.close()

    # Merge staging into chunked matches_XXXXX.txt files
    n_out_files, n_out_lines = chunk_staging_files(
        staging_collected,
        OUTPUT_MATCHES_FOLDER,
        "matches",
        MAX_LINES_PER_MATCH_FILE,
    )
    summary["match_files_created"] = n_out_files
    summary["match_lines_written"] = n_out_lines

    # Post-pass: enforce max-line policy on WORKING_COPY_FOLDER
    files_split, parts_created = enforce_max_lines_in_working_copy(MAX_LINES_PER_WORK_FILE)
    summary["files_split"] = files_split
    summary["parts_created"] = parts_created

    write_summary(summary)

if __name__ == "__main__":
    main()
