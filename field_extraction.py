#!/usr/bin/env python3
import os
import re
import sys
import traceback
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime
from collections import defaultdict
import time
import threading

# ========= CONFIGURATION ========= #
INPUT_FOLDER  = "input_logs"         # 🔧 just folder name, script finds it in current dir
OUTPUT_FOLDER = "output_mobile"      # 🔧 created in current dir
FIELDS_FOLDER = Path(OUTPUT_FOLDER) / "fields_identified"
MIRROR_FOLDER = Path(OUTPUT_FOLDER) / "mirror"
SUMMARY_FILE  = Path(OUTPUT_FOLDER) / "summary_mobile_fields.txt"
FIELDS_RUN    = Path(OUTPUT_FOLDER) / "valid_fields_run.txt"
RESUME_FILE   = Path(OUTPUT_FOLDER) / "resume.log"
MAX_WORKERS   = 6
CHUNK_SIZE    = 10000
SUMMARY_REFRESH_INTERVAL = 30        # seconds
# ================================= #

# ✅ MOBILE REGEX
MOBILE_REGEX = re.compile(r'(?<![A-Za-z0-9])(?:91)?[6-9]\d{9}(?![A-Za-z0-9])')

# ✅ Known good/bad fields
VALID_FIELDS = {
    "msisdn", "mobileNo", "contact", "phone",
    "userid", "custID", "customerId"
}
INVALID_FIELDS = {
    "for", "to", "from", "by", "get", "fetch",
    "create", "send", "sms", "message", "text"
}

# ============================================= #
#                PASS 1: DISCOVERY              #
# ============================================= #
def discover_fields(file_path: Path, discovered: set) -> int:
    """Scan file for structured fields and add to discovered set."""
    new_count = 0
    try:
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or ";" not in line:
                    continue
                log_line, path = line.rsplit(";", 1)

                for m in MOBILE_REGEX.finditer(log_line):
                    json_match = re.search(
                        r'["\']?\s*([A-Za-z0-9_\-\. ]+)\s*["\']?\s*[:=\-]\s*["\']?' + re.escape(m.group(0)), log_line
                    )
                    if json_match:
                        discovered.add(json_match.group(1).strip())
                        new_count += 1
                        continue
                    xml_match = re.search(
                        r'<\s*([A-Za-z0-9_\-\. ]+)[^>]*>' + re.escape(m.group(0)), log_line
                    )
                    if xml_match:
                        discovered.add(xml_match.group(1).strip())
                        new_count += 1
                        continue
    except Exception:
        traceback.print_exc()
    return new_count

def discover_worker(file_path: Path):
    """Worker wrapper for discovery (returns a set)."""
    local_discovered = set()
    discover_fields(file_path, local_discovered)
    return local_discovered

# ============================================= #
#            PASS 2: EXTRACTION                 #
# ============================================= #
def extract_lines(file_path: Path, valid_fields: set):
    """Worker: process a file and return results."""
    local_stats = defaultdict(int)
    local_field_counts = defaultdict(int)
    local_examples = {}
    local_extracted = []
    local_mirrored = []
    local_skipped_path_only = []

    try:
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                local_stats["lines_scanned"] += 1
                line = line.strip()
                if not line or ";" not in line:
                    continue
                log_line, path = line.rsplit(";", 1)

                mobiles = list(MOBILE_REGEX.finditer(line))
                if not mobiles:
                    local_stats["no_match"] += 1
                    continue

                log_matches = [m for m in mobiles if m.start() < len(log_line)]
                path_matches = [m for m in mobiles if m.start() > len(log_line)]

                if log_matches and not path_matches:
                    _process_log_matches(
                        log_line, path, log_matches, valid_fields,
                        local_stats, local_field_counts, local_examples,
                        local_extracted, local_mirrored
                    )
                elif log_matches and path_matches:
                    local_stats["both_log_and_path"] += 1
                    _process_log_matches(
                        log_line, path, log_matches, valid_fields,
                        local_stats, local_field_counts, local_examples,
                        local_extracted, local_mirrored
                    )
                elif not log_matches and path_matches:
                    local_stats["path_only"] += 1
                    local_skipped_path_only.append(line)
                else:
                    local_stats["no_match"] += 1
    except Exception:
        traceback.print_exc()

    return (local_stats, local_field_counts, local_examples,
            local_extracted, local_mirrored, local_skipped_path_only, str(file_path))

def _process_log_matches(log_line, path, matches, valid_fields,
                         stats, field_counts, examples, extracted, mirrored):
    matched_any = False
    partial = False
    for m in matches:
        mobile = m.group(0)
        field = _identify_field(log_line, mobile, valid_fields)
        if field:
            stats["valid_extracted"] += 1
            field_counts[field] += 1
            if field not in examples:
                examples[field] = f"{log_line} ; {path} ; {field} ; mobile_regex ; {mobile}"
            extracted.append(f"{log_line} ; {path} ; {field} ; mobile_regex ; {mobile}")
            matched_any = True
        else:
            partial = True
    if partial and matched_any:
        stats["partial_valid"] += 1
    if not matched_any:
        stats["no_field"] += 1
        mirrored.append(f"{log_line} ; {path}")

def _identify_field(log_line, mobile, valid_fields):
    m = re.search(r'["\']?\s*([A-Za-z0-9_\-\. ]+)\s*["\']?\s*[:=\-]\s*["\']?' + re.escape(mobile), log_line)
    if m:
        return m.group(1).strip()
    m = re.search(r'<\s*([A-Za-z0-9_\-\. ]+)[^>]*>' + re.escape(mobile), log_line)
    if m:
        return m.group(1).strip()
    m = re.search(r'([A-Za-z0-9_\-\. ]+)=["\']?' + re.escape(mobile), log_line)
    if m:
        return m.group(1).strip()

    tokens = re.split(r'[\s:;=,_\-\.\|\[\]\{\}\(\)\'"]+', log_line)
    for i, tok in enumerate(tokens):
        if tok == mobile and i > 0:
            candidate = tokens[i-1].strip()
            cand_lower = candidate.lower()
            if cand_lower in (f.lower() for f in valid_fields):
                return candidate
            if cand_lower in (f.lower() for f in INVALID_FIELDS):
                return None
    return None

# ============================================= #
#              SUMMARY WRITER                   #
# ============================================= #
def write_summary(stats, field_counts, examples, skipped_path_only, stage="Final"):
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Summary Report ({stage}) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*40 + "\n\n")

        f.write("---------- Scan Statistics ----------\n")
        f.write(f"Total Lines Scanned : {stats['lines_scanned']}\n")
        f.write(f"Valid Mobile Extracted : {stats['valid_extracted']}\n")
        f.write(f"Lines removed (no mobile matches) : {stats['no_match']}\n")
        f.write(f"Lines removed (path-only matches) : {stats['path_only']}\n")
        f.write(f"Partial-valid lines : {stats['partial_valid']}\n")
        f.write(f"No-field lines (log matches but no valid field) : {stats['no_field']}\n\n")

        f.write("---------- Per-field Counts ----------\n")
        for fld, cnt in sorted(field_counts.items(), key=lambda x: x[0].lower()):
            f.write(f"{fld} = {cnt}\n")
            if fld in examples:
                f.write(f"  Example: {examples[fld]}\n")
        f.write("\n")

        f.write("---------- Path-only Skipped Lines ----------\n")
        for i, line in enumerate(skipped_path_only[:10], 1):
            f.write(f"{i}. {line}\n")
        if len(skipped_path_only) > 10:
            f.write(f"... {len(skipped_path_only)-10} more skipped lines\n")

        f.write("\n---------- Invalid Fields List ----------\n")
        for fld in sorted(INVALID_FIELDS):
            f.write(f"{fld}\n")

def summary_refresher(stats, field_counts, examples, skipped_path_only, stop_event):
    while not stop_event.is_set():
        write_summary(stats, field_counts, examples, skipped_path_only, stage="Live")
        time.sleep(SUMMARY_REFRESH_INTERVAL)

# ============================================= #
#                    MAIN                       #
# ============================================= #
def main():
    input_path = Path(INPUT_FOLDER)
    all_files = [p for p in input_path.rglob("*.txt")]
    if not all_files:
        print("No .txt files found.")
        return

    FIELDS_FOLDER.mkdir(parents=True, exist_ok=True)
    MIRROR_FOLDER.mkdir(parents=True, exist_ok=True)
    Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

    # Resume: read completed files
    completed_files = set()
    if Path(RESUME_FILE).exists():
        with open(RESUME_FILE, "r", encoding="utf-8") as f:
            completed_files = {line.strip() for line in f if line.strip()}

    files_to_process = [f for f in all_files if str(f) not in completed_files]

    stats = defaultdict(int)
    field_counts = defaultdict(int)
    examples = {}
    skipped_path_only = []

    # ---------- PASS 1 ---------- #
    print("PASS 1: Discovering valid fields...")
    discovered = set()
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(discover_worker, f) for f in files_to_process]
        for fut in tqdm(as_completed(futures), total=len(futures)):
            discovered.update(fut.result())
    valid_fields = VALID_FIELDS.union(discovered)
    with open(FIELDS_RUN, "w", encoding="utf-8") as f:
        for fld in sorted(valid_fields):
            f.write(fld + "\n")

    # ---------- PASS 2 ---------- #
    print("PASS 2: Extraction + Writing...")
    stop_event = threading.Event()
    refresher = threading.Thread(target=summary_refresher, args=(stats, field_counts, examples, skipped_path_only, stop_event))
    refresher.start()

    extracted_buffer, mirror_buffer = [], []
    extracted_idx, mirror_idx = 1, 1

    def flush_buffer(buffer, folder, prefix, idx):
        if not buffer:
            return idx
        out_file = Path(folder) / f"{prefix}_{idx:03d}.txt"
        with out_file.open("w", encoding="utf-8") as out:
            for line in buffer:
                out.write(line + "\n")
        return idx + 1

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(extract_lines, f, valid_fields) for f in files_to_process]
        for fut in tqdm(as_completed(futures), total=len(futures)):
            (local_stats, local_field_counts, local_examples,
             local_extracted, local_mirrored,
             local_skipped_path_only, file_path) = fut.result()

            # Merge stats
            for k, v in local_stats.items():
                stats[k] += v
            for k, v in local_field_counts.items():
                field_counts[k] += v
            for k, v in local_examples.items():
                if k not in examples:
                    examples[k] = v
            skipped_path_only.extend(local_skipped_path_only)

            # Handle extracted lines
            extracted_buffer.extend(local_extracted)
            while len(extracted_buffer) >= CHUNK_SIZE:
                chunk, extracted_buffer = extracted_buffer[:CHUNK_SIZE], extracted_buffer[CHUNK_SIZE:]
                extracted_idx = flush_buffer(chunk, FIELDS_FOLDER, "extracted", extracted_idx)

            # Handle mirrored lines
            mirror_buffer.extend(local_mirrored)
            while len(mirror_buffer) >= CHUNK_SIZE:
                chunk, mirror_buffer = mirror_buffer[:CHUNK_SIZE], mirror_buffer[CHUNK_SIZE:]
                mirror_idx = flush_buffer(chunk, MIRROR_FOLDER, "mirror", mirror_idx)

            # Mark file completed
            with open(RESUME_FILE, "a", encoding="utf-8") as rf:
                rf.write(file_path + "\n")

    # Flush remaining buffers
    if extracted_buffer:
        extracted_idx = flush_buffer(extracted_buffer, FIELDS_FOLDER, "extracted", extracted_idx)
    if mirror_buffer:
        mirror_idx = flush_buffer(mirror_buffer, MIRROR_FOLDER, "mirror", mirror_idx)

    stop_event.set()
    refresher.join()

    write_summary(stats, field_counts, examples, skipped_path_only, stage="Final")

    print(f"\n✅ Completed. Summary written to {SUMMARY_FILE}")
    print(f"Extracted files: {extracted_idx-1} | Mirror files: {mirror_idx-1}")
    print(f"Unique fields found: {len(field_counts)}")

if __name__ == "__main__":
    main()
