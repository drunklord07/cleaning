#!/usr/bin/env python3
import re
import sys
import traceback
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime
from tqdm import tqdm
import random

# ============= CONFIG ============= #
INPUT_FOLDER  = "input_logs"          # put your input folder name here (in current dir)
OUTPUT_FOLDER = "output_mobile"       # outputs will be created under this
MAX_WORKERS   = 6
CHUNK_SIZE    = 10_000                # files of 10k rows each
# ================================== #

OUT_FIELDS_DIR  = Path(OUTPUT_FOLDER) / "fields_identified"
OUT_MIRROR_DIR  = Path(OUTPUT_FOLDER) / "mirror"
SUMMARY_FILE    = Path(OUTPUT_FOLDER) / "summary_mobile_fields.txt"
ERRORS_FILE     = Path(OUTPUT_FOLDER) / "errors.log"

# Strict mobile regex (India 10 or prefixed 91 + 10); prevents A–Z / 0–9 adjacency
MOBILE_RE = re.compile(r'(?<![A-Za-z0-9])(?:91)?[6-9]\d{9}(?![A-Za-z0-9])')

# ---------- field detection patterns (compiled lazily per mobile) ----------
def make_patterns(mobile_escaped: str):
    """
    Return patterns (in preferred order) that capture a field name tied to the exact mobile value.
    Each pattern must have groups: (?P<field>...), (?P<mobile>...).
    """
    # JSON-like with quoted field (double or single), delimiter : or =
    p_json_quoted = re.compile(
        rf'["\']\s*(?P<field>[^"\']+?)\s*["\']\s*[:=]\s*["\']?(?P<mobile>{mobile_escaped})["\']?',
        re.IGNORECASE,
    )
    # Unquoted key=value or key:value  (allow . _ - in field)
    p_kv = re.compile(
        rf'\b(?P<field>[A-Za-z0-9_.\-]+)\s*[:=]\s*["\']?(?P<mobile>{mobile_escaped})["\']?',
        re.IGNORECASE,
    )
    # XML attribute: <... field="mobile" ...>
    p_xml_attr = re.compile(
        rf'<[^>]*\b(?P<field>[A-Za-z0-9_.\-]+)\s*=\s*["\'](?P<mobile>{mobile_escaped})["\'][^>]*>',
        re.IGNORECASE | re.DOTALL,
    )
    # XML tag content: <field>...mobile...</field>
    p_xml_tag = re.compile(
        rf'<\s*(?P<field>[A-Za-z0-9_.\-]+)[^>]*>[^<]*?(?P<mobile>{mobile_escaped})[^<]*?</\s*\1\s*>',
        re.IGNORECASE | re.DOTALL,
    )

    # Priority: JSON quoted → key/value → XML attribute → XML tag content
    return (p_json_quoted, p_kv, p_xml_attr, p_xml_tag)


def identify_field_for_mobile(log_line: str, mobile: str):
    """Return field name if any of the allowed patterns ties field → mobile; else None."""
    mob_esc = re.escape(mobile)
    for pat in make_patterns(mob_esc):
        m = pat.search(log_line)
        if m:
            field = m.group("field").strip()
            if field:
                return field
    return None


def process_file(path: Path):
    """
    Worker: returns a tuple with:
    (
      extracted_rows,      # list[str] rows: log ; path ; field ; mobile_regex ; value
      mirrored_rows,       # list[str] rows: log ; path ; UNIDENTIFIED_FIELD ; mobile_regex ; value ; reason=...
      stats,               # dict counters
      per_field_counts,    # dict field -> count
      per_field_example,   # dict field -> example row (first seen)
      path_only_samples,   # list[str] some samples of path-only lines
    )
    """
    local_extracted = []
    local_mirror = []
    stats = defaultdict(int)
    per_field_counts = defaultdict(int)
    per_field_example = {}
    path_only_samples = []

    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = raw.rstrip("\n")
                if not line:
                    continue

                # Split into log_line ; path (last ';' is the delimiter)
                if ";" in line:
                    log_line, file_path = line.rsplit(";", 1)
                else:
                    log_line, file_path = line, ""  # tolerate missing path

                # Global mobile matches (log+path)
                all_matches = list(MOBILE_RE.finditer(line))
                if not all_matches:
                    stats["lines_no_regex"] += 1
                    # dropped entirely (no match to count toward totals)
                    continue

                # Partition matches by location
                split_at = len(log_line)
                log_matches  = [m for m in all_matches if m.start() < split_at]
                path_matches = [m for m in all_matches if m.start() >= split_at]

                # Count toward total regex matches
                stats["total_regex_matches"] += len(all_matches)

                # If only path matches → drop (count as dropped_path_only per match)
                if log_matches == [] and path_matches:
                    stats["dropped_path_only_matches"] += len(path_matches)
                    # keep a few samples for auditing
                    if len(path_only_samples) < 20:
                        path_only_samples.append(line)
                    continue  # nothing to write

                # For each log-line match: one output row per match
                for m in log_matches:
                    mobile_val = m.group(0)
                    field = identify_field_for_mobile(log_line, mobile_val)

                    if field:
                        # extracted row
                        row = f"{log_line} ; {file_path} ; {field} ; mobile_regex ; {mobile_val}"
                        local_extracted.append(row)
                        stats["extracted_matches"] += 1
                        per_field_counts[field] += 1
                        if field not in per_field_example:
                            per_field_example[field] = row
                    else:
                        # mirror row (per match) with reason
                        reason = "NO_FIELD_PATTERN"  # strict rule: field must be explicit
                        row = f"{log_line} ; {file_path} ; UNIDENTIFIED_FIELD ; mobile_regex ; {mobile_val} ; reason={reason}"
                        local_mirror.append(row)
                        stats["mirrored_matches"] += 1

    except Exception as e:
        # File-level error logging and continue
        stats["errors"] += 1
        with open(ERRORS_FILE, "a", encoding="utf-8") as ef:
            ef.write(f"[{datetime.now().isoformat(timespec='seconds')}] {path}: {e}\n")
            ef.write(traceback.format_exc() + "\n")

    return (local_extracted, local_mirror, stats, per_field_counts, per_field_example, path_only_samples)


def flush_chunk(buffer, folder: Path, prefix: str, idx: int):
    if not buffer:
        return idx
    out_path = folder / f"{prefix}_{idx:03d}.txt"
    with out_path.open("w", encoding="utf-8") as out:
        out.write("\n".join(buffer))
        out.write("\n")
    return idx + 1


def main():
    input_dir = Path(INPUT_FOLDER)
    if not input_dir.exists():
        print(f"Input folder not found: {input_dir.resolve()}")
        sys.exit(1)

    # Prepare output dirs
    Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)
    OUT_FIELDS_DIR.mkdir(parents=True, exist_ok=True)
    OUT_MIRROR_DIR.mkdir(parents=True, exist_ok=True)

    # Enumerate *.txt recursively
    files = [p for p in input_dir.rglob("*.txt")]
    if not files:
        print("No .txt files found in input.")
        sys.exit(0)

    # Shuffle to distribute big files across the run (smoother progress)
    random.shuffle(files)

    # Global aggregations
    G_stats = defaultdict(int)
    G_field_counts = defaultdict(int)
    G_field_example = {}
    G_path_only_samples = []

    extracted_buf, mirror_buf = [], []
    extracted_idx, mirror_idx = 1, 1

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_file, p) for p in files]
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            (loc_extr, loc_mirr, loc_stats, loc_field_counts, loc_field_example, loc_path_only) = fut.result()

            # Merge stats
            for k, v in loc_stats.items():
                G_stats[k] += v

            for k, v in loc_field_counts.items():
                G_field_counts[k] += v

            for k, v in loc_field_example.items():
                if k not in G_field_example:
                    G_field_example[k] = v

            # path-only samples
            for ln in loc_path_only:
                if len(G_path_only_samples) < 50:
                    G_path_only_samples.append(ln)

            # Buffers and chunked writing
            extracted_buf.extend(loc_extr)
            while len(extracted_buf) >= CHUNK_SIZE:
                chunk, extracted_buf = extracted_buf[:CHUNK_SIZE], extracted_buf[CHUNK_SIZE:]
                extracted_idx = flush_chunk(chunk, OUT_FIELDS_DIR, "extracted", extracted_idx)

            mirror_buf.extend(loc_mirr)
            while len(mirror_buf) >= CHUNK_SIZE:
                chunk, mirror_buf = mirror_buf[:CHUNK_SIZE], mirror_buf[CHUNK_SIZE:]
                mirror_idx = flush_chunk(chunk, OUT_MIRROR_DIR, "mirror", mirror_idx)

    # Flush remaining
    if extracted_buf:
        extracted_idx = flush_chunk(extracted_buf, OUT_FIELDS_DIR, "extracted", extracted_idx)
    if mirror_buf:
        mirror_idx = flush_chunk(mirror_buf, OUT_MIRROR_DIR, "mirror", mirror_idx)

    # Compute consistency numbers
    total_matches = G_stats["total_regex_matches"]
    extracted_matches = G_stats["extracted_matches"]
    mirrored_matches  = G_stats["mirrored_matches"]
    dropped_path_only = G_stats["dropped_path_only_matches"]
    lines_no_regex    = G_stats["lines_no_regex"]
    errors_count      = G_stats["errors"]

    # Sanity: extracted + mirror + dropped_path_only == total_regex_matches
    consistency_ok = (extracted_matches + mirrored_matches + dropped_path_only) == total_matches

    # Write summary
    with open(SUMMARY_FILE, "w", encoding="utf-8") as sf:
        sf.write(f"Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        sf.write("=" * 60 + "\n\n")

        sf.write("INPUT / OUTPUT\n")
        sf.write(f"Input folder: {input_dir.resolve()}\n")
        sf.write(f"Output folder: {Path(OUTPUT_FOLDER).resolve()}\n")
        sf.write(f"Fields Identified: {OUT_FIELDS_DIR.resolve()}\n")
        sf.write(f"Mirror: {OUT_MIRROR_DIR.resolve()}\n")
        sf.write("\n")

        sf.write("COUNTS\n")
        sf.write(f"Total regex matches: {total_matches}\n")
        sf.write(f"  Extracted matches: {extracted_matches}\n")
        sf.write(f"  Mirrored matches:  {mirrored_matches}\n")
        sf.write(f"  Dropped (path-only): {dropped_path_only}\n")
        sf.write(f"Lines with no regex at all (dropped lines): {lines_no_regex}\n")
        sf.write(f"Errors: {errors_count}\n")
        sf.write(f"Consistency check (extracted + mirrored + dropped == total): {consistency_ok}\n")
        sf.write("\n")

        sf.write("PER-FIELD COUNTS (extracted)\n")
        for fld, cnt in sorted(G_field_counts.items(), key=lambda kv: kv[0].lower()):
            sf.write(f"{fld} = {cnt}\n")
            ex = G_field_example.get(fld)
            if ex:
                sf.write(f"  Example: {ex}\n")
        sf.write("\n")

        sf.write("SAMPLE PATH-ONLY LINES (skipped)\n")
        if G_path_only_samples:
            for i, ln in enumerate(G_path_only_samples, 1):
                sf.write(f"{i}. {ln}\n")
        else:
            sf.write("(none)\n")
        sf.write("\n")

        sf.write("NOTES\n")
        sf.write("- Extraction was performed strictly on allowed field→value patterns only.\n")
        sf.write("- Each mobile match becomes exactly one output row (extracted or mirrored).\n")
        sf.write("- Matches found only in the path were dropped and counted as dropped_path_only.\n")
        sf.write("- Lines with zero matches were dropped entirely and do not count toward total regex matches.\n")

    print("\n✅ Done.")
    print(f"Extracted files: {extracted_idx-1}  |  Mirror files: {mirror_idx-1}")
    print(f"Summary: {SUMMARY_FILE.resolve()}")
    if errors_count:
        print(f"Errors logged: {ERRORS_FILE.resolve()}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Fatal error:", e)
        print(traceback.format_exc())
