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
MIRROR_TRUNCATE = 1500                # max chars of log_line kept in mirror output
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
    p_json_quoted = re.compile(
        rf'["\']\s*(?P<field>[^"\']+?)\s*["\']\s*[:=]\s*["\']?(?P<mobile>{mobile_escaped})["\']?',
        re.IGNORECASE,
    )
    p_kv = re.compile(
        rf'\b(?P<field>[A-Za-z0-9_.\-]+)\s*[:=]\s*["\']?(?P<mobile>{mobile_escaped})["\']?',
        re.IGNORECASE,
    )
    p_xml_attr = re.compile(
        rf'<[^>]*\b(?P<field>[A-Za-z0-9_.\-]+)\s*=\s*["\'](?P<mobile>{mobile_escaped})["\'][^>]*>',
        re.IGNORECASE | re.DOTALL,
    )
    p_xml_tag = re.compile(
        rf'<\s*(?P<field>[A-Za-z0-9_.\-]+)[^>]*>[^<]*?(?P<mobile>{mobile_escaped})[^<]*?</\s*\1\s*>',
        re.IGNORECASE | re.DOTALL,
    )
    return (p_json_quoted, p_kv, p_xml_attr, p_xml_tag)


def identify_field_for_mobile(log_line: str, mobile: str):
    mob_esc = re.escape(mobile)
    for pat in make_patterns(mob_esc):
        m = pat.search(log_line)
        if m:
            field = m.group("field").strip()
            if field:
                return field
    return None


def process_file(path: Path, extracted_writer, mirror_writer):
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

                if ";" in line:
                    log_line, file_path = line.rsplit(";", 1)
                else:
                    log_line, file_path = line, ""

                matches = list(MOBILE_RE.finditer(line))
                if not matches:
                    stats["lines_no_regex"] += 1
                    continue

                split_at = len(log_line)
                log_matches  = [m for m in matches if m.start() < split_at]
                path_matches = [m for m in matches if m.start() >= split_at]

                stats["total_regex_matches"] += len(matches)

                if not log_matches and path_matches:
                    stats["dropped_path_only_matches"] += len(path_matches)
                    if len(path_only_samples) < 20:
                        path_only_samples.append(line)
                    continue

                for m in log_matches:
                    mobile_val = m.group(0)
                    field = identify_field_for_mobile(log_line, mobile_val)

                    if field:
                        row = f"{log_line} ; {file_path} ; {field} ; mobile_regex ; {mobile_val}\n"
                        extracted_writer.write(row)
                        stats["extracted_matches"] += 1
                        per_field_counts[field] += 1
                        if field not in per_field_example:
                            per_field_example[field] = row.strip()
                    else:
                        reason = "NO_FIELD_PATTERN"
                        short_log = (
                            log_line[:MIRROR_TRUNCATE] + "...TRUNCATED..."
                            if len(log_line) > MIRROR_TRUNCATE
                            else log_line
                        )
                        row = f"{short_log} ; {file_path} ; UNIDENTIFIED_FIELD ; mobile_regex ; {mobile_val} ; reason={reason}\n"
                        mirror_writer.write(row)
                        stats["mirrored_matches"] += 1

    except Exception as e:
        stats["errors"] += 1
        with open(ERRORS_FILE, "a", encoding="utf-8") as ef:
            ef.write(f"[{datetime.now().isoformat(timespec='seconds')}] {path}: {e}\n")
            ef.write(traceback.format_exc() + "\n")

    return stats, per_field_counts, per_field_example, path_only_samples


def open_chunk_writer(base_dir: Path, prefix: str, chunk_size: int):
    base_dir.mkdir(parents=True, exist_ok=True)
    counter, lines = 1, 0
    fh = (base_dir / f"{prefix}_{counter:03d}.txt").open("w", encoding="utf-8")

    def writer(line: str):
        nonlocal fh, lines, counter
        fh.write(line)
        lines += 1
        if lines >= chunk_size:
            fh.close()
            counter += 1
            lines = 0
            fh = (base_dir / f"{prefix}_{counter:03d}.txt").open("w", encoding="utf-8")

    def close():
        fh.close()

    return writer, close


def main():
    input_dir = Path(INPUT_FOLDER)
    if not input_dir.exists():
        print(f"Input folder not found: {input_dir.resolve()}")
        sys.exit(1)

    Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)
    OUT_FIELDS_DIR.mkdir(parents=True, exist_ok=True)
    OUT_MIRROR_DIR.mkdir(parents=True, exist_ok=True)

    files = [p for p in input_dir.rglob("*.txt")]
    if not files:
        print("No .txt files found in input.")
        sys.exit(0)
    random.shuffle(files)

    extracted_writer, close_extr = open_chunk_writer(OUT_FIELDS_DIR, "extracted", CHUNK_SIZE)
    mirror_writer, close_mirr = open_chunk_writer(OUT_MIRROR_DIR, "mirror", CHUNK_SIZE)

    G_stats = defaultdict(int)
    G_field_counts = defaultdict(int)
    G_field_example = {}
    G_path_only_samples = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_file, p, extracted_writer, mirror_writer) for p in files]
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            stats, field_counts, field_example, path_only = fut.result()
            for k, v in stats.items():
                G_stats[k] += v
            for k, v in field_counts.items():
                G_field_counts[k] += v
            for k, v in field_example.items():
                if k not in G_field_example:
                    G_field_example[k] = v
            for ln in path_only:
                if len(G_path_only_samples) < 50:
                    G_path_only_samples.append(ln)

    close_extr()
    close_mirr()

    total_matches = G_stats["total_regex_matches"]
    extracted_matches = G_stats["extracted_matches"]
    mirrored_matches  = G_stats["mirrored_matches"]
    dropped_path_only = G_stats["dropped_path_only_matches"]
    lines_no_regex    = G_stats["lines_no_regex"]
    errors_count      = G_stats["errors"]

    consistency_ok = (extracted_matches + mirrored_matches + dropped_path_only) == total_matches

    with open(SUMMARY_FILE, "w", encoding="utf-8") as sf:
        sf.write(f"Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        sf.write("=" * 60 + "\n\n")

        sf.write("COUNTS\n")
        sf.write(f"Total regex matches: {total_matches}\n")
        sf.write(f"  Extracted matches: {extracted_matches}\n")
        sf.write(f"  Mirrored matches:  {mirrored_matches}\n")
        sf.write(f"  Dropped (path-only): {dropped_path_only}\n")
        sf.write(f"Lines with no regex at all (dropped lines): {lines_no_regex}\n")
        sf.write(f"Errors: {errors_count}\n")
        sf.write(f"Consistency check: {consistency_ok}\n\n")

        sf.write("PER-FIELD COUNTS\n")
        for fld, cnt in sorted(G_field_counts.items(), key=lambda kv: kv[0].lower()):
            sf.write(f"{fld} = {cnt}\n")
            ex = G_field_example.get(fld)
            if ex:
                sf.write(f"  Example: {ex}\n")
        sf.write("\n")

        sf.write("SAMPLE PATH-ONLY LINES\n")
        if G_path_only_samples:
            for i, ln in enumerate(G_path_only_samples, 1):
                sf.write(f"{i}. {ln}\n")
        else:
            sf.write("(none)\n")

    print("\n✅ Done.")
    print(f"Extracted files in: {OUT_FIELDS_DIR.resolve()}")
    print(f"Mirror files in: {OUT_MIRROR_DIR.resolve()}")
    print(f"Summary: {SUMMARY_FILE.resolve()}")
    if errors_count:
        print(f"Errors logged: {ERRORS_FILE.resolve()}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Fatal error:", e)
        print(traceback.format_exc())
