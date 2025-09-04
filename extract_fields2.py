#!/usr/bin/env python3
import re
import sys
import traceback
import threading
import time
import random
import multiprocessing as mp
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime
from tqdm import tqdm

# ============= CONFIG ============= #
INPUT_FOLDER    = "input_logs"          # put your input folder name here (in current dir)
OUTPUT_FOLDER   = "output_mobile"       # outputs will be created under this
MAX_WORKERS     = 6
CHUNK_SIZE      = 10_000                # files of 10k rows each
MIRROR_TRUNCATE = 1500                  # max chars of log_line kept in mirror output
SUMMARY_REFRESH_INTERVAL = 30           # seconds (live summary rewrite)
QUEUE_MAXSIZE   = 10000                 # queue capacity for backpressure
# ================================== #

OUT_FIELDS_DIR  = Path(OUTPUT_FOLDER) / "fields_identified"
OUT_MIRROR_DIR  = Path(OUTPUT_FOLDER) / "mirror"
SUMMARY_FILE    = Path(OUTPUT_FOLDER) / "summary_mobile_fields.txt"
ERRORS_FILE     = Path(OUTPUT_FOLDER) / "errors.log"

# Strict mobile regex (India 10 or prefixed 91 + 10); prevents A–Z / 0–9 adjacency
MOBILE_RE = re.compile(r'(?<![A-Za-z0-9])(?:91)?[6-9]\d{9}(?![A-Za-z0-9])')


# ---------- field detection patterns ----------
def make_patterns(mobile_escaped: str):
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


def process_file(path: Path, extracted_q, mirror_q):
    stats = defaultdict(int)
    per_field_counts = defaultdict(int)
    per_field_example = {}
    path_only_samples = []
    file_failed = False

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

                line_had_extracted = False
                line_had_mirrored  = False

                for m in log_matches:
                    mobile_val = m.group(0)
                    field = identify_field_for_mobile(log_line, mobile_val)

                    if field:
                        row = f"{log_line} ; {file_path} ; {field} ; mobile_regex ; {mobile_val}\n"
                        extracted_q.put(row)
                        stats["extracted_matches"] += 1
                        per_field_counts[field] += 1
                        if field not in per_field_example:
                            per_field_example[field] = row.strip()
                        line_had_extracted = True
                    else:
                        reason = "NO_FIELD_PATTERN"
                        short_log = (
                            log_line[:MIRROR_TRUNCATE] + "...TRUNCATED..."
                            if len(log_line) > MIRROR_TRUNCATE
                            else log_line
                        )
                        row = f"{short_log} ; {file_path} ; UNIDENTIFIED_FIELD ; mobile_regex ; {mobile_val} ; reason={reason}\n"
                        mirror_q.put(row)
                        stats["mirrored_matches"] += 1
                        line_had_mirrored = True

                if line_had_extracted and line_had_mirrored:
                    stats["partial_valid_lines"] += 1

    except Exception as e:
        stats["errors"] += 1
        file_failed = True
        with open(ERRORS_FILE, "a", encoding="utf-8") as ef:
            ef.write(f"[{datetime.now().isoformat(timespec='seconds')}] {path}: {e}\n")
            ef.write(traceback.format_exc() + "\n")

    return stats, per_field_counts, per_field_example, path_only_samples, file_failed


def writer_loop(queue: mp.Queue, base_dir: Path, prefix: str, chunk_size: int, stop_event: threading.Event):
    base_dir.mkdir(parents=True, exist_ok=True)
    counter, lines = 1, 0
    fh = (base_dir / f"{prefix}_{counter:03d}.txt").open("w", encoding="utf-8")

    try:
        while True:
            try:
                item = queue.get(timeout=0.5)
            except Exception:
                if stop_event.is_set() and queue.empty():
                    break
                continue

            if item is None:
                if stop_event.is_set() and queue.empty():
                    break
                else:
                    continue

            fh.write(item)
            lines += 1
            if lines >= chunk_size:
                fh.close()
                counter += 1
                lines = 0
                fh = (base_dir / f"{prefix}_{counter:03d}.txt").open("w", encoding="utf-8")
    finally:
        fh.close()


def write_summary(input_dir: Path,
                  G_stats: dict,
                  G_field_counts: dict,
                  G_field_example: dict,
                  G_path_only_samples: list,
                  failed_files: list,
                  stage: str = "Final"):
    total_matches     = G_stats.get("total_regex_matches", 0)
    extracted_matches = G_stats.get("extracted_matches", 0)
    mirrored_matches  = G_stats.get("mirrored_matches", 0)
    dropped_path_only = G_stats.get("dropped_path_only_matches", 0)
    lines_no_regex    = G_stats.get("lines_no_regex", 0)
    errors_count      = G_stats.get("errors", 0)
    partial_lines     = G_stats.get("partial_valid_lines", 0)
    files_total       = G_stats.get("files_total", 0)
    files_processed   = G_stats.get("files_processed", 0)
    files_failed      = G_stats.get("files_failed", 0)

    consistency_ok = (extracted_matches + mirrored_matches + dropped_path_only) == total_matches

    with open(SUMMARY_FILE, "w", encoding="utf-8") as sf:
        sf.write(f"Summary ({stage}) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        sf.write("=" * 60 + "\n\n")

        sf.write("INPUT / OUTPUT\n")
        sf.write(f"Input folder: {input_dir.resolve()}\n")
        sf.write(f"Output folder: {Path(OUTPUT_FOLDER).resolve()}\n")
        sf.write(f"Fields Identified (chunk size {CHUNK_SIZE}): {OUT_FIELDS_DIR.resolve()}\n")
        sf.write(f"Mirror (chunk size {CHUNK_SIZE}): {OUT_MIRROR_DIR.resolve()}\n")
        sf.write(f"Mirror truncation: first {MIRROR_TRUNCATE} chars of log_line\n\n")

        sf.write("FILE COUNTS\n")
        sf.write(f"Total files discovered: {files_total}\n")
        sf.write(f"Files processed successfully: {files_processed}\n")
        sf.write(f"Files failed: {files_failed}\n\n")

        if failed_files:
            sf.write("FAILED FILES\n")
            for f in failed_files:
                sf.write(f"- {f}\n")
            sf.write("\n")

        sf.write("COUNTS\n")
        sf.write(f"Total regex matches: {total_matches}\n")
        sf.write(f"  Extracted matches: {extracted_matches}\n")
        sf.write(f"  Mirrored matches:  {mirrored_matches}\n")
        sf.write(f"  Dropped (path-only matches): {dropped_path_only}\n")
        sf.write(f"Lines with no regex at all (dropped lines): {lines_no_regex}\n")
        sf.write(f"Partial-valid lines: {partial_lines}\n")
        sf.write(f"Errors: {errors_count}\n")
        sf.write(f"Consistency check: {consistency_ok}\n\n")

        sf.write("PER-FIELD COUNTS (extracted)\n")
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
        sf.write("\n")

        sf.write("NOTES\n")
        sf.write("- Extraction strictly on allowed field→value patterns.\n")
        sf.write("- One output row per regex match. Path-only matches dropped.\n")
        sf.write("- Mirror rows truncate log_line.\n")


def summary_refresher_loop(input_dir: Path,
                           G_stats: dict,
                           G_field_counts: dict,
                           G_field_example: dict,
                           G_path_only_samples: list,
                           failed_files: list,
                           stop_event: threading.Event):
    while not stop_event.is_set():
        try:
            write_summary(input_dir, G_stats, G_field_counts, G_field_example, G_path_only_samples, failed_files, stage="Live")
        except Exception as e:
            with open(ERRORS_FILE, "a", encoding="utf-8") as ef:
                ef.write(f"[{datetime.now().isoformat(timespec='seconds')}] summary_refresher: {e}\n")
                ef.write(traceback.format_exc() + "\n")
        stop_event.wait(SUMMARY_REFRESH_INTERVAL)


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
        write_summary(input_dir, defaultdict(int), {}, {}, [], [], stage="Final")
        sys.exit(0)

    random.shuffle(files)

    manager = mp.Manager()
    extracted_q = manager.Queue(maxsize=QUEUE_MAXSIZE)
    mirror_q    = manager.Queue(maxsize=QUEUE_MAXSIZE)

    extracted_stop = threading.Event()
    mirror_stop    = threading.Event()

    t_extr = threading.Thread(target=writer_loop, args=(extracted_q, OUT_FIELDS_DIR, "extracted", CHUNK_SIZE, extracted_stop), daemon=True)
    t_mirr = threading.Thread(target=writer_loop, args=(mirror_q, OUT_MIRROR_DIR, "mirror", CHUNK_SIZE, mirror_stop), daemon=True)
    t_extr.start()
    t_mirr.start()

    G_stats = defaultdict(int)
    G_field_counts = defaultdict(int)
    G_field_example = {}
    G_path_only_samples = []
    failed_files = []

    G_stats["files_total"] = len(files)

    stop_event = threading.Event()
    refresher = threading.Thread(target=summary_refresher_loop, args=(input_dir, G_stats, G_field_counts, G_field_example, G_path_only_samples, failed_files, stop_event), daemon=True)
    refresher.start()

    with ProcessPoolExecutor(max_workers=MAX_WORKERS, mp_context=mp.get_context("spawn")) as ex:
        futures = {ex.submit(process_file, p, extracted_q, mirror_q): p for p in files}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            path = futures[fut]
            try:
                stats, field_counts, field_example, path_only, file_failed = fut.result()
                if file_failed:
                    G_stats["files_failed"] += 1
                    failed_files.append(str(path))
                else:
                    G_stats["files_processed"] += 1

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
            except Exception as e:
                G_stats["files_failed"] += 1
                failed_files.append(str(path))
                with open(ERRORS_FILE, "a", encoding="utf-8") as ef:
                    ef.write(f"[{datetime.now().isoformat(timespec='seconds')}] {path}: {e}\n")
                    ef.write(traceback.format_exc() + "\n")

    extracted_stop.set()
    mirror_stop.set()
    extracted_q.put(None)
    mirror_q.put(None)
    t_extr.join()
    t_mirr.join()

    stop_event.set()
    refresher.join(timeout=2)
    write_summary(input_dir, G_stats, G_field_counts, G_field_example, G_path_only_samples, failed_files, stage="Final")

    print("\n✅ Done.")
    print(f"Total files: {G_stats['files_total']}  |  Processed: {G_stats['files_processed']}  |  Failed: {G_stats['files_failed']}")
    print(f"Extracted files in: {OUT_FIELDS_DIR.resolve()}")
    print(f"Mirror files in:    {OUT_MIRROR_DIR.resolve()}")
    print(f"Summary:            {SUMMARY_FILE.resolve()}")
    if G_stats['files_failed'] > 0:
        print(f"Errors logged:      {ERRORS_FILE.resolve()}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Fatal error:", e)
        print(traceback.format_exc())
