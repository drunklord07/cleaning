import os
import sys
import time
import re
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from tqdm import tqdm

# ====== CONFIGURATION ====== #
INPUT_FOLDER = "input_logs"         # Folder with .txt inputs (non-recursive)
OUTPUT_FOLDER = "cleaned_output"    # Outputs as .txt (same basenames)
SUMMARY_FILE = "summary_report.txt" # Saved in current working dir
RESUME_LOG = "resume_files.log"     # Checkpoint log in current working dir
MAX_WORKERS = 6                     # Parallelism
ALLOWED_EXTS = (".txt",)            # Process only .txt files

CASE_SENSITIVE = True               # 'CustomerId' must match case exactly
EMIT_SINGLE_SPACE = True            # Normalize join spacing around kept items
# =========================== #

# Preamble patterns (allow 1+ spaces between tokens; tolerate leading spaces).
# 1) [T1] [T2] [T3] [T4] [T5] [T6] [T7] - [T8] <body>
PREAMBLE8_RE = re.compile(
    r'^\s*'
    r'(\[[^\]]*\])\s+'  # T1
    r'(\[[^\]]*\])\s+'  # T2
    r'(\[[^\]]*\])\s+'  # T3
    r'(\[[^\]]*\])\s+'  # T4
    r'(\[[^\]]*\])\s+'  # T5
    r'(\[[^\]]*\])\s+'  # T6
    r'(\[[^\]]*\])\s+'  # T7
    r'-\s+'             # hyphen
    r'(\[[^\]]*\])'     # T8
    r'(.*)$'            # body
)

# 2) [T1] [T2] [T3] [T4] [T5] [T6] [T7] - <body>
PREAMBLE7_RE = re.compile(
    r'^\s*'
    r'(\[[^\]]*\])\s+'  # T1
    r'(\[[^\]]*\])\s+'  # T2
    r'(\[[^\]]*\])\s+'  # T3
    r'(\[[^\]]*\])\s+'  # T4
    r'(\[[^\]]*\])\s+'  # T5
    r'(\[[^\]]*\])\s+'  # T6
    r'(\[[^\]]*\])\s+'  # T7
    r'-\s+'             # hyphen
    r'(.*)$'            # body
)

CUSTOMER_KEY = "CustomerId:"

def classify_customer_bracket(token: str, case_sensitive: bool) -> str:
    """
    token: "[CustomerId: ...]" or other "[...]"
    Returns: "none" | "empty" | "nonempty"
    """
    if not (token.startswith("[") and token.endswith("]")):
        return "none"

    inner = token[1:-1]
    key = CUSTOMER_KEY if case_sensitive else CUSTOMER_KEY.lower()
    target = inner if case_sensitive else inner.lower()
    if not target.startswith(key):
        return "none"

    suffix = inner[len(CUSTOMER_KEY):] if case_sensitive else inner[len(key):]
    return "empty" if suffix.strip() == "" else "nonempty"

def transform_preamble(tokens, body):
    """
    Apply CustomerId rules over the given preamble tokens (list of strings) and body.
    Returns (new_text, changed: bool, removed: bool, reduced: bool).
    - removed=True when whole preamble is dropped (empty CustomerId).
    - reduced=True when only [CustomerId: non-empty] is kept from preamble.
    """
    cust_index = -1
    cust_class = "none"
    for i, tok in enumerate(tokens):
        c = classify_customer_bracket(tok, CASE_SENSITIVE)
        if c != "none":
            cust_index, cust_class = i, c
            break

    if cust_index == -1:
        # no CustomerId in preamble -> unchanged
        return body if False else None, False, False, False

    if cust_class == "empty":
        # Drop entire preamble; keep only body
        out_body = body.lstrip() if EMIT_SINGLE_SPACE else body
        return out_body, True, True, False
    else:
        # Keep only that CustomerId token + body
        keep_token = tokens[cust_index]
        if EMIT_SINGLE_SPACE:
            out_body = body.lstrip()
            new_text = keep_token + ((" " + out_body) if out_body else "")
        else:
            new_text = keep_token + body
        return new_text, True, False, True

def transform_line(line: str) -> (str, dict):
    """
    Try 8-token preamble first; if not matched, try 7-token.
    Returns the (possibly) transformed line and a stats dict describing the change.
    """
    stats = {"matched8": False, "matched7": False, "removed": False, "reduced": False, "changed": False}

    has_nl = line.endswith("\n")
    base = line[:-1] if has_nl else line

    m8 = PREAMBLE8_RE.match(base)
    if m8:
        stats["matched8"] = True
        tokens = list(m8.groups()[:8])
        body = m8.group(9)
        new_text, changed, removed, reduced = transform_preamble(tokens, body)
        if changed:
            stats.update({"changed": True, "removed": removed, "reduced": reduced})
            return (new_text + ("\n" if has_nl else "")), stats
        else:
            # unchanged
            return line, stats

    m7 = PREAMBLE7_RE.match(base)
    if m7:
        stats["matched7"] = True
        tokens = list(m7.groups()[:7])
        body = m7.group(8)
        new_text, changed, removed, reduced = transform_preamble(tokens, body)
        if changed:
            stats.update({"changed": True, "removed": removed, "reduced": reduced})
            return (new_text + ("\n" if has_nl else "")), stats
        else:
            return line, stats

    # No preamble matched
    return line, stats

def process_file(file_path: str) -> dict:
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_modified": 0,
        "preambles_removed": 0,
        "preambles_reduced": 0,
        "matched8": 0,
        "matched7": 0,
        "error": None,
    }

    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    try:
        if os.path.exists(out_path):
            os.remove(out_path)
    except Exception:
        pass

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f_in, \
             open(out_path, "w", encoding="utf-8") as f_out:

            for raw in f_in:
                local["lines_processed"] += 1
                new_line, st = transform_line(raw)

                if st["matched8"]:
                    local["matched8"] += 1
                if st["matched7"]:
                    local["matched7"] += 1
                if st["changed"]:
                    local["lines_modified"] += 1
                    if st["removed"]:
                        local["preambles_removed"] += 1
                    elif st["reduced"]:
                        local["preambles_reduced"] += 1

                f_out.write(new_line)

    except Exception as e:
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        err = f"{local['file_name']}: {e.__class__.__name__}: {e}"
        err += "\n" + "".join(traceback.format_exception_only(type(e), e)).strip()
        local["error"] = err

    return local

def load_completed_set(log_path: str) -> set:
    completed = set()
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                name = line.strip()
                if name and not line.startswith("#"):
                    completed.add(name)
    return completed

def append_completed(log_path: str, file_name: str):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(file_name + "\n")

def write_summary(summary):
    summary["end_ts"] = time.time()
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Preamble CustomerId Normalizer (7-or-8) - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder: {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"Max Workers: {summary['max_workers']}\n")
        f.write(f"Case-sensitive: {CASE_SENSITIVE}\n")
        f.write(f"Emit single space: {EMIT_SINGLE_SPACE}\n\n")

        f.write("=== Files ===\n")
        f.write(f"Processed: {summary['files_scanned']}\n")
        f.write(f"Success:   {summary['files_success']}\n")
        f.write(f"Errors:    {summary['files_error']}\n\n")

        f.write("=== Lines ===\n")
        f.write(f"Total processed:       {summary['total_lines_processed']}\n")
        f.write(f"Total modified:        {summary['total_lines_modified']}\n")
        f.write(f"Preambles removed:     {summary['preambles_removed']}\n")
        f.write(f"Preambles reduced:     {summary['preambles_reduced']}\n")
        f.write(f"Matched 8-token lines: {summary['matched8']}\n")
        f.write(f"Matched 7-token lines: {summary['matched7']}\n\n")

        if summary["errors"]:
            f.write("=== Errors ===\n")
            for err in summary["errors"]:
                f.write(f"- {err}\n")

def main():
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: INPUT_FOLDER does not exist: {INPUT_FOLDER}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    all_files = sorted(
        os.path.join(INPUT_FOLDER, f)
        for f in os.listdir(INPUT_FOLDER)
        if os.path.isfile(os.path.join(INPUT_FOLDER, f))
        and os.path.splitext(f)[1].lower() in ALLOWED_EXTS
    )

    if not all_files:
        print(f"No {ALLOWED_EXTS} files found in INPUT_FOLDER.", file=sys.stderr)
        return

    completed = load_completed_set(RESUME_LOG)
    pending_files = [fp for fp in all_files if os.path.basename(fp) not in completed]

    if not pending_files:
        print("All files already processed per resume log. Nothing to do.")
        return

    summary = {
        "start_ts": time.time(),
        "end_ts": None,
        "max_workers": MAX_WORKERS,
        "files_scanned": 0,
        "files_success": 0,
        "files_error": 0,
        "total_lines_processed": 0,
        "total_lines_modified": 0,
        "preambles_removed": 0,
        "preambles_reduced": 0,
        "matched8": 0,
        "matched7": 0,
        "errors": []
    }

    overall_bar = tqdm(total=len(pending_files), desc="Overall", unit="file", leave=True)

    try:
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(process_file, fp): fp for fp in pending_files}

            for fut in as_completed(futures):
                file_path = futures[fut]
                base_name = os.path.basename(file_path)

                try:
                    res = fut.result()
                    summary["files_scanned"] += 1
                    summary["total_lines_processed"] += res["lines_processed"]
                    summary["total_lines_modified"] += res["lines_modified"]
                    summary["preambles_removed"] += res["preambles_removed"]
                    summary["preambles_reduced"] += res["preambles_reduced"]
                    summary["matched8"] += res["matched8"]
                    summary["matched7"] += res["matched7"]

                    if res["error"]:
                        summary["files_error"] += 1
                        summary["errors"].append(res["error"])
                    else:
                        summary["files_success"] += 1
                        append_completed(RESUME_LOG, base_name)

                except Exception as e:
                    summary["files_scanned"] += 1
                    summary["files_error"] += 1
                    summary["errors"].append(f"{base_name}: worker exception: {e}")

                overall_bar.update(1)

                # ETA
                elapsed = time.time() - summary["start_ts"]
                avg = elapsed / max(1, summary["files_scanned"])
                remaining = len(pending_files) - summary["files_scanned"]
                eta = max(0, int(remaining * avg))
                overall_bar.set_postfix_str(f"ETA: {str(timedelta(seconds=eta))}")

    finally:
        overall_bar.close()
        write_summary(summary)

if __name__ == "__main__":
    main()
