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

# Pattern for a space-separated preamble:
# [T1] [T2] [T3] [T4] [T5] [T6] [T7] - [T8] <body>
# We'll allow 1+ spaces in input but output clean single spaces if EMIT_SINGLE_SPACE.
PREAMBLE_RE = re.compile(
    r'^\s*'                                   # optional leading spaces
    r'(\[[^\]]*\])\s+'                        # T1
    r'(\[[^\]]*\])\s+'                        # T2
    r'(\[[^\]]*\])\s+'                        # T3
    r'(\[[^\]]*\])\s+'                        # T4
    r'(\[[^\]]*\])\s+'                        # T5
    r'(\[[^\]]*\])\s+'                        # T6
    r'(\[[^\]]*\])\s+'                        # T7
    r'-\s+'                                   # hyphen
    r'(\[[^\]]*\])'                           # T8
    r'(.*)$'                                  # body (can be empty), includes any trailing spaces
)

CUSTOMER_KEY = "CustomerId:"

def classify_customer_bracket(token: str, case_sensitive: bool) -> str:
    """
    token: e.g. "[CustomerId: 123]" or "[Foo]".
    Returns:
      - "none"      : not a CustomerId token
      - "empty"     : [CustomerId:] or [CustomerId: ] (only whitespace after colon)
      - "nonempty"  : [CustomerId: <non-empty>]
    """
    if not (token.startswith("[") and token.endswith("]")):
        return "none"

    inner = token[1:-1]
    key = CUSTOMER_KEY if case_sensitive else CUSTOMER_KEY.lower()
    target = inner if case_sensitive else inner.lower()

    if not target.startswith(key):
        return "none"

    # Suffix after "CustomerId:"
    suffix = inner[len(CUSTOMER_KEY):] if case_sensitive else inner[len(key):]
    # If suffix is only whitespace (including empty), treat as empty
    return "empty" if suffix.strip() == "" else "nonempty"

def transform_line(line: str) -> str:
    """Apply the preamble rules to a single line; preserve newline."""
    has_nl = line.endswith("\n")
    base = line[:-1] if has_nl else line

    m = PREAMBLE_RE.match(base)
    if not m:
        return line  # leave unchanged

    tokens = list(m.groups()[:8])  # T1..T8
    body = m.group(9)              # body (may be empty; may start with spaces)

    # Find first CustomerId token and classify
    cust_index = -1
    cust_class = "none"
    for i, tok in enumerate(tokens):
        c = classify_customer_bracket(tok, CASE_SENSITIVE)
        if c != "none":
            cust_index = i
            cust_class = c
            break

    # No CustomerId in preamble => unchanged
    if cust_index == -1:
        return line

    # Build output according to rules
    if cust_class == "empty":
        # Remove entire preamble; keep only the body
        out_body = body.lstrip() if EMIT_SINGLE_SPACE else body
        out = out_body
    else:
        # nonempty: keep only the CustomerId token + body
        keep_token = tokens[cust_index]
        if EMIT_SINGLE_SPACE:
            out_body = body.lstrip()
            out = keep_token + ((" " + out_body) if out_body else "")
        else:
            out = keep_token + body  # preserve original spacing

    return (out + ("\n" if has_nl else ""))

def process_file(file_path: str) -> dict:
    """
    Processes a file line by line applying the transform_line() rules.
    """
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_modified": 0,
        "preambles_removed": 0,
        "preambles_reduced": 0,
        "error": None,
    }

    out_path = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path))

    # Clean any stale partial from a previous failed attempt
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
                new = transform_line(raw)
                if new != raw:
                    local["lines_modified"] += 1
                    # classify what happened for counts
                    # Use same logic again but cheaper: we can detect by regex
                    m = PREAMBLE_RE.match(raw.rstrip("\n"))
                    if m:
                        tokens = list(m.groups()[:8])
                        # did we remove entire preamble (empty CustomerId) or reduce (non-empty)?
                        changed = False
                        for tok in tokens:
                            cls = classify_customer_bracket(tok, CASE_SENSITIVE)
                            if cls == "empty":
                                local["preambles_removed"] += 1
                                changed = True
                                break
                            elif cls == "nonempty":
                                local["preambles_reduced"] += 1
                                changed = True
                                break
                        if not changed:
                            # shouldn't happen, but just in case
                            local["preambles_reduced"] += 1
                f_out.write(new)

    except Exception as e:
        # Remove partial output so the file is retried next run
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
        f.write(f"Preamble CustomerId Normalizer - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
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
        f.write(f"Total processed:      {summary['total_lines_processed']}\n")
        f.write(f"Total modified:       {summary['total_lines_modified']}\n")
        f.write(f"Preambles removed:    {summary['preambles_removed']}\n")
        f.write(f"Preambles reduced:    {summary['preambles_reduced']}\n\n")

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
