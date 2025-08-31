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
OUTPUT_FOLDER = "cleaned_output"    # Cleaned outputs as .txt (same basenames)
SUMMARY_FILE = "summary_report.txt" # Saved in current working dir
RESUME_LOG = "resume_files.log"     # Checkpoint log in current working dir
MAX_WORKERS = 6                     # Use 6–8 for optimal performance
ALLOWED_EXTS = (".txt",)            # Process only .txt files

# Multiple markers: remove a line if it contains ANY of these
# (If USE_REGEX=False, these are treated as LITERAL substrings)
MARKERS = [
    "<###",               # example; add more as needed
    # r"\bERROR\b",       # when USE_REGEX=True
]

USE_REGEX = False                   # False = literal substring; True = regex patterns
CASE_INSENSITIVE = False            # Applies to both literal and regex modes

# CustomerId handling
CUSTOMER_ID_CASE_SENSITIVE = True   # 'CustomerId' exact case match
EMIT_SINGLE_SPACE = True            # one space between kept bracket and kept tail
# =========================== #

# Pre-compiled helpers
if USE_REGEX:
    _marker_flags = re.IGNORECASE if CASE_INSENSITIVE else 0
    MARKER_OBJS = [re.compile(p, _marker_flags) for p in MARKERS]
else:
    MARKER_OBJS = [m.lower() if CASE_INSENSITIVE else m for m in MARKERS]

# [CustomerId: ...] finder — preserves exact inner text
_CUST_FLAGS = 0 if CUSTOMER_ID_CASE_SENSITIVE else re.IGNORECASE
CUST_RE = re.compile(r"\[CustomerId:(.*?)\]", _CUST_FLAGS)

def _line_hits_any_marker(line: str) -> (bool, list):
    """Return (hit, hit_indexes). hit_indexes are indices into MARKERS for which a hit occurred."""
    hits = []
    if USE_REGEX:
        for i, rx in enumerate(MARKER_OBJS):
            if rx.search(line):
                hits.append(i)
    else:
        hay = line.lower() if CASE_INSENSITIVE else line
        for i, lit in enumerate(MARKER_OBJS):
            if lit and (lit in hay):
                hits.append(i)
    return (len(hits) > 0, hits)

def _find_semicolon_before_path(s: str) -> int:
    """
    Find the index of the first ';' that is followed (after optional spaces/tabs)
    by something that looks like a path:
      - Windows drive:    C:\...  or  C:/...
      - UNC:              \\server\share\...
      - Unix absolute:    /...
      - Relative:         ./..., ../...
      - Home:             ~/..., ~\...
    Return -1 if none.
    """
    i = -1
    start = 0
    L = len(s)
    while True:
        i = s.find(";", start)
        if i == -1:
            return -1
        j = i + 1
        # skip spaces/tabs
        while j < L and s[j] in " \t":
            j += 1
        # Check path starts
        if j + 2 < L and s[j].isalpha() and s[j+1] == ":" and (s[j+2] == "\\" or s[j+2] == "/"):
            return i  # Windows drive
        if j + 1 < L and s[j] == "\\" and s[j+1] == "\\":  # UNC
            return i
        if j < L and s[j] == "/":                          # Unix abs
            return i
        if j + 1 < L and s[j] == "~" and (s[j+1] in "/\\"):  # ~/
            return i
        if j + 1 < L and s[j] == "." and (s.startswith("./", j) or s.startswith(".\\", j)):
            return i
        if j + 2 < L and s[j] == "." and s[j+1] == "." and (s[j+2] in "/\\"):  # ../ or ..\
            return i
        start = i + 1

def _salvage_with_customer(line_wo_nl: str, keep_bracket: str) -> str:
    """
    Build salvaged line: [CustomerId: ...] + space + substring from the semicolon-before-path.
    If no such semicolon found, return just the bracket.
    """
    semi_idx = _find_semicolon_before_path(line_wo_nl)
    if semi_idx == -1:
        return keep_bracket
    tail = line_wo_nl[semi_idx:]  # keep from ';' through end
    if EMIT_SINGLE_SPACE:
        return keep_bracket + (" " + tail.lstrip() if tail else "")
    else:
        return keep_bracket + tail

def process_file(file_path: str) -> dict:
    """
    Removes any line that matches ANY marker, with CustomerId salvage rules:
      - [CustomerId:] or [CustomerId: ]  -> drop line
      - [CustomerId: non-empty]          -> keep only that bracket + ' ' + ';<file-path>...' tail
      - no CustomerId bracket            -> drop line
    Lines with no marker are kept unchanged.
    """
    local = {
        "file_name": os.path.basename(file_path),
        "lines_processed": 0,
        "lines_removed": 0,
        "lines_salvaged": 0,
        "per_marker_hits": [0] * len(MARKERS),
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
                has_nl = raw.endswith("\n")
                base = raw[:-1] if has_nl else raw

                hit, idxs = _line_hits_any_marker(base)
                if not hit:
                    # no marker -> keep as-is
                    f_out.write(raw)
                    continue

                # Count marker hits (even if we end up salvaging)
                for i in idxs:
                    local["per_marker_hits"][i] += 1

                # Look for CustomerId brackets
                matches = list(CUST_RE.finditer(base))
                if not matches:
                    # No CustomerId -> drop
                    local["lines_removed"] += 1
                    continue

                # Prefer first NON-empty CustomerId; otherwise treat as empty
                nonempty = None
                empty_present = False
                for m in matches:
                    inner = m.group(1)  # content after colon up to closing ']'
                    if inner.strip() == "":
                        empty_present = True
                    elif nonempty is None:
                        nonempty = m

                if nonempty is not None:
                    keep_token = nonempty.group(0)  # preserve exact bracket text
                    salvaged = _salvage_with_customer(base, keep_token)
                    f_out.write(salvaged + ("\n" if has_nl else ""))
                    local["lines_salvaged"] += 1
                else:
                    # only empty CustomerId present -> drop
                    local["lines_removed"] += 1

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
    """Loads a set of completed files from the resume log."""
    completed = set()
    if os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                name = line.strip()
                if name and not line.startswith("#"):
                    completed.add(name)
    return completed

def append_completed(log_path: str, file_name: str):
    """Appends a completed file name to the resume log."""
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(file_name + "\n")

def write_summary(summary_data):
    """Writes the summary report to a file."""
    summary_data["end_ts"] = time.time()
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        f.write(f"Line Filter + CustomerId Salvage - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input Folder: {os.path.abspath(INPUT_FOLDER)}\n")
        f.write(f"Output Folder: {os.path.abspath(OUTPUT_FOLDER)}\n")
        f.write(f"Max Workers: {summary_data['max_workers']}\n")
        f.write(f"Use regex: {USE_REGEX} | Case-insensitive: {CASE_INSENSITIVE}\n")
        f.write(f"CustomerId case-sensitive: {CUSTOMER_ID_CASE_SENSITIVE}\n")
        f.write("Markers:\n")
        for i, m in enumerate(MARKERS):
            f.write(f"  {i+1}. {m!r}\n")
        f.write("\n")

        f.write("=== Files Processed ===\n")
        f.write(f"Processed: {summary_data['files_scanned']}\n")
        f.write(f"Success:   {summary_data['files_success']}\n")
        f.write(f"Errors:    {summary_data['files_error']}\n\n")

        f.write("=== Lines ===\n")
        f.write(f"Total lines processed: {summary_data['total_lines_processed']}\n")
        f.write(f"Total lines removed:   {summary_data['total_lines_removed']}\n")
        f.write(f"Total lines salvaged:  {summary_data['total_lines_salvaged']}\n\n")

        f.write("Per-marker hits (line may increment multiple markers):\n")
        for i, m in enumerate(MARKERS):
            f.write(f"  {i+1}. {m!r}: {summary_data['per_marker_hits'][i]}\n")

        if summary_data["errors"]:
            f.write("\n=== Errors ===\n")
            for err in summary_data["errors"]:
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
        "total_lines_removed": 0,
        "total_lines_salvaged": 0,
        "per_marker_hits": [0] * len(MARKERS),
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
                    summary["total_lines_removed"] += res["lines_removed"]
                    summary["total_lines_salvaged"] += res["lines_salvaged"]

                    # aggregate marker hits
                    if len(res["per_marker_hits"]) == len(summary["per_marker_hits"]):
                        for i, c in enumerate(res["per_marker_hits"]):
                            summary["per_marker_hits"][i] += c
                    else:
                        # defensive handling if config changed
                        for i in range(min(len(res["per_marker_hits"]), len(summary["per_marker_hits"]))):
                            summary["per_marker_hits"][i] += res["per_marker_hits"][i]

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
                if summary["files_scanned"] > 0:
                    elapsed = time.time() - summary["start_ts"]
                    avg = elapsed / summary["files_scanned"]
                    remaining = len(pending_files) - summary["files_scanned"]
                    eta_seconds = max(0, remaining * avg)
                    overall_bar.set_postfix_str(f"ETA: {str(timedelta(seconds=int(eta_seconds)))}")

    finally:
        overall_bar.close()
        write_summary(summary)

if __name__ == "__main__":
    main()
