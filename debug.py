#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ==== CONFIG (match your run) ====
INPUT_FOLDER = "cleaned_output"      # originals for this step (script input)
OUTPUT_FOLDER = "rewritten_output"   # rewritten outputs from the step you just ran
FINAL_FOLDER = "brackets_final"
FINAL_FILE = "brackets_final_mobile.txt"
ALLOWED_EXTS = (".txt",)
# =================================

import os, re, sys

final_path = os.path.join(FINAL_FOLDER, FINAL_FILE)

nonempty_no_mobile_re   = re.compile(r'^\[(CustomerNo|Mobile-No):\s*([0-9]+)\]\s*;\s*(\S.*)$')
nonempty_with_mobile_re = re.compile(r'^\[(CustomerNo|Mobile-No):\s*([0-9]+)\](.+)$')  # split by last ; later

def count_lines_in_folder(folder):
    total = 0
    files = [os.path.join(folder, f) for f in os.listdir(folder)
             if os.path.isfile(os.path.join(folder, f)) and os.path.splitext(f)[1].lower() in ALLOWED_EXTS]
    for p in files:
        with open(p, "r", encoding="utf-8", errors="ignore") as fin:
            for _ in fin: total += 1
    return total, files

def scan_input_candidates(folder):
    """Scan INPUT_FOLDER to estimate how many lines *should* have been moved/split."""
    should_move = 0
    should_split = 0
    cand_move_examples = []
    cand_split_examples = []

    files = [os.path.join(folder, f) for f in os.listdir(folder)
             if os.path.isfile(os.path.join(folder, f)) and os.path.splitext(f)[1].lower() in ALLOWED_EXTS]
    for p in files:
        with open(p, "r", encoding="utf-8", errors="ignore") as fin:
            for line in fin:
                s = line.rstrip("\n")

                # nonempty_no_mobile candidates in input
                if nonempty_no_mobile_re.match(s):
                    should_move += 1
                    if len(cand_move_examples) < 5: cand_move_examples.append(s)
                    continue

                # nonempty_with_mobile candidates in input (must have a last ';')
                m = nonempty_with_mobile_re.match(s)
                if m and ";" in s:
                    should_split += 1
                    if len(cand_split_examples) < 5: cand_split_examples.append(s)
    return should_move, should_split, cand_move_examples, cand_split_examples

def scan_final_file(path):
    cnt = 0
    exists = os.path.exists(path)
    if exists:
        with open(path, "r", encoding="utf-8", errors="ignore") as fin:
            for _ in fin: cnt += 1
    return exists, cnt

def main():
    # 1) Totals
    input_total, _  = count_lines_in_folder(INPUT_FOLDER)
    output_total, _ = count_lines_in_folder(OUTPUT_FOLDER)
    final_exists, final_total = scan_final_file(final_path)

    # 2) What *should* have been moved/split (based on INPUT content)
    should_move, should_split, ex_move, ex_split = scan_input_candidates(INPUT_FOLDER)

    # Expected relationships:
    # A) output_total == input_total - should_move
    # B) final_total  == should_move + should_split
    # C) output_total + final_total == input_total + should_split
    checkA = (output_total == input_total - should_move)
    checkB = (final_total == should_move + should_split)
    checkC = (output_total + final_total == input_total + should_split)

    print("=== AUDIT ===")
    print(f"Input lines      : {input_total}")
    print(f"Output lines     : {output_total}")
    print(f"Final file lines : {final_total}  ({'present' if final_exists else 'MISSING'})")
    print()
    print(f"Should move  (nonempty_no_mobile): {should_move}")
    print(f"Should split (nonempty_with_mobile): {should_split}")
    print()
    print(f"A) output == input - should_move        : {output_total} == {input_total} - {should_move}  => {'PASS' if checkA else 'FAIL'}")
    print(f"B) final  == should_move + should_split : {final_total} == {should_move} + {should_split}  => {'PASS' if checkB else 'FAIL'}")
    print(f"C) output + final == input + splits     : {output_total + final_total} == {input_total} + {should_split}  => {'PASS' if checkC else 'FAIL'}")
    print()

    if not checkA or not checkB or not checkC:
        print("Examples (first 5) of candidates from INPUT_FOLDER:")
        if ex_move:
            print("\n  nonempty_no_mobile (should move):")
            for s in ex_move: print("   ", s)
        if ex_split:
            print("\n  nonempty_with_mobile (should split):")
            for s in ex_split: print("   ", s)

if __name__ == "__main__":
    if not os.path.isdir(INPUT_FOLDER):
        print(f"ERROR: input folder not found: {INPUT_FOLDER}", file=sys.stderr); sys.exit(1)
    if not os.path.isdir(OUTPUT_FOLDER):
        print(f"ERROR: output folder not found: {OUTPUT_FOLDER}", file=sys.stderr); sys.exit(1)
    main()
