#!/usr/bin/env python3
"""Print uncovered source lines from the kcov merged report.

Usage:
  scripts/uncovered.py                 # summary: file -> uncovered line ranges
  scripts/uncovered.py channel/udp.zig # show each uncovered line with source text

Run scripts/coverage.sh first to (re)generate coverage/merged/.
"""
import re
import sys
import glob
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MERGED = os.path.join(ROOT, "coverage/merged/kcov-merged")

LINE_RE = re.compile(r'"lineNum":"\s*(\d+)".*?"class":"(lineCov|lineNoCov|linePartCov)"')
FILE_RE = re.compile(r'"link":"([^"]+)\.html".*?"summary_name":"\[\.\.\.\]([^"]*)"')


def load():
    """Return {repo-relative-path: [uncovered line numbers]}."""
    with open(os.path.join(MERGED, "index.js")) as f:
        idx = f.read()
    files = {}
    for m in FILE_RE.finditer(idx):
        js_base, tail = m.group(1), m.group(2)  # tail like /zigbolt/src/ffi/exports.zig
        if "/src/" not in tail:
            continue
        rel = "src/" + tail.split("/src/")[-1]
        js_path = os.path.join(MERGED, js_base + ".js")
        if not os.path.exists(js_path):
            continue
        missing = []
        with open(js_path) as f:
            for line in f:
                lm = LINE_RE.search(line)
                if lm and lm.group(2) == "lineNoCov":
                    missing.append(int(lm.group(1)))
        if missing:
            files[rel] = sorted(missing)
    return files


def ranges(nums):
    out, start, prev = [], None, None
    for n in nums:
        if start is None:
            start = prev = n
        elif n == prev + 1:
            prev = n
        else:
            out.append((start, prev))
            start = prev = n
    if start is not None:
        out.append((start, prev))
    return ", ".join(f"{a}" if a == b else f"{a}-{b}" for a, b in out)


def main():
    if not os.path.isdir(MERGED):
        print("coverage/merged/ not found — run scripts/coverage.sh first.")
        sys.exit(1)
    files = load()
    if len(sys.argv) > 1:
        needle = sys.argv[1]
        for rel, missing in sorted(files.items()):
            if needle not in rel:
                continue
            print(f"=== {rel} ({len(missing)} uncovered) ===")
            with open(os.path.join(ROOT, rel)) as f:
                src_lines = f.readlines()
            for n in missing:
                text = src_lines[n - 1].rstrip() if n <= len(src_lines) else "<?>"
                print(f"  {n:5d}: {text}")
    else:
        total = 0
        for rel, missing in sorted(files.items(), key=lambda kv: -len(kv[1])):
            total += len(missing)
            print(f"{len(missing):5d}  {rel}: {ranges(missing)}")
        print(f"\nTOTAL uncovered: {total}")


if __name__ == "__main__":
    main()
