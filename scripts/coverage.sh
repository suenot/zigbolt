#!/bin/sh
# ZigBolt line-coverage measurement.
#
# kcov has no macOS support, so coverage is measured on Linux: the test
# binaries are cross-compiled (aarch64-linux-gnu) and run under kcov inside
# a Debian container (image `zigbolt-kcov`, see scripts/kcov.Dockerfile).
# The repo is mounted at its host absolute path so DWARF source paths resolve.
#
# Usage: ./scripts/coverage.sh
# Output: coverage/kcov-merged/index.html + percent on stdout.
set -e
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

DEVELOPER_DIR=/Library/Developer/CommandLineTools zig build install-tests -Dtarget=aarch64-linux-gnu

rm -rf coverage
mkdir -p coverage

docker run --rm --platform linux/arm64 \
    --cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
    -v "$ROOT:$ROOT" -w "$ROOT" --tmpfs /tmp:exec,size=512m \
    zigbolt-kcov sh -c "
        # kcov-skip: marker for lines that are provably untestable in-process
        # (defensive unreachable/panic guards, OS-failure branches that cannot
        # be injected). Use sparingly and always with an inline justification.
        kcov --include-pattern=$ROOT/src/ --exclude-line='kcov-skip' coverage/root zig-out/tests/root_tests >/dev/null 2>&1
        kcov --include-pattern=$ROOT/src/ --exclude-line='kcov-skip' coverage/ffi zig-out/tests/ffi_tests >/dev/null 2>&1
        kcov --merge coverage/merged coverage/root coverage/ffi >/dev/null 2>&1
    "

python3 - <<'EOF'
import json
with open('coverage/merged/kcov-merged/coverage.json') as f:
    d = json.load(f)
print(f"TOTAL: {d['percent_covered']}% ({d['covered_lines']}/{d['total_lines']} lines)")
files = sorted(d['files'], key=lambda x: float(x['percent_covered']))
print("\nPer-file (worst first):")
for fl in files:
    print(f"  {float(fl['percent_covered']):6.2f}%  {int(fl['total_lines'])-int(fl['covered_lines']):5d} uncovered  {fl['file'].split('/src/')[-1]}")
EOF
