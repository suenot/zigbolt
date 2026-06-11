#!/bin/sh
# ZigBolt line-coverage measurement.
#
# kcov has no macOS support and needs ptrace, so coverage is measured on Linux.
# The repo is mounted/run at its host absolute path so the DWARF source paths
# embedded in the test binaries resolve.
#
# IMPORTANT: the test binaries must be built with the LLVM backend. Zig 0.15.1's
# self-hosted x86_64 backend (the default for native x86_64 Debug builds) emits
# a DWARF5 line table whose file entries carry the vendor content type
# DW_LNCT_LLVM_source (0x2001); kcov's line parser doesn't understand it and
# silently extracts ZERO line records, reporting 0/0 coverage. The LLVM backend
# emits a standard file-name table that kcov parses correctly. This script
# selects the LLVM backend automatically:
#   - Linux host:   `zig build install-tests -Dcoverage` (forces use_llvm) and
#                   runs the native-arch binaries under kcov.
#   - macOS/other:  cross-compiles for aarch64-linux-gnu (cross builds already
#                   use the LLVM backend) and runs under kcov in a Linux image.
#
# kcov itself runs natively when it is on PATH (e.g. CI installs it), otherwise
# inside the Debian container `zigbolt-kcov` (see scripts/kcov.Dockerfile);
# Docker must be running in that case.
#
# Usage: ./scripts/coverage.sh
#   COVERAGE_MIN=100   # optional: exit non-zero if measurable % is below this
# Output: coverage/merged/kcov-merged/index.html + percent on stdout.
set -e
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

HOST_OS="$(uname -s)"
HOST_ARCH="$(uname -m)"

# ── 1. Build the test binaries with the LLVM backend ─────────────
NATIVE_BINS=0
if [ "$HOST_OS" = "Linux" ]; then
    zig build install-tests -Dcoverage
    NATIVE_BINS=1
    case "$HOST_ARCH" in
        aarch64 | arm64) PLATFORM="linux/arm64" ;;
        *) PLATFORM="linux/amd64" ;;
    esac
else
    # macOS's Zig probes Xcode for the cross sysroot; point it at the CLI tools.
    DEVELOPER_DIR=/Library/Developer/CommandLineTools zig build install-tests -Dtarget=aarch64-linux-gnu
    PLATFORM="linux/arm64"
fi

# kcov-skip: marker for lines that are provably untestable in-process
# (defensive unreachable/panic guards, OS-failure branches that cannot be
# injected). Use sparingly and always with an inline justification. The
# --exclude-line flag must be repeated on the merge step too: the merge
# re-parses the sources and would otherwise reintroduce marker lines.
KCOV_CMDS="
    kcov --include-pattern=$ROOT/src/ --exclude-line='kcov-skip' coverage/root zig-out/tests/root_tests >/dev/null 2>&1
    kcov --include-pattern=$ROOT/src/ --exclude-line='kcov-skip' coverage/ffi zig-out/tests/ffi_tests >/dev/null 2>&1
    kcov --merge --exclude-line='kcov-skip' coverage/merged coverage/root coverage/ffi >/dev/null 2>&1
"

# ── 2. Run kcov (native if available, else containerized) ────────
if [ "$NATIVE_BINS" = "1" ] && command -v kcov >/dev/null 2>&1; then
    rm -rf coverage
    mkdir -p coverage
    sh -c "$KCOV_CMDS"
else
    # kcov writes as root inside the container, so clean the prior run from
    # inside the container to avoid host permission errors.
    docker run --rm -v "$ROOT:$ROOT" -w "$ROOT" zigbolt-kcov rm -rf coverage
    mkdir -p coverage
    docker run --rm --platform "$PLATFORM" \
        --cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
        -v "$ROOT:$ROOT" -w "$ROOT" --tmpfs /tmp:exec,size=512m \
        zigbolt-kcov sh -c "$KCOV_CMDS"
fi

# ── 3. Report (and optionally enforce a minimum) ─────────────────
COVERAGE_MIN="${COVERAGE_MIN:-}" python3 - <<'EOF'
import json, os, sys
with open('coverage/merged/kcov-merged/coverage.json') as f:
    d = json.load(f)
pct = float(d['percent_covered'])
print(f"TOTAL: {d['percent_covered']}% ({d['covered_lines']}/{d['total_lines']} measurable lines)")
files = sorted(d['files'], key=lambda x: float(x['percent_covered']))
print("\nPer-file (worst first):")
for fl in files:
    print(f"  {float(fl['percent_covered']):6.2f}%  {int(fl['total_lines'])-int(fl['covered_lines']):5d} uncovered  {fl['file'].split('/src/')[-1]}")
minimum = os.environ.get('COVERAGE_MIN', '')
if minimum:
    if pct + 1e-9 < float(minimum):
        print(f"\nFAIL: measurable coverage {pct}% is below the required {minimum}%", file=sys.stderr)
        sys.exit(1)
    print(f"\nOK: measurable coverage {pct}% meets the required {minimum}%")
EOF
