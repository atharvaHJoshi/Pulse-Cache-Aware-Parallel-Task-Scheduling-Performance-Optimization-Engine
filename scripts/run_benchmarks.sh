#!/usr/bin/env bash
# Build Pulse and run the full benchmark matrix.
#
# Produces data/pulse_results.csv (all worker counts concatenated) and, if
# Python is available, data/report/report.html.
#
# Environment overrides:
#   BUILD_DIR    build directory           (default: <root>/build)
#   BUILD_TYPE   CMake build type          (default: Release)
#   WORKERS      space-separated worker counts (default: "1 2 4 8 16")
#   REPEATS      repetitions per cell      (default: 5)
#   JOBS         parallel build jobs       (default: nproc)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BUILD_DIR="${BUILD_DIR:-$ROOT/build}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
WORKERS="${WORKERS:-1 2 4 8 16}"
REPEATS="${REPEATS:-5}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
OUT="${OUT:-$ROOT/data/pulse_results.csv}"

echo "==> configuring ($BUILD_TYPE) in $BUILD_DIR"
cmake -S "$ROOT" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
      -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON >/dev/null

echo "==> building with $JOBS jobs"
cmake --build "$BUILD_DIR" -j "$JOBS" >/dev/null

echo "==> running tests"
( cd "$BUILD_DIR" && ctest --output-on-failure )

BENCH="$BUILD_DIR/benchmarks/pulse_bench"
[[ -x "$BENCH" ]] || { echo "error: $BENCH not found"; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

mkdir -p "$(dirname "$OUT")"
: > "$OUT"

first=1
for wc in $WORKERS; do
    echo "==> benchmarking with $wc workers"
    "$BENCH" --workers "$wc" --repeats "$REPEATS" --out "$TMP/w$wc.csv" > "$TMP/w$wc.log" 2>&1
    if [[ $first -eq 1 ]]; then
        cat "$TMP/w$wc.csv" >> "$OUT"
        first=0
    else
        tail -n +2 "$TMP/w$wc.csv" >> "$OUT"
    fi
    grep -v '^wrote ' "$TMP/w$wc.log" || true
done

echo "==> wrote $OUT"

if command -v python3 >/dev/null 2>&1; then
    python3 "$ROOT/scripts/plot_results.py" "$OUT" --out "$ROOT/data/report" || true
fi
