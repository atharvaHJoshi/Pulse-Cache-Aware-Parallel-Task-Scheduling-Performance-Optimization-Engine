#!/usr/bin/env bash
# Run the full Pulse project end-to-end:
#   1. configure + build (Release by default, tests + benchmarks enabled)
#   2. run the unit test suite
#   3. run the OCaml differential check (skips cleanly if OCaml is missing)
#   4. run the full benchmark matrix (all workloads x policies x worker counts)
#   5. render data/report/report.html (and optional PNGs)
#
# Override behaviour with environment variables, e.g.:
#   BUILD_DIR=... BUILD_TYPE=Debug WORKERS="1 4 8" REPEATS=2 JOBS=4 ./run.sh
#   CLEAN=1               wipe the build directory before configuring
#   SANITIZE=thread       build with the named sanitizer (address/thread/undefined)
#   RUN_SANITIZERS=1      also build + test ASan/UBSan and TSan
#   MATPLOTLIB=1          also emit PNG charts
#   RUN_OCAML=0           skip the differential check

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

BUILD_DIR="${BUILD_DIR:-$ROOT/build}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
WORKERS="${WORKERS:-1 2 4 8 16}"
REPEATS="${REPEATS:-5}"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
OUT="${OUT:-$ROOT/data/pulse_results.csv}"
SANITIZE="${SANITIZE:-}"
RUN_SANITIZERS="${RUN_SANITIZERS:-0}"
RUN_OCAML="${RUN_OCAML:-1}"
MATPLOTLIB="${MATPLOTLIB:-0}"
CLEAN="${CLEAN:-0}"

# ---------- clean up stale builds -------------------------------------------
if [[ "$CLEAN" == "1" ]]; then
    rm -rf "$BUILD_DIR"
elif [[ -f "$BUILD_DIR/tests/pulse_tests" ]] && ldd "$BUILD_DIR/tests/pulse_tests" 2>/dev/null | grep -q 'not found'; then
    echo "==> stale build detected; rebuilding"
    rm -rf "$BUILD_DIR"
fi

# ---------- configure + build -----------------------------------------------
SANITIZE_ARG=()
if [[ -n "$SANITIZE" ]]; then SANITIZE_ARG=(-DPULSE_SANITIZE="$SANITIZE"); fi

echo "==> configuring ($BUILD_TYPE) in $BUILD_DIR"
cmake -S "$ROOT" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON "${SANITIZE_ARG[@]}" >/dev/null

echo "==> building with $JOBS jobs"
cmake --build "$BUILD_DIR" -j "$JOBS"

# ---------- tests ------------------------------------------------------------
echo "==> running tests"
(cd "$BUILD_DIR" && ctest --output-on-failure)

# ---------- differential check vs OCaml -------------------------------------
if [[ "$RUN_OCAML" == "1" ]]; then
    echo "==> OCaml differential check"
    "$ROOT/scripts/check_ocaml.sh" || true
fi

# ---------- optional sanitizer builds ----------------------------------------
if [[ "$RUN_SANITIZERS" == "1" ]]; then
    for mode in address thread; do
        echo "==> sanitizer build: $mode"
        sdir="$ROOT/build-$mode"
        rm -rf "$sdir"
        cmake -S "$ROOT" -B "$sdir" -DCMAKE_BUILD_TYPE=Debug -DPULSE_SANITIZE="$mode" \
            -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON >/dev/null
        cmake --build "$sdir" -j "$JOBS"
        (cd "$sdir" && ctest --output-on-failure)
    done
fi

# ---------- benchmark matrix -------------------------------------------------
BENCH="$BUILD_DIR/benchmarks/pulse_bench"
[[ -x "$BENCH" ]] || { echo "error: $BENCH not found"; exit 1; }

mkdir -p "$(dirname "$OUT")"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "==> running benchmark matrix"
: > "$OUT"
first=1
read -ra WC_ARRAY <<< "$WORKERS"
for wc in "${WC_ARRAY[@]}"; do
    echo "   workers=$wc"
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

# ---------- report -----------------------------------------------------------
if command -v python3 >/dev/null 2>&1; then
    echo "==> rendering report"
    PLOT_ARGS=("$OUT" --out "$ROOT/data/report")
    if [[ "$MATPLOTLIB" == "1" ]]; then PLOT_ARGS+=(--matplotlib); fi
    python3 "$ROOT/scripts/plot_results.py" "${PLOT_ARGS[@]}"
fi

echo "==> done"
echo "    tests: PASS"
echo "    results: $OUT"
echo "    report:  $ROOT/data/report/report.html"
