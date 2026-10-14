#!/usr/bin/env bash
# Differential test: compare the C++ engine's DAG analysis against the
# independent OCaml reference model.
#
# For every standard workload it checks that
#   * the topological order is acyclic,
#   * the total critical path matches, and
#   * every per-task bottom level (critical-path-to-sink) matches.
#
# Skips cleanly (exit 0) when no OCaml toolchain is available so the rest of
# the build is not blocked.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORKLOADS=(chain-1k forkjoin-d10 layered-20x100 random-5k-3 dataflow-8k straggler contended)
WORKERS=8

# --- locate an OCaml compiler -------------------------------------------------
OCAMLOPT="${OCAMLOPT:-}"
if [[ -z "$OCAMLOPT" ]]; then
    if command -v ocamlopt >/dev/null 2>&1; then
        OCAMLOPT=ocamlopt
    elif [[ -x /tmp/opencode/ocaml-prefix/bin/ocamlopt ]]; then
        OCAMLOPT=/tmp/opencode/ocaml-prefix/bin/ocamlopt
    fi
fi

if [[ -z "$OCAMLOPT" ]]; then
    echo "SKIP: no ocamlopt found; install OCaml or run 'make' in ocaml/"
    exit 0
fi

# --- locate / build the C++ dumper -------------------------------------------
BENCH=""
for cand in build/benchmarks/pulse_bench build/pulse_bench; do
    [[ -x "$cand" ]] && BENCH="$cand" && break
done
if [[ -z "$BENCH" ]]; then
    echo "SKIP: pulse_bench not built; run cmake --build build first"
    exit 0
fi

# --- build the reference model -----------------------------------------------
REF="$ROOT/ocaml/pulse_ref"
if [[ ! -x "$REF" ]]; then
    ( cd "$ROOT/ocaml" && "$OCAMLOPT" -o pulse_ref pulse_ref.ml )
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

status=0
for w in "${WORKLOADS[@]}"; do
    "$BENCH" --dump-dag "$w" > "$TMP/$w.dag"
    out="$("$REF" "$TMP/$w.dag" "$WORKERS")"

    cpp_cp="$(awk '/^critical_path /{print $2}' "$TMP/$w.dag")"
    ref_cp="$(awk '/^critical_path /{print $2}' <<<"$out")"
    topo_ok="$(awk '/^topo_ok /{print $2}' <<<"$out")"

    if [[ "$topo_ok" != "true" ]]; then
        echo "FAIL $w: reference model reports a cyclic graph"; status=1; continue
    fi
    if [[ "$cpp_cp" != "$ref_cp" ]]; then
        echo "FAIL $w: critical_path cpp=$cpp_cp ref=$ref_cp"; status=1; continue
    fi
    if ! diff -q <(grep '^bottom ' "$TMP/$w.dag") <(grep '^bottom ' <<<"$out") >/dev/null; then
        echo "FAIL $w: per-task bottom levels differ"; status=1; continue
    fi
    printf 'ok   %-16s critical_path=%-9s topo_ok=%s\n' "$w" "$ref_cp" "$topo_ok"
done

if [[ $status -eq 0 ]]; then
    echo "all workload DAGs agree with the OCaml reference model"
fi
exit $status
