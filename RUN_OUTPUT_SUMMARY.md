# Pulse run output summary

This document summarizes a successful full run of `./run.sh` and explains what the
program is doing.

## What Pulse is

Pulse is a C++20 benchmark-driven parallel task scheduler. It runs the same
directed acyclic graphs (DAGs) of tasks through four scheduling policies on an
identical execution engine:

- `fifo` — one global FIFO ready queue
- `work-stealing` — per-worker deques with stealing
- `critical-path` — distributed priority queues by bottom-level
- `locality-aware` — critical-path placement plus measured data locality and an
  adaptive load override

The runtime owns thread pooling, dependency resolution, wakeups, timing, and
statistics. A policy only decides which ready task a worker runs next.

## What this run did

1. Configured and built Pulse in Release mode with tests and benchmarks enabled.
2. Ran all 10 GoogleTest tests; all passed.
3. Ran the OCaml differential check. It skipped cleanly because `ocamlopt` is not
   installed.
4. Ran the benchmark matrix for worker counts 1, 2, 4, 8, and 16, 5 repeats each.
5. Wrote aggregated benchmark rows to `data/pulse_results.csv`.
6. Rendered `data/report/report.html`.

## Metrics in the output

Each benchmark line reports, per workload and policy:

- `makespan` — wall time from first task start to last task end
- `tput` — tasks completed per second
- `util` — CPU utilization (`total work / (makespan x workers)`)
- `idle` — idle time fraction
- `imb` — load imbalance (`std-dev / mean` of per-worker busy time)
- `p50`, `p95`, `p99` — task completion-latency percentiles
- `steals` — successful cross-worker acquisitions
- `migr` — tasks migrated away from their locality home

## Representative results at 8 workers

| Workload | Policy | Makespan (ms) | Steals |
|---|---|---:|---:|
| chain-1k | fifo | 22.535 | 0 |
| chain-1k | work-stealing | 22.391 | 2 |
| chain-1k | critical-path | 22.501 | 3 |
| chain-1k | locality-aware | 22.674 | 3 |
| forkjoin-d10 | fifo | 2.953 | 0 |
| forkjoin-d10 | work-stealing | 2.983 | 50 |
| forkjoin-d10 | critical-path | 2.984 | 271 |
| forkjoin-d10 | locality-aware | 2.917 | 11 |
| layered-20x100 | fifo | 13.428 | 0 |
| layered-20x100 | work-stealing | 13.409 | 282 |
| layered-20x100 | critical-path | 13.284 | 545 |
| layered-20x100 | locality-aware | 13.448 | 39 |
| random-5k-3 | fifo | 40.180 | 0 |
| random-5k-3 | work-stealing | 40.126 | 213 |
| random-5k-3 | critical-path | 40.029 | 1671 |
| random-5k-3 | locality-aware | 39.739 | 92 |
| dataflow-8k | fifo | 6.957 | 0 |
| dataflow-8k | work-stealing | 5.016 | 251 |
| dataflow-8k | critical-path | 5.256 | 493 |
| dataflow-8k | locality-aware | 5.835 | 79 |
| straggler | fifo | 37.242 | 0 |
| straggler | work-stealing | 26.568 | 4003 |
| straggler | critical-path | 26.940 | 3510 |
| straggler | locality-aware | 26.351 | 30 |
| contended | fifo | 2.443 | 0 |
| contended | work-stealing | 2.455 | 5246 |
| contended | critical-path | 5.867 | 5352 |
| contended | locality-aware | 2.007 | 424 |

## What the results show

- `chain-1k` is almost entirely sequential, so more workers do not help; makespan
  stays near 22 ms regardless of policy.
- `straggler` has parallel cheap tasks in front of a heavy chain. Work-stealing
  and critical-path reduce makespan dramatically versus FIFO, while
  locality-aware matches that speed with far fewer steals.
- `contended` touches shared cache regions. `locality-aware` is fastest and
  reduces steals by roughly an order of magnitude compared with work-stealing and
  critical-path because it keeps region traffic on fewer cores.
- `random-5k-3` is utilization-bound. All policies have similar makespan, while
  priority-based policies increase tail latency by running urgent tasks first.
- `dataflow-8k` benefits from task priorities. Locality-aware has fewer steals
  than the other priority policies while remaining close in makespan.
- Worker scaling improves makespan for parallel workloads, but utilization falls
  once worker count exceeds available parallelism. For example, `straggler` at
  16 workers has utilization around 0.28 because of its long critical chain.

## Output files

- `data/pulse_results.csv` — aggregated benchmark data
- `data/report/report.html` — interactive HTML/SVG report
- `run.sh` — single script used to run the whole project

## Notes

- The OCaml checker was skipped only because `ocamlopt` is not installed. All
  C++ tests still pass.
- If an OCaml toolchain is installed, `./scripts/check_ocaml.sh` cross-checks
  critical paths and per-task bottom levels against the independent reference
  model.
