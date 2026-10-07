# Milestone 2: working prototype and code

Author: Xiaofan Cao

This file maps each Milestone 2 deliverable to the files that provide it. It also lists which targets from the Milestone 1 proposal this run meets and which it misses.

## Deliverables

| Deliverable | Where it is |
| --- | --- |
| Instructor-accessible Git repository | This repository, branch `main`. The instructor gets access as a collaborator on the host. |
| Automated benchmark harness | `benchmarks/run_all.py`. `python benchmarks/run_all.py --quick` is the smoke test and `python benchmarks/run_all.py` runs the full matrix. Both exit non-zero if a hypothesis check fails. CI runs the unit tests and the smoke test (`.github/workflows/ci.yml`). |
| Raw execution trace logs (CSV/JSON) | `benchmarks/results/benchmark_run_summary.csv` has one row per workload and system. `benchmarks/results/traces.json` has the same rows plus the run config, the pricing table, semantic violations and VSA counts. |
| Preliminary performance plots | `benchmarks/plots/latency_vs_manifest_depth.{pdf,png}`, `benchmarks/plots/s3_cost_comparison.{pdf,png}`, `benchmarks/plots/tps_vs_contention.{pdf,png}`. |

## Contents

```
README.md                 setup, workloads, and findings
MILESTONE2.md             this file
requirements.txt          pyarrow, duckdb, pyroaring, matplotlib, numpy
semlake/                  prototype engine
  metadata.py             commit DAG, LCA, schema union, snapshot commits
  storage.py              in-memory object store, Parquet IO, roaring deletion vectors
  merge.py                3-way diff, metadata-only merge, VSA choice nodes
  verification.py         semantic checks and WHERE-clause overlap analysis
  baselines.py            B1 file OCC, B2 copy-on-write, B3 tuple versions, B4 CRDT log
benchmarks/
  profiler.py             S3 request pricing and latency model
  workloads.py            W1, W1S, W2, W3, W3C, W4, and the M4 scan
  run_all.py              harness and hypothesis checks
  plot.py                 figures from the CSV
  results/                CSV and JSON traces from the full run
  plots/                  PDF and PNG figures from the full run
tests/test_core.py        unit tests (12)
.github/workflows/ci.yml  CI: unit tests plus the smoke test
```

## Reproduce

```bash
pip install -r requirements.txt
python -m unittest tests.test_core -v
python benchmarks/run_all.py --quick --results /tmp/semlake_results --plots /tmp/semlake_plots
python benchmarks/run_all.py
```

The full run takes under a minute on a laptop and rewrites `benchmarks/results/` and `benchmarks/plots/`. The `--quick` example writes to separate directories so the full-run artifacts stay as they are.

## Results against the proposal metrics

The numbers come from the committed full run. Timings vary a little from run to run.

| Metric | Proposal target | Measured | Status |
| --- | --- | --- | --- |
| M1 false aborts on W1 | 0 | SemLake 0; B1 aborts on 1 overlapping file | Met |
| M1 semantic recall on W3 | 100% | SemLake 1/1; B1-B4 0/1 | Met on this case |
| M1 false positives on W3C (control) | not stated | 0 for every system | Met |
| M2 merge time at 10K manifest files | sub-second, 10x faster than B1/B2 | SemLake CPU about 0.3 ms, B1 about 71 ms, B2 about 78 ms (W4) | Met on CPU time. On estimated cloud latency the gap is small, because a few fixed round trips dominate both sides. |
| M3 S3 request cost vs B2 | at least 95% lower | about 99.97% lower on W1S at 10K files (6 vs 20007 requests) | Met on W1S. W4 has no cost gap, since B2 also adds a nullable column as metadata. |
| M4 scan overhead before compaction | under 8% | 14.6% in the committed run; 8-17% across five repeated runs | Not met |

## Known gaps

M4 misses its target. The prototype applies the deletion vector as a boolean column that DuckDB reads for every row, because DuckDB has no Python hook for an external selection vector. Reading the 5% of deleted rows adds 2% to 7%, close to the expected 5.3%. Reading and testing the mask column adds 4% to 12%, and that part stays when the delete rate drops to 0.1%. A reader that applies the bitmap inside the scan would avoid the mask column, and the prototype has none. Each query takes about 6 ms, so run-to-run noise is several percent. The harness interleaves the three queries in every round so that drift spreads over all of them. The data is a synthetic Q6-style table with 1e6 rows, much smaller than TPC-H SF1 or SF10.

Each commit records the WHERE clause of its delete or of the query it read, and the checker decides statically whether two clauses can match a common row. It supports conjunctions of comparisons, `IN` and `BETWEEN` on literals, and counts `OR`, `NOT`, functions and joins as a possible overlap. It does not extract predicates from query plans.

B1 decides per file. Its rows report overlapping files in `file_conflicts` and leave `cell_conflicts` at 0.

The proposal's substrate (PyIceberg or delta-rs, Lance, MinIO, LST-Bench, 512 MB files) is not used. The object store lives in memory, and every request is counted and priced at AWS S3 Standard rates. W4 and W1S data files are small shared buffers.

Additive columns and type widening are supported. Rename is not implemented and a column drop is rejected.

W2 throughput is successful merges per CPU second, with no client retry model.

The proposal lists 20% recall for B3 on a broader suite. B3 scores 0 on W3 here.
