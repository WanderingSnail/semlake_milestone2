# SemLake: Zero-Copy Branching and 3-Way Semantic Merging for Lakehouses

## Overview

SemLake applies 3-way semantic merge, as used for source code, to branched tables in a data lakehouse such as Apache Iceberg or Delta Lake. The milestone 2 prototype is a single-process engine with a benchmark harness.

A merge writes metadata only. Deletes go into roaring deletion vectors and the base Parquet files are never rewritten. Rows merge under 3-way rules for inserts, updates, deletes and compatible schema adds. When both branches set the same cell to different values, the merge commits and records both values in a version-space (VSA) choice node. A semantic check blocks the merge when one branch deletes rows that an aggregate on the other branch still reads.

The harness compares SemLake with the four baselines from the proposal. B1 is file-level OCC and B2 is copy-on-write. B3 is a tuple-version dictionary in the style of Decibel and OrpheusDB. B4 is a CRDT op log with last-writer-wins and no branch checkpoint.

The object store is an in-memory S3 stand-in. It counts every PUT, GET and LIST, and the harness prices them at the public AWS S3 Standard request rates. The CSV also has an estimated cloud latency: measured CPU, plus a fixed round trip per request, plus put bytes over bandwidth. The harness never sleeps to imitate a slow network. The latency plot shows measured merge CPU time, since a few round trips would dominate the estimate and hide the CPU gap.

## Layout

```
semlake/                 core engine
  metadata.py            commit DAG, branches, LCA, schema union
  storage.py             object store, Parquet, roaring deletion vectors
  merge.py               3-way diff, zero-copy merge, VSA nodes
  verification.py        predicate checks and WHERE-clause overlap (W3)
  baselines.py           B1 OCC, B2 CoW, B3 tuple versions, B4 CRDT log
benchmarks/
  profiler.py            request prices and latency model
  workloads.py           W1-W4, the disjoint-file cost case, and the Q6-style scan
  plot.py                latency, S3 cost, and TPS figures
  run_all.py             one-command harness
  results/               benchmark_run_summary.csv and traces.json
  plots/                 pdf and png figures
```

## Run

From the repository root, with Python 3.11 or later:

```bash
pip install -r requirements.txt && python benchmarks/run_all.py --quick
```

`--quick` is the smoke test, with small manifests and fewer trials. It writes `benchmarks/results/` and `benchmarks/plots/` and then checks the milestone hypotheses. The exit code is non-zero if a check fails.

The full matrix covers manifest depths of 1K, 5K and 10K files and the whole contention curve:

```bash
python benchmarks/run_all.py
```

Unit tests:

```bash
python -m unittest tests.test_core -v
```

## Workloads and metrics

| Workload | Setup | Recorded |
| --- | --- | --- |
| W1 | Two branches update different rows of the same file | False-abort flag. B1 aborts. SemLake, B3 and B4 commit. B2 rewrites the file, and B3 writes a version record for every tuple. |
| W1S | Each branch updates rows in its own half of the files | Request count and API cost. B2 rewrites every changed file. SemLake writes a delta manifest. |
| W2 | Contention rate from 0 to 1 on a shared row | Successful merges per CPU second, with no client retry loop. B1 falls to 0. SemLake keeps the conflict in a VSA node. B4 keeps committing and overwrites the conflict. |
| W3 | Branch A runs `DELETE ... WHERE kind = 'vip'`; branch B writes an aggregate over `WHERE kind = 'vip' AND value >= 10` | Semantic recall. Each commit records only its WHERE clause. SemLake is the only system that returns `semantic_conflict`. |
| W3C | Same setup, except that A deletes `kind = 'spam'` | False-positive flag. Every system, SemLake included, must commit. |
| W4 | Column add plus one appended file, at 1K to 10K manifest files | Merge CPU time. A nullable column add leaves untouched files alone. |
| M4 | Synthetic Q6-style DuckDB scan with a 5% deletion vector | Compacted scan, full scan without a mask, masked scan, and overhead. |

## Plots

- `benchmarks/plots/latency_vs_manifest_depth.pdf`
- `benchmarks/plots/s3_cost_comparison.pdf`
- `benchmarks/plots/tps_vs_contention.pdf`

PNG copies sit next to the PDFs. The figures are drawn from the CSV; `traces.json` holds the same rows with more detail.

## Findings

On W1, SemLake merges disjoint row updates in one file (`false_abort = 0`) and B1 aborts (`false_abort = 1`). B1 decides per file, so its row puts the overlapping files in `file_conflicts` and leaves `cell_conflicts` at 0.

On W3, SemLake returns `semantic_conflict` and B1 to B4 commit, so they score recall 0. The checker compares the WHERE clause of the delete with the WHERE clause of the aggregate and finds a value that both can match, without reading table data. The proposal gives B3 20% recall on a broader suite; this harness has one case and does not reproduce that figure. On W3C the two clauses cannot match the same row, and SemLake commits (`false_positive = 0`).

On W2 at full contention, B1 throughput is 0. SemLake still commits and keeps a VSA node, and B4 commits by last-writer-wins. Throughput is successful merges divided by CPU time.

On W4, SemLake's merge CPU time stays flat as the manifest grows. At 10K files it is at least 10x below B1, which scans the full file list. The estimated cloud latency in the CSV shows a much smaller gap, because both systems issue only a handful of requests and the fixed round trips outweigh the CPU difference.

On W1S, SemLake's request cost is at most 5% of B2's, since B2 rewrites each changed file. W4 has no such gap: B2 also treats an added nullable column as a metadata change, as Iceberg and Delta do.

## Limitations

The proposal targets a scan overhead under 8% for M4, and this run misses it. The overhead is 8% to 17% across five repeated runs and 14.6% in the committed run. The microbenchmark is a Q6-style query over 1e6 synthetic rows held in memory, much smaller than TPC-H SF1 or SF10. The CSV splits the overhead in two parts. Reading the 5% of deleted rows adds 2% to 7%, close to the 5.3% expected from reading 1/0.95 as many rows. Reading and testing the mask column adds 4% to 12%, and that part stays when the delete rate drops to 0.1%. DuckDB has no Python hook for an external selection vector, so the deletion vector becomes an extra column that every row reads. The Delta and Iceberg readers apply the bitmap inside the scan and would skip that column. This prototype has no such reader.

The WHERE-clause check handles conjunctions of comparisons, `IN` and `BETWEEN` against literals. It counts `OR`, `NOT`, functions and joins as a possible overlap, so it can over-report on them. The writer attaches the clause to the commit; nothing derives predicates from query plans.

Schema rename is not implemented and dropping a column is rejected. Type widening is supported, though W4 only adds a column.

The prototype does not use PyIceberg, delta-rs, Lance, MinIO or LST-Bench. W4 files are small shared buffers, and there is no measured resident-memory figure.

## Repository access

Keep this repository private. Before sharing the link, add the instructor as a collaborator in the host's access settings.
