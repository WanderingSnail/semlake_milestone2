# SemLake: Zero-Copy Branching and 3-Way Semantic Merging for Lakehouses

## Overview

SemLake applies 3-way semantic merge, as used for source code, to branched tables in a data lakehouse such as Apache Iceberg or Delta Lake. The milestone 2 prototype is a single-process engine with a benchmark harness.

A merge writes metadata and one small delta file. Deletes, and the old positions of updated rows, go into roaring deletion vectors stored as positions inside each file. The new values are appended to a delta Parquet file, and the base files stay where they are. The merge snapshot is registered, so the result can be read back or committed on again. Rows merge under 3-way rules for inserts, updates, deletes and compatible schema adds. When both branches set the same cell to different values, the merge commits, keeps the base value for that cell, and records both options in a version-space (VSA) choice node. A semantic check blocks the merge when one branch deletes rows that an aggregate on the other branch still reads, when an assignment breaks a domain clause, or when a delete removes a declared foreign-key parent.

The harness compares SemLake with the four baselines from the proposal. B1 is file-level OCC and B2 is copy-on-write. B3 is a tuple-version dictionary in the style of Decibel and OrpheusDB. B4 is a CRDT op log with last-writer-wins and no branch checkpoint.

The object store is an in-memory S3 stand-in. It counts every PUT, GET and LIST, and the harness prices them at the public AWS S3 Standard request rates. The CSV also has an estimated cloud latency: measured CPU, plus a fixed round trip per request, plus put bytes over bandwidth. The harness never sleeps to imitate a slow network. The latency plot shows measured merge CPU time, since a few round trips would dominate the estimate and hide the CPU gap.

## Layout

```
semlake/                 core engine
  metadata.py            commit DAG, branches, LCA, schema union
  storage.py             object store, Parquet, roaring deletion vectors
  merge.py               3-way diff, zero-copy merge, VSA nodes
  verification.py        predicate checks and WHERE-clause overlap (W3)
  sql_predicates.py      WHERE text from one SELECT or DELETE
  baselines.py           B1 OCC, B2 CoW, B3 tuple versions, B4 CRDT log
benchmarks/
  profiler.py            request prices and latency model
  workloads.py           W1-W4, the disjoint-file cost case, and the Q6-style scan
  cost_efficiency.py     dollars per 1,000 merges, compaction, request growth
  swarm_service.py       catalog CAS, group commit, one head per row
  delta_baseline.py      second Delta writer from the same version
  dolt_baseline.py       labelled cases replayed in Dolt
  plot.py                latency, S3 cost, and TPS figures
  run_all.py             one-command harness
  results/               CSV, JSON traces, M4 timings, cost report
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

## Reproducing the PVLDB paper

The paper is `paper/semlake_pvldb.tex` (PVLDB template, acmart `sigconf,nonacm`). Every number and table in it is generated from `benchmarks/results/` by `benchmarks/paper_numbers.py`; nothing is typed in. Run, in this order, one at a time so that timings do not compete for CPU:

```bash
pip install -r requirements.txt            # adds tpchgen-cli for real TPC-H data
python benchmarks/scion_sim.py             # swarm: 8-64 agents, 5 seeds (scion_sim*.csv/json)
python benchmarks/tpch_experiments.py pairs        # 42 TPC-H refresh-vs-report pairs, incl. Dolt
python benchmarks/tpch_experiments.py selectivity  # false blocks vs purge size
python benchmarks/run_all.py               # W1-W4, H1, labelled suite (m1s_cases.csv)
python benchmarks/tpch_experiments.py scan         # Q6 on SF 1 lineitem with open DVs
python benchmarks/tpch_experiments.py compaction   # Q6 overhead under compaction policies
python benchmarks/cost_efficiency.py       # dollars per 1,000 merges (reads tpch_scan.json)
python benchmarks/paper_experiments.py all # packing ablation and all paper figures
python benchmarks/paper_numbers.py         # paper/generated/*.tex
cd paper && latexmk -pdf semlake_pvldb.tex
```

Labels in the code and in the paper:

| Code | Paper |
| --- | --- |
| W1, W1S, `packing.json` | W1 (shared-file updates); W1S is the 10K-file request count |
| W2, W2L | W2 (hot rows, contention kappa) |
| W3, W3C, M1S, `tpch_pairs.json`, `selectivity.json` | W3 and M1 |
| W4, H1 | W4 (schema change and table size; H1 is history depth) |
| `tpch_scan.json`, `compaction.json` | M4 |
| `cost_efficiency.json`, W1S request counts | M3 |
| `scion_sim*` | agent swarm (Section 7.6) |
| b1, b1r, b2, b3, b4, dolt, b6 | B1, B1R, B2, B3, B4, B5, B6 |

B1R and B6 are rule models in `benchmarks/rule_baselines.py`; `semlake_static` is SemLake with the row check turned off. Compaction and group commit exist only in the benchmark harness. The sections below describe the milestone 2 harness; their numbers predate the paper runs.

## Milestone 2 record

The sections from here to "Limitations" record the milestone 2 runs and are kept as that record. Several numbers changed after the engine fix of 17 October 2026 (for example, Dolt is now a 2.5.0 source build and the TPC-H runs use real `lineitem`). For current numbers, use `paper/generated/numbers.tex`, which `benchmarks/paper_numbers.py` writes from `benchmarks/results/` (see "Reproducing the PVLDB paper" above).

### Workloads and metrics

| Workload | Setup | Recorded |
| --- | --- | --- |
| W1 | Two branches update different rows of the same file | False-abort flag. B1 aborts. SemLake, B3 and B4 commit. B2 rewrites the file, and B3 writes a version record for every tuple. |
| W1S | Each branch updates rows in its own half of the files | Request count and API cost. B2 rewrites every changed file. SemLake writes a delta manifest. |
| W2 | Contention rate from 0 to 1 on a shared row | Successful merges per CPU second, with no client retry loop. B1 falls to 0. SemLake keeps the conflict in a VSA node. B4 keeps committing and overwrites the conflict. |
| W2L | W2 on a 10K-file table in the full run | Merge CPU time at contention 0, 1, 5 and 20%. |
| W5 | 4 or 16 writers commit to main with compare-and-swap. One row per file | Commits per second, retries per commit, p99, and how many commits are applied or parked on a VSA choice. |
| M1S | 27 labelled predicate cases, including domain, foreign key, a check, and two TPC-H predicates taken from SQL | Precision and recall for SemLake, B1 to B4, and Dolt when `dolt` is installed. An unparsed `OR` or `NOT` is checked against the deleted rows. |
| H1 | A chain of merges, measured at 120 and 360 snapshots | LCA time and one 3-way merge. |
| W3 | Branch A runs `DELETE ... WHERE kind = 'vip'`; branch B writes an aggregate over `WHERE kind = 'vip' AND value >= 10` | Semantic recall. Each commit records only its WHERE clause. SemLake is the only system that returns `semantic_conflict`. |
| W3C | Same setup, except that A deletes `kind = 'spam'` | False-positive flag. Every system, SemLake included, must commit. |
| W4 | Column add plus one appended file, at 1K to 10K manifest files | Merge CPU time. A nullable column add leaves untouched files alone. |
| M4 | Q6-style Parquet scan, ordered by row id | Overhead for a W1-shaped deletion vector and for uniform deletes at 0.1%, 1% and 5%. Clean row groups are read as stored. A dirty group keeps its data file and is joined, by position, to a separate one-column mask. |

### Plots

- `benchmarks/plots/latency_vs_manifest_depth.pdf`
- `benchmarks/plots/s3_cost_comparison.pdf`
- `benchmarks/plots/tps_vs_contention.pdf`
- `benchmarks/plots/merge_time_vs_contention.pdf`
- `benchmarks/plots/latency_vs_history.pdf`
- `benchmarks/plots/tps_vs_writers.pdf`

PNG copies sit next to the PDFs. The figures are drawn from the CSV; `traces.json` holds the same rows with more detail.

### Findings

On W1, SemLake merges disjoint row updates in one file (`false_abort = 0`) and B1 aborts (`false_abort = 1`). B1 decides per file, so its row puts the overlapping files in `file_conflicts` and leaves `cell_conflicts` at 0.

On W3, SemLake returns `semantic_conflict` and B1 to B4 commit, so they score recall 0. The checker compares the WHERE clause of the delete with the WHERE clause of the aggregate and finds a value that both can match, without reading table data. M1S repeats that question on 27 labelled cases, two of them taken from a TPC-H refresh and Q6. SemLake's recall is 1.00 and its precision is 1.00. A clause with `OR` or `NOT` is still treated as a possible overlap, and the checker then looks at the deleted rows: if none of them satisfy the other clause, the merge commits. The case where `NOT kind = 'user'` deletes a vip row still conflicts with a read of `kind = 'vip'`. B1 to B4 score recall 0. The proposal gives B3 20% recall on a broader suite; this harness does not reproduce that figure. On W3C the two clauses cannot match the same row, and SemLake commits (`false_positive = 0`).

The same 27 cases were replayed in Dolt 2.4.0. Each case gets its own repository: the base rows are loaded, each branch is committed, and `dolt merge` runs. A foreign key is a real `FOREIGN KEY`, and a domain clause is a `CHECK`. Dolt's precision on this suite is 1.00 and its recall is 0.14 (2 of 14 real conflicts). It catches the foreign key, and it catches `CHECK (score >= 0)` when every base row already has `score = 0` and the other branch sets `-1`. It misses all 10 WHERE overlaps and the `rows_live` case, because those predicates are not SQL constraints. `CHECK (value >= 0)` is refused on the declaring branch: rows 0 and 1 already hold `-2` and `-1`, so the constraint is never in force and the merge commits the later assignment of `-5`. SemLake still flags that assignment, because its domain clause applies to the other branch's writes. Per-case outcomes are in `benchmarks/results/m1s_cases.csv`.

On W2 at full contention, B1 throughput is 0. SemLake still commits and keeps a VSA node, and B4 commits by last-writer-wins. Throughput there is successful merges divided by CPU time, with no client retry. W5 adds the retry, with one row in each file. Several writers branch from main and publish with a compare-and-swap on the branch head. SemLake turns a lost race into a 3-way merge and tries the swap again. A clean merge counts as applied. A merge that still holds a VSA choice counts as parked. B1 commits the file-level merge when the files differ, and reapplies the write from the new head when they do not. Throughput counts applied writes. A parked VSA choice is reported separately and is not part of the rate. With 16 writers and disjoint rows, SemLake applies about 1,100 commits per second (p99 10.8 ms) and B1 about 1,190 (p99 15.2 ms). Neither parks a write. With 16 writers on the same row, SemLake parks 90 of 96 commits and applies 6, about 90 applied commits per second (p99 6.9 ms). B1 applies all 96 by redoing the transaction, about 960 commits per second (p99 17.2 ms). Parking keeps the conflict open. It does not install the writer's value.

W2L records merge CPU time on a 10K-file table. At contention 0 and 1%, SemLake takes about 5.5 ms and B1 about 79 ms, which is more than 10x. At 5% the same run is 15 ms against 83 ms, and at 20% it is 17 ms against 83 ms, about 5x. The 10x figure is the disjoint end of that curve.

On W4, SemLake's merge CPU time stays flat as the manifest grows. At 10K files it is about 1.6 ms, against about 68 ms for B1 and 77 ms for B2. The estimated cloud latency in the CSV shows a much smaller gap, because both systems issue only a handful of requests and the fixed round trips outweigh the CPU difference. LCA time on a merge-heavy history stays at about 0.002 ms from 120 snapshots to 360. The walk keeps a generation number on each snapshot and stops once every open snapshot is older than the best candidate.

On W1S, SemLake's request cost is at most 5% of B2's (7 requests against 20,007 at 10K files), since B2 rewrites each changed file. W4 has no such gap: B2 also treats an added nullable column as a metadata change, as Iceberg and Delta do.

### Cost of a merged change

`benchmarks/cost_efficiency.py` writes `benchmarks/results/cost_efficiency.json`. Eight agents over 13 rounds, each keeping its own branch and editing a private row, make 104 merges and 0 version-space choices.

Prices are AWS S3 Standard request rates, $0.023 per GB-month of storage, and $0.10 per CPU-hour. B2 pays for one rewrite of a 128 MiB file. SemLake pays for the bytes its merge PUT, about 3 KB. The scan CPU is the 6 million row base scan, 0.129 s. The overhead fractions are the 1 million row run: 10.2% when the delete sits in one row group, and 75.3% when every row group is dirty. The timed file-level abort on one shared file is under a millisecond.

At 10 scans per merge, 1,000 localized merges cost about $0.072 with the deletion vector left in place, and about $0.21 for the copy-on-write rewrite. The open deletion vector reaches the rewrite cost at about 400 scans per merge when the delete sits in one row group, and at about 54 scans per merge when every row group is dirty. Compacting the 128 MiB file on every merge costs about $0.20 per 1,000 merges at 10 scans, close to copy-on-write. Waiting until the deletion vector covers 20% of the file brings that to about $0.07 per 1,000 merges.

A merge whose other side forked at the current head stays at 8 requests and about 2.7 KB as the main chain grows from 1 to 32 commits. A side that forked at the root reads every later summary: 9, 16 and 40 requests at those lengths.

### Limitations

M4 compares a compacted Parquet scan with a merge-on-read scan of the same rows. The committed rows use 1e6 synthetic rows in `row_id` order, which is smaller than TPC-H SF1 (6e6). The base files are not rewritten. A row group with an empty deletion vector is scanned as stored. A dirty group is joined, by position, to a separate one-column mask. On the W1 layout, 1% of the first row group is deleted and the other nine groups are clean. The five timing processes run one after another. The overhead there is 10.2%, and the 95% interval is 9.4% to 11.0%, above the 8% target. The same W1 layout on 6 million rows and 60 row groups, three processes run one after another, has a median overhead of -2.4% and a 95% interval of -9.5% to 6.3%. The upper end is under 8%, and the interval is wide enough that the sign is not stable. Uniform deletes land in every row group, so the positional join covers the whole table. At 1 million rows that overhead is 74.4%, 75.3% and 76.4% at 0.1%, 1% and 5%. At 6 million rows and 1% uniform deletes it is 67.2% (60.4% to 71.8%). DuckDB has no Python hook for an external selection vector, which is why the mask is a second file rather than a selection vector inside the scan.

The WHERE-clause check handles conjunctions of comparisons, `IN` and `BETWEEN` against literals. It counts `OR`, `NOT`, functions and joins as a possible overlap, then drops that overlap when every deleted row fails the other clause and that clause itself parses. The writer attaches the clause to the commit; nothing derives predicates from query plans. A domain clause (`value >= 0`) is checked against assignments on the other branch. A foreign key is a writer-declared list of parent row ids, checked the same way as `rows_live`. A merge result carries an agent outcome: `keep` for a clean merge, `replan` when a VSA choice is still open, and `block` for a semantic or schema conflict. Predicates and open choices are replayed from the non-merge commits, so a later merge still sees them.

Schema rename is not implemented and dropping a column is rejected. Type widening is supported, though W4 only adds a column. An unresolved VSA cell keeps the value from the base snapshot.

The object store is an in-memory stand-in priced at AWS S3 Standard rates. This tree does not run PyIceberg, Lance, MinIO or LST-Bench. A Delta log check runs when `deltalake` is installed: the second writer that started from the same table version is rejected. Dolt is used for the labelled merge cases when the `dolt` binary is on `PATH`. W4 files are small shared buffers, and there is no measured resident-memory figure.

`CORRECTNESS.md` states the purge-safe invariant for the parsed predicate fragment. A commit keeps a sparse row view over its parent, so unchanged rows are not copied. `benchmarks/swarm_service.py` charges 0.2 s of catalog time per compare-and-swap and does not sleep. Across 5 seeds that charge puts a single head near 5.0 applied commits per second, group commit of 8 merges per swap near 39, and one head per row near 40. The partition figure charges the longest head, because those heads move together. The hazard probe after a chain of merges stays at 0. Deletion vectors are positional roaring bitmaps, the role Iceberg v3 gives a Puffin deletion vector, and `row_pos` stands in for row lineage. This tree does not write Puffin files. SQL `WHERE` clauses are taken from `DELETE` and `SELECT` by `semlake/sql_predicates.py`. A 200-row TPC-H refresh against a Q6-shaped read conflicts. A curation pair, and a pair of path edits, merge when the edits are disjoint and stay open when they share a cell.

## Repository access

Keep this repository private. Before sharing the link, add the instructor as a collaborator in the host's access settings.
