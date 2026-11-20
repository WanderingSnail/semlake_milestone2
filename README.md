# SemLake: Zero-Copy Branching and Three-Way Semantic Merging for Data Lakehouses

## Overview

SemLake brings three-way semantic merge to branched tables in cloud data lakehouses such as Apache Iceberg and Delta Lake. It decouples metadata-time conflict verification over compact commit summaries from zero-copy physical reconciliation over positional Roaring deletion vectors (DVs).

When two diverged branches merge relative to their lowest common ancestor (LCA), SemLake:

1. **Replays non-merge commits since the fork (`ReplaySinceFork`)**: walks the commit DAG from the generation-pruned LCA, deduplicating internal merge diamonds (`StripPrefix`) and folding row operations, schema changes, attached predicates, and recorded pre-images from compact JSON commit summaries (`1 GET` per non-merge commit).
2. **Verifies cross-branch semantic compatibility (`VerifyMergeSemantics`)**: runs a two-tier check pairing a fast column-interval satisfiability solver (`WhereOverlap`) over a decidable conjunctive SQL fragment with an exact row-value evaluator (`AnyMatch` and `Unstable`) over recorded fork/delete pre-images and update/insert deltas, alongside `rows_live`, `foreign_key`, and `domain` invariant checks.
3. **Reconciles row and schema state without rewriting base Parquet files**: coalesces disjoint-column updates on shared rows, isolates true cell collisions into Version Space Algebra (VSA) choice nodes (`choice(r, c, [(A, v_A), (B, v_B)])`), unifies additive nullable columns and lossless numeric widenings (`int32 -> int64 -> double`), updates per-file Roaring deletion vectors by bitwise union (`|`), and writes a single delta Parquet file plus a delta manifest.

The repository combines Apache Parquet I/O (`pyarrow`), 32-bit Roaring bitmaps (`pyroaring`), SQL AST predicate extraction (`sqlglot`), single-threaded DuckDB scans at TPC-H SF 1 and SF 10, an instrumented in-memory object store (`MemoryObjectStore`), live Amazon S3 runs at 1K and 10K files (`S3ObjectStore`), a real `lakeFS` server, native `Dolt`, `delta-rs`, and `Spark 4.0 + Delta Lake 4.0` cross-checks, a 300-pair result oracle, and a discrete-event multi-agent swarm simulator calibrated with measured S3 latencies.

## Repository Layout

```text
semlake/                       core engine (~2,500 LoC)
  metadata.py                  commit DAG, branches, generation-pruned LCA, schema union, RowView
  storage.py                   MemoryObjectStore, Parquet I/O, Roaring deletion vectors
  merge.py                     DAG replay since fork, 3-way diff, zero-copy DV merge, VSA nodes
  verification.py              2-tier predicate & row-value checks (WhereOverlap, AnyMatch, Unstable)
  sql_predicates.py            SQL AST predicate and column extraction (SELECT / DELETE)
  reader.py                    snapshot materialization and Parquet + DV export for DuckDB scans
  s3store.py                   live Amazon S3 object store (boto3) with request/byte counters
  baselines.py                 B1 file OCC, B2 copy-on-write, B3 tuple versions, B4 CRDT op log
benchmarks/                    benchmark and simulation drivers (~6,000 LoC)
  profiler.py                  AWS S3 Standard request pricing and analytical latency model
  workloads.py                 W1-W4, W1S, W2L, H1, M1S 34-case suite, and synthetic scan drivers
  tpch_experiments.py          real TPC-H lineitem pairs (42 delete + 35 update/insert at SF 0.01 & 0.1),
                               selectivity curve, SF 1 and SF 10 DuckDB Q6 scans, and 40-merge compaction
  merge_sf01_parts.py          merges partitioned SF 0.1 TPC-H delete and update experiment outputs
  oracle_experiments.py        300-pair randomized refresh-vs.-report result oracle benchmark
  preimage_cost.py             commit summary pre-image size and merge verification time at SF 0.1
  scion_sim.py                 discrete-event swarm simulation (8-64 agents, 5 seeds, group commit)
  swarm_service.py             server-side catalog CAS merge service and group commit
  cost_efficiency.py           dollars per 1,000 merges and scan-to-merge crossover analysis
  s3_run.py                    live 1K and 10K-file Amazon S3 merge benchmark (W1S, W4, parallel B2)
  delta_baseline.py            real delta-rs table concurrency verification for B1
  delta_spark_baseline.py      real Spark 4.0 + Delta Lake 4.0 concurrency verification across 77 TPC-H pairs
  lakefs_baseline.py           real lakeFS 1.88 server branch/merge driver
  lakefs_runs.py               scores real lakeFS across all 111 scenario and TPC-H branch pairs
  dolt_baseline.py             native Dolt SQL merge runner for B5
  rule_baselines.py            B1R (Delta row-write + file-read check) and B6 (classical SSI)
  paper_experiments.py         file-packing ablation and publication figure generator
  paper_numbers.py             generates paper/generated/*.tex from benchmarks/results/
  plot.py                      standalone benchmark plots
  run_all.py                   one-command benchmark harness and hypothesis checks
  results/                     committed CSV and JSON experimental results
  plots/                       committed PDF and PNG benchmark plots
paper/
  semlake_pvldb.tex            PVLDB paper source (acmart sigconf,nonacm + pvldb)
  semlake_pvldb.pdf            compiled PVLDB paper
  semlake_paper_zh.md          complete Chinese translation synchronized with semlake_pvldb.tex
  references_pvldb.bib         bibliography (49 entries)
  figures/                     publication figures (PDF and PNG)
  generated/                   auto-generated LaTeX macros (numbers.tex) and tables (tab_*.tex)
tests/
  test_core.py                 unit tests for 3-way merge, predicates, VSA, and baselines
  test_phase_bc.py             tests for SQL extraction, swarm service, Delta check, and TPC-H
  test_edge_cases.py           edge-case tests for clauses outside F, pre-images, and read stability
  test_fuzz.py                 200-seed randomized DAG merge soundness fuzzer against row oracle
```

## Quick Start and Tests

With Python 3.11 or later:

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
python benchmarks/run_all.py --quick
```

`--quick` runs the smoke benchmark matrix and verifies the core hypotheses. For the full in-memory matrix (1K, 5K, and 10K files across the contention and history curves):

```bash
python benchmarks/run_all.py
```

## Reproducing the PVLDB Paper

Every number and table in `paper/semlake_pvldb.tex` is generated from `benchmarks/results/` by `benchmarks/paper_numbers.py`. To regenerate the LaTeX macros and tables from the committed results:

```bash
python benchmarks/paper_numbers.py
```

To rerun all experiments from scratch (run sequentially so timings do not contend for CPU):

```bash
pip install -r requirements.txt                    # includes tpchgen-cli for TPC-H data
python benchmarks/scion_sim.py                     # swarm simulation: 8-64 agents, 5 seeds (calibrated S3)
python benchmarks/tpch_experiments.py pairs        # 42 TPC-H delete-refresh vs. report pairs (SF 0.01 & 0.1)
python benchmarks/tpch_experiments.py updates      # 35 TPC-H update/insert vs. report pairs (SF 0.01 & 0.1)
python benchmarks/tpch_experiments.py selectivity  # false blocks vs. delete selectivity
python benchmarks/oracle_experiments.py            # 300 random refresh-vs.-report pairs against result oracle
python benchmarks/preimage_cost.py                 # pre-image summary size and merge cost at SF 0.1
python benchmarks/lakefs_runs.py                   # real lakeFS 1.88 server across 111 pairs (requires lakeFS)
python benchmarks/delta_spark_baseline.py          # real Spark 4.0 + Delta Lake 4.0 across 77 TPC-H pairs
python benchmarks/run_all.py                       # W1-W4, H1, 34-case suite (m1s_cases.csv)
python benchmarks/tpch_experiments.py scan         # DuckDB Q6 scan overhead at SF 1
python benchmarks/tpch_experiments.py scan10       # DuckDB Q6 scan overhead at SF 10
python benchmarks/tpch_experiments.py compaction   # 40-merge Q6 compaction threshold trace
python benchmarks/cost_efficiency.py               # S3 + CPU cost per 1,000 merges
python benchmarks/s3_run.py --bucket <bucket> --region eu-north-1  # optional live 1K & 10K S3 run
python benchmarks/paper_experiments.py all         # packing ablation and paper/figures/*.pdf
python benchmarks/paper_numbers.py                 # writes paper/generated/*.tex
```

### Mapping Between Code and Paper

| Code Artifact | Paper Section / Metric |
| --- | --- |
| `W1`, `W1S`, `packing.json` | $W_1$ (shared-file disjoint updates); `W1S` is the 10K-file S3 request benchmark |
| `W2`, `W2L` | $W_2$ (hotspot key contention $\rho \in [0\%, 20\%]$ on 10K files) |
| `W3`, `W3C`, `m1s_cases.csv`, `tpch_pairs.json`, `tpch_update_pairs.json`, `tpch_pairs_sf0.1.json`, `tpch_update_pairs_sf0.1.json`, `oracle_pairs.json`, `selectivity.json` | $W_3$ and $M_1$ (34 labelled scenarios, 42 delete-refresh pairs, 35 update/insert pairs at SF 0.01 & SF 0.1, 300-pair result oracle, selectivity curve) |
| `W4`, `H1`, `preimage_cost.json` | $W_4$ and $M_2$ (schema evolution, 1K-10K file manifests, 42-360 snapshot DAG depth, pre-image summary size and merge cost) |
| `cost_efficiency.json`, `s3_run.json`, `s3_run_v2.json` | $M_3$ (S3 request counts, 1K & 10K-file live `eu-north-1` S3 runs with parallel $B_2$ and calibrated latency model, cost crossover $N_{\text{scans}}^*$) |
| `tpch_scan.json`, `tpch_scan_sf10.json`, `compaction.json` | $M_4$ (DuckDB TPC-H Q6 scan overhead at SF 1 and SF 10, 40-merge compaction trace) |
| `scion_sim_calibrated_summary.json`, `scion_sim_summary.json` | $W_{\text{swarm}}$ (Section 7.7, 8-64 concurrent agents with group commit $G=8$, priced with measured S3 latencies) |
| `b1`, `lakefs`, `b1r`, `delta_spark`, `b2`, `b3`, `b4`, `dolt`, `b6` | Baselines $B_1$, real lakeFS 1.88, $B_1\text{R}$, real Delta Lake 4.0, $B_2$, $B_3$, $B_4$, $B_5$ (Dolt 2.5.0), $B_6$ (SSI) |
| `semlake_static`, `semlake_deletes_only` | Ablations: static test only (no `AnyMatch`) and deletes only (no `Unstable`) |

## Summary of Key Empirical Findings

1. **Semantic Merge Correctness ($M_1$)**:
   - Across the **34 labelled scenarios** (18 conflicts) and **42 TPC-H delete-refresh vs. query pairs** (22 conflicts on SF 0.01 `lineitem`, 60,175 rows in 31 files), SemLake achieves **1.00 recall** and **1.00 precision** with **0 false aborts** on shared files ($W_1$). Disabling the Tier-2 deleted-row value check (`AnyMatch`) keeps 1.00 recall but drops precision to **0.90** on the 34-case suite and **0.73** on the 42 TPC-H pairs.
   - Across the **35 TPC-H update/insert-refresh vs. query pairs** (10 conflicts), SemLake achieves **1.00 recall** and **1.00 precision**; disabling `Unstable` (Line 14 of Algorithm 2) drops recall on update/insert conflicts to **0.00**. At **SF 0.1** (600,572 rows, all **77 TPC-H pairs**, 33 conflicts), SemLake again achieves **1.00 recall** and **1.00 precision** ($B_1\text{R}$: **0.79 recall**, **0.60 precision**).
   - Under the **Result Oracle** (**300 random refresh-vs.-report pairs**, 105 conflicts that change the report's query output), SemLake misses zero conflicts (**1.00 recall**) at **0.74 precision**: all 37 false blocks are `max` reports where a non-maximum row changed (**1.00 precision** excluding `max` reports), vs. **0.94 recall / 0.47 precision** for $B_1\text{R}$ and **0.53 precision** for static-only SemLake.
   - File-level OCC ($B_1$), a **real lakeFS 1.88 server** (matching $B_1$ on all **111/111** pairs), CoW ($B_2$), tuple versioning ($B_3$), CRDT ($B_4$), and classical SSI ($B_6$) score **0.00 recall** on cross-branch read-write hazards. Delta-style file-read tracking ($B_1\text{R}$) scores precision **0.64** on the suite, **0.88** on delete pairs, **0.24** on update/insert pairs, and **1.00 false-block rate** at $\ge 0.1\%$ delete selectivity; **real Delta Lake 4.0 (Spark 4.0)** blocks 70 of the 77 TPC-H pairs (recall **1.00**, precision **0.63** on deletes and **0.29** on updates). Dolt ($B_5$) catches declared `FOREIGN KEY` and `CHECK` violations (recall **0.11** on the suite, **0.00** on TPC-H queries).

2. **Merge CPU Latency and Metadata Scalability ($M_2$)**:
   - On a 10,000-file table ($W_2$), SemLake merges in **9.02 ms** at $\rho = 0\%$ and **8.12 ms** at $\rho = 20\%$ (vs. **227.3 ms** and **203.2 ms** for $B_1$).
   - On $W_4$, as manifest depth scales from 1,000 to 10,000 files, SemLake merge CPU is **0.38 ms** to **3.22 ms** (40-53$\times$ faster than $B_1$, $B_2$, and $B_3$ at 10K files).
   - Across commit DAG depths from **42 to 360 snapshots** (`H1`), generation-pruned LCA takes **3.5 to 6.6 $\mu$s** and full merge CPU rises from **0.53 ms to 1.20 ms**.
   - Recording delete pre-images in commit summaries costs ~**202 bytes/row**: deleting 10% of SF 0.1 `lineitem` (59,790 rows) produces a **15.6 MB** summary (**12.1 MB** of pre-images, or **9.2 MB** when projected to filter columns), and merging it against a report takes **1.7 s** (vs. **0.40 s** at 0.1% delete) without reading any base Parquet file.

3. **Cloud Object-Store Cost and Live S3 Validation ($M_3$)**:
   - On a 10,000-file shared-file update ($W_1$), SemLake issues **7 S3 requests** (3 `PUT`, 3 `GET`, 1 `LIST`, 380 bytes written) vs. **20,007 requests** for Copy-on-Write ($B_2$), reducing S3 API request cost by **99.96%**.
   - On a live Amazon S3 bucket in `eu-north-1` at **1,000 and 10,000 files** (3 timed runs per point), request counts match the in-memory store identically (7 vs. 2,007 at 1K; 7 vs. 20,007 at 10K): SemLake merges $W_1$ in **1.68 s** (1K) and **2.15 s** (10K) and $W_4$ in **1.52 s** (1K and 10K), whereas 32-way parallel $B_2$ takes **16.2 s** at 1K (**10$\times$ slower**) and **148.6 s** at 10K (**69$\times$ slower**), and sequential $B_2$ takes **469 s** at 1K (**268$\times$ slower** in the 5-run v1 series: 1.76 s vs. 472 s). Recalibrated with measured S3 medians (PUT 297 ms, GET 154 ms, LIST 147 ms), the analytical model predicts every sequential S3 run within **1.01-1.40$\times$**.
   - Leaving DVs uncompacted remains cheaper than eager 128 MiB CoW file rewrites up to **3,094 full-table scans per merge** for localized deletes and **446 scans per merge** for uniform deletes.

4. **Post-Merge Analytical Scan Overhead ($M_4$)**:
   - Running single-threaded DuckDB TPC-H Q6 over SF 1 `lineitem` (6,001,215 rows across 61 row groups of 100,000 rows), localized 1% deletes incur **+0.9%** median scan overhead (95% CI: $-3.4\%$ to $+6.9\%$), and uniform 1% deletes incur **+6.0%**.
   - At SF 10 (59,986,052 rows across 600 row groups, base scan 1,714 ms), localized 1% deletes incur **$-1.4\%$** overhead ($-5.6\%$ to $+2.7\%$) and uniform 1% deletes incur **+5.6%** ($+2.9\%$ to $+9.0\%$).
   - Over 40 consecutive merges deleting 0.1% of rows each, a **2%** row-group compaction threshold reduces localized mean scan overhead from **5.0% to 0.0%** while rewriting 89,549 rows per merge.

5. **Concurrent Agent Swarms ($W_{\text{swarm}}$)**:
   - Across 5 seeds of 60 s discrete-event simulations (8 to 64 agents, 10 ms catalog RTT) priced with measured S3 request latencies, single-CAS SemLake levels off near **1.60 commits/s** at 64 agents (450 S3 requests/commit), whereas **SemLake with group commit ($G=8$)** reaches **12.14 commits/s** (63 S3 requests/commit) with **0 committed semantic hazards** and **0 false file-conflict redos** (across standard and purge-heavy mixes, $B_1$ commits 15 hazards with 413.2 redos/run at 64 agents, $B_2$ commits 8 hazards, and $B_1\text{R}$ commits 11 hazards with 13.0 and 22.8 false blocks/run at 32 and 64 agents).
