# SemLake: Zero-Copy Branching and Three-Way Semantic Merging for Data Lakehouses

## Overview

SemLake brings three-way semantic merge to branched tables in cloud data lakehouses such as Apache Iceberg and Delta Lake. It decouples metadata-time conflict verification over compact commit summaries from zero-copy physical reconciliation over positional Roaring deletion vectors (DVs).

When two diverged branches merge relative to their lowest common ancestor (LCA), SemLake:

1. **Replays non-merge commits since the fork (`ReplaySinceFork`)**: walks the commit DAG from the generation-pruned LCA, deduplicating internal merge diamonds (`StripPrefix`) and folding row operations, schema changes, attached predicates, and recorded pre-images from compact JSON commit summaries (`1 GET` per non-merge commit).
2. **Verifies cross-branch semantic compatibility (`VerifyMergeSemantics`)**: runs a two-tier check pairing a fast column-interval satisfiability solver (`WhereOverlap`) over a decidable conjunctive SQL fragment with an exact row-value evaluator (`AnyMatch` and `Unstable`) over recorded fork/delete pre-images and update/insert deltas, alongside `rows_live`, `foreign_key`, and `domain` invariant checks.
3. **Reconciles row and schema state without rewriting base Parquet files**: coalesces disjoint-column updates on shared rows, isolates true cell collisions into Version Space Algebra (VSA) choice nodes (`choice(r, c, [(A, v_A), (B, v_B)])`), unifies additive nullable columns and lossless numeric widenings (`int32 -> int64 -> double`), updates per-file Roaring deletion vectors by bitwise union (`|`), and writes a single delta Parquet file plus a delta manifest.

To evaluate both algorithmic semantics and cloud storage behavior under single-machine academic constraints, the repository combines physical Apache Parquet I/O (`pyarrow`), 32-bit Roaring bitmaps (`pyroaring`), SQL AST predicate extraction (`sqlglot`), single-threaded DuckDB analytical scans at TPC-H SF 1 and SF 10, an instrumented in-memory object store (`MemoryObjectStore`), a live Amazon S3 backend (`S3ObjectStore`), native `Dolt` and `delta-rs` cross-checks, and a discrete-event multi-agent swarm simulator.

## Repository Layout

```text
semlake/                   core engine (~2,400 LoC)
  metadata.py              commit DAG, branches, generation-pruned LCA, schema union, RowView
  storage.py               MemoryObjectStore, Parquet I/O, Roaring deletion vectors
  merge.py                 DAG replay since fork, 3-way diff, zero-copy DV merge, VSA nodes
  verification.py          2-tier predicate & row-value checks (WhereOverlap, AnyMatch, Unstable)
  sql_predicates.py        SQL AST predicate and column extraction (SELECT / DELETE)
  reader.py                snapshot materialization and Parquet + DV export for DuckDB scans
  s3store.py               live Amazon S3 object store (boto3) with request/byte counters
  baselines.py             B1 file OCC, B2 copy-on-write, B3 tuple versions, B4 CRDT op log
benchmarks/                benchmark and simulation drivers (~5,100 LoC)
  profiler.py              AWS S3 Standard request pricing and analytical latency model
  workloads.py             W1-W4, W1S, W2L, H1, M1S 34-case suite, and synthetic scan drivers
  tpch_experiments.py      real TPC-H lineitem pairs (42 delete + 35 update/insert), selectivity,
                           SF 1 and SF 10 DuckDB Q6 scans, and 40-merge compaction traces
  scion_sim.py             discrete-event swarm simulation (8-64 agents, 5 seeds, group commit)
  swarm_service.py         server-side catalog CAS merge service and group commit
  cost_efficiency.py       dollars per 1,000 merges and scan-to-merge crossover analysis
  s3_run.py                live 1,000-file Amazon S3 merge benchmark (W1S, W4)
  delta_baseline.py        real delta-rs table concurrency verification for B1
  dolt_baseline.py         native Dolt SQL merge runner for B5
  rule_baselines.py        B1R (Delta row-write + file-read check) and B6 (classical SSI)
  paper_experiments.py     file-packing ablation and publication figure generator
  paper_numbers.py         generates paper/generated/*.tex from benchmarks/results/
  plot.py                  standalone benchmark plots
  run_all.py               one-command benchmark harness and hypothesis checks
  results/                 committed CSV and JSON experimental results
  plots/                   committed PDF and PNG benchmark plots
paper/
  semlake_pvldb.tex        PVLDB paper source (acmart sigconf,nonacm + pvldb)
  semlake_pvldb.pdf        compiled 13-page PVLDB paper
  semlake_paper_zh.md      complete Chinese translation synchronized with semlake_pvldb.tex
  references_pvldb.bib     bibliography
  figures/                 publication figures (PDF and PNG)
  generated/               auto-generated LaTeX macros (numbers.tex) and tables (tab_*.tex)
tests/
  test_core.py             unit tests for 3-way merge, predicates, VSA, and baselines
  test_phase_bc.py         tests for SQL extraction, swarm service, Delta check, and TPC-H
  test_fuzz.py             200-seed randomized DAG merge soundness fuzzer against row oracle
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
python benchmarks/scion_sim.py                     # swarm simulation: 8-64 agents, 5 seeds
python benchmarks/tpch_experiments.py pairs        # 42 TPC-H delete-refresh vs. report pairs
python benchmarks/tpch_experiments.py updates      # 35 TPC-H update/insert vs. report pairs
python benchmarks/tpch_experiments.py selectivity  # false blocks vs. delete selectivity
python benchmarks/run_all.py                       # W1-W4, H1, 34-case suite (m1s_cases.csv)
python benchmarks/tpch_experiments.py scan         # DuckDB Q6 scan overhead at SF 1
python benchmarks/tpch_experiments.py scan10       # DuckDB Q6 scan overhead at SF 10
python benchmarks/tpch_experiments.py compaction   # 40-merge Q6 compaction threshold trace
python benchmarks/cost_efficiency.py               # S3 + CPU cost per 1,000 merges
python benchmarks/s3_run.py --bucket <bucket> --region eu-north-1  # optional live S3 run
python benchmarks/paper_experiments.py all         # packing ablation and paper/figures/*.pdf
python benchmarks/paper_numbers.py                 # writes paper/generated/*.tex
```

### Mapping Between Code and Paper

| Code Artifact | Paper Section / Metric |
| --- | --- |
| `W1`, `W1S`, `packing.json` | $W_1$ (shared-file disjoint updates); `W1S` is the 10K-file S3 request benchmark |
| `W2`, `W2L` | $W_2$ (hotspot key contention $\rho \in [0\%, 20\%]$ on 10K files) |
| `W3`, `W3C`, `m1s_cases.csv`, `tpch_pairs.json`, `tpch_update_pairs.json`, `selectivity.json` | $W_3$ and $M_1$ (34 labelled scenarios, 42 delete-refresh pairs, 35 update/insert pairs, selectivity curve) |
| `W4`, `H1` | $W_4$ and $M_2$ (schema evolution, 1K-10K file manifests, 42-360 snapshot DAG depth) |
| `cost_efficiency.json`, `s3_run.json` | $M_3$ (S3 request counts, 1,000-file live `eu-north-1` S3 run, cost crossover $N_{\text{scans}}^*$) |
| `tpch_scan.json`, `tpch_scan_sf10.json`, `compaction.json` | $M_4$ (DuckDB TPC-H Q6 scan overhead at SF 1 and SF 10, 40-merge compaction trace) |
| `scion_sim_summary.json` | $W_{\text{swarm}}$ (Section 7.7, 8-64 concurrent agents with group commit $G=8$) |
| `b1`, `b1r`, `b2`, `b3`, `b4`, `dolt`, `b6` | Baselines $B_1$, $B_1\text{R}$, $B_2$, $B_3$, $B_4$, $B_5$ (Dolt 2.5.0), $B_6$ (SSI) |
| `semlake_static`, `semlake_deletes_only` | Ablations: static test only (no `AnyMatch`) and deletes only (no `Unstable`) |

## Summary of Key Empirical Findings

1. **Semantic Merge Correctness ($M_1$)**:
   - Across the **34 labelled scenarios** (18 conflicts) and **42 TPC-H delete-refresh vs. query pairs** (22 conflicts on SF 0.01 `lineitem`, 60,175 rows in 31 files), SemLake achieves **1.00 recall** and **1.00 precision** with **0 false aborts** on shared files ($W_1$).
   - Disabling the Tier-2 deleted-row value check (`AnyMatch`) keeps 1.00 recall but drops precision to **0.90** on the 34-case suite and **0.73** on the 42 TPC-H pairs.
   - Across the **35 TPC-H update/insert-refresh vs. query pairs** (10 conflicts), SemLake achieves **1.00 recall** and **1.00 precision**; disabling `Unstable` (Line 14 of Algorithm 2) drops recall on update/insert conflicts to **0.00**.
   - File-level OCC ($B_1$), CoW ($B_2$), tuple versioning ($B_3$), CRDT ($B_4$), and classical SSI ($B_6$) score **0.00 recall** on cross-branch read-write hazards, while Delta-style file-read tracking ($B_1\text{R}$) blocks safe merges whenever a purge touches any file scanned by a report (precision **0.64** on the suite, **0.88** on delete pairs, **0.24** on update/insert pairs, and **1.00 false-block rate** at $\ge 0.1\%$ delete selectivity). Dolt ($B_5$) catches declared `FOREIGN KEY` and `CHECK` violations (recall **0.11** on the suite, **0.00** on TPC-H queries).

2. **Merge CPU Latency and Metadata Scalability ($M_2$)**:
   - On a 10,000-file table ($W_2$), SemLake merges in **9.02 ms** at $\rho = 0\%$ and **8.12 ms** at $\rho = 20\%$ (vs. **227.3 ms** and **203.2 ms** for $B_1$).
   - On $W_4$, as manifest depth scales from 1,000 to 10,000 files, SemLake merge CPU is **0.38 ms** to **3.22 ms** (40-53$\times$ faster than $B_1$, $B_2$, and $B_3$ at 10K files).
   - Across commit DAG depths from **42 to 360 snapshots** (`H1`), generation-pruned LCA takes **3.5 to 6.6 $\mu$s** and full merge CPU rises from **0.53 ms to 1.20 ms** due to in-memory store cloning and uncompacted delta-manifest accumulation.

3. **Cloud Object-Store Cost and Live S3 Validation ($M_3$)**:
   - On a 10,000-file shared-file update ($W_1$), SemLake issues **7 S3 requests** (3 `PUT`, 3 `GET`, 1 `LIST`, 380 bytes written) vs. **20,007 requests** for Copy-on-Write ($B_2$), reducing S3 API request cost by **99.96%**.
   - On a live 1,000-file Amazon S3 table in `eu-north-1` (5 timed runs), request counts match the in-memory store identically (7 vs. 2,007): SemLake merges $W_1$ in **1.76 s** and $W_4$ in **1.50 s**, whereas sequential $B_2$ takes **472 s** (**268$\times$ slower**). Because WAN HTTPS requests averaged 235 ms from our client, real S3 wall-clock times are 3.4-4.5$\times$ the analytical latency model (`0.44 s`), showing that remote merge wall-clock time is governed by network round-trips rather than CPU.
   - Leaving DVs uncompacted remains cheaper than eager 128 MiB CoW file rewrites up to **3,094 full-table scans per merge** for localized deletes and **446 scans per merge** for uniform deletes.

4. **Post-Merge Analytical Scan Overhead ($M_4$)**:
   - Running single-threaded DuckDB TPC-H Q6 over SF 1 `lineitem` (6,001,215 rows across 61 row groups of 100,000 rows), localized 1% deletes incur **+0.9%** median scan overhead (95% CI: $-3.4\%$ to $+6.9\%$), and uniform 1% deletes incur **+6.0%**.
   - At SF 10 (59,986,052 rows across 600 row groups, base scan 1,714 ms), localized 1% deletes incur **$-1.4\%$** overhead ($-5.6\%$ to $+2.7\%$) and uniform 1% deletes incur **+5.6%** ($+2.9\%$ to $+9.0\%$).
   - Over 40 consecutive merges deleting 0.1% of rows each, a **2%** row-group compaction threshold reduces localized mean scan overhead from **5.0% to 0.0%** while rewriting 89,549 rows per merge.

5. **Concurrent Agent Swarms ($W_{\text{swarm}}$)**:
   - Across 5 seeds of 60 s discrete-event simulations (8 to 64 agents, 10 ms catalog RTT), single-CAS SemLake saturates near **4.81 commits/s** at 64 agents due to per-merge object-store round-trips, whereas **SemLake with group commit ($G=8$)** reaches **17.12 commits/s** with **0 committed semantic hazards** and **0 false file-conflict redos** (compared to 63 hazards and 895.6 redos/run for $B_1$, and 27 hazards and 49.6 false blocks/run for $B_1\text{R}$).
