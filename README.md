# SemLake: Zero-Copy Branching and Three-Way Semantic Merging for Data Lakehouses

## Overview

SemLake is a storage and catalog layer for three-way merging of branched lakehouse tables with a stated
correctness contract, *purge-safety*: no row a branch read may have been deleted, moved across the read's
filter, or changed in a column the read uses by the other branch. This repository holds the Python
prototype, every benchmark, the raw results and the PVLDB-format paper (Milestone 4 revision).

When two branches merge relative to their fork, SemLake:

1. **Replays the commits since the fork** (`ReplaySinceFork`): one `GET` per non-merge commit, folding row
   operations, pre-images, schema changes and predicates from JSON commit summaries.
2. **Checks purge-safety from summaries alone** (`VerifyMergeSemantics`): a static test on a conjunctive
   fragment and, when that cannot decide, an evaluation on the recorded row values. Joins and subqueries
   are projected soundly onto the merged table, one clause per query block. No data file is read.
3. **Reconciles without rewriting base files**: disjoint-column updates converge, same-cell collisions
   become choice nodes that a protected branch refuses until resolved, a purge wins over a racing update,
   schemas unify by lossless widening, deletion vectors are unioned, and the merge publishes one delta
   file, one summary and a delta manifest with **one catalog compare-and-swap**.
4. **Recovers by doing nothing**: every object key contains its snapshot id and the CAS is the commit
   point, so a crash leaves only unreachable objects (invariants I1 to I5, tested by fault injection).

See `CORRECTNESS.md` for the contract, the proofs' assumptions and the test that covers each claim,
`ARTIFACT.md` for the reproduction package, and `CHANGELOG.md` for what the revision changed.

## Reproduce the paper

```bash
./reproduce.sh              # every number, table, figure and the PDF from the committed raw results
./reproduce.sh check        # + unit tests and a decision check of the labelled suite
./reproduce.sh quick        # smoke-run every local experiment at a small size
./reproduce.sh rerun        # rerun every experiment that needs no cloud account (hours)
docker build -t semlake . && docker run --rm -v "$PWD/out:/out" semlake
```

`ARTIFACT.md` maps each figure and table to its raw result file and the command that produces it.

## Repository Layout

```text
semlake/                       core engine
  metadata.py                  commit DAG, branches, LCA, schemas, delta manifests, protected branches
  storage.py                   MemoryObjectStore with per-class request and byte counters, Parquet I/O, DVs
  merge.py                     replay since the fork, three-way reconcile, purge-wins, choice nodes, DV merge
  verification.py              two-tier check (static fragment, row values), update check, pruned pre-images
  sql_predicates.py            read and delete predicates from SQL; projection of joins and subqueries
  maintenance.py               choice resolution, pre-image pruning, fault-injecting store, garbage collection, I1 to I5 check
  reader.py                    snapshot materialization from published objects alone; DuckDB export
  s3store.py                   live Amazon S3 object store (boto3)
  baselines.py                 B1 file OCC, B2 copy-on-write, B3 tuple versions, B4 CRDT
benchmarks/
  revision_experiments.py      ledger, tail latency, delta sweep, threads, fault injection, pruning
  tpch_full_queries.py         17 full TPC-H queries as declared reads, labelled by query results
  tpch_experiments.py          TPC-H filter pairs, selectivity, SF 1 and SF 10 scans, compaction
  oracle_experiments.py        300 random refresh-versus-report pairs labelled by query results
  scion_sim.py, swarm_service.py   swarm simulation; every system with and without the commit service
  rule_baselines.py            B1R (row-level writes, file-level read check) and B6 (SSI)
  s3_run.py, lakefs_runs.py, delta_spark_baseline.py, dolt_baseline.py   real-system profiles
  verify_committed.py          recomputes the committed decisions with the current code
  paper_numbers.py, revision_numbers.py   paper/generated/*.tex from benchmarks/results/
  paper_experiments.py, revision_figures.py   paper/figures/*.pdf from benchmarks/results/
  check_pdf.py                 page budget, embedded fonts, no raster images
  results/                     raw results (JSON and CSV), each with the environment that produced it
paper/
  semlake_pvldb.tex            PVLDB source; generated/ and figures/ are produced by the scripts above
  semlake_pvldb.pdf            compiled paper (12 pages of body plus references)
  semlake_paper_zh.md          Chinese translation synchronized with the Milestone 4 revision
tests/
  test_core.py, test_phase_bc.py, test_edge_cases.py   merge, predicates, baselines, SQL extraction
  test_revision.py             tests added for the revision (see CORRECTNESS.md, section 8)
  test_fuzz.py                 200-seed randomized merges against a brute-force oracle
reproduce.sh, Dockerfile, requirements.lock, ARTIFACT.md   the reproduction package
```

## Tests

```bash
pip install -r requirements.lock
python3 -m pytest -q tests
```

## Main results of the revision

All numbers are generated from `benchmarks/results/` by `benchmarks/paper_numbers.py`; the paper
(`paper/semlake_pvldb.pdf`) gives the method for each.

1. **Correctness under a result oracle (M1).** 17 full TPC-H queries plus `count(*)` and
   `SELECT *`, run as declared reads against 15 refreshes (285 pairs, 51 conflicts by
   query result): SemLake recall 1.00, precision 0.41, 31% of safe pairs blocked.
   A Delta-style file-level read check (B1R): recall 1.00, precision 0.24, 69%
   blocked. Row-level merges without a read check detect no conflict. On 300 random single-table
   pairs: recall 1.00, precision 0.74 (all 37 false blocks are `max` reports).
   Labels that follow the definition (34 scenarios, 77 TPC-H pairs at SF 0.01 and SF 0.1) give 1.00/1.00 and
   are reported as a consistency check only.
2. **Crash recovery.** 2,400 injected crashes before every PUT of commits, merges and group commits:
   0 violations of invariants I1 to I5.
3. **Merge latency (M2).** On a 10,000-file table, 60 merges per point: P50 3.55 ms and
   P99 5.19 ms without contention, P50 3.64 ms and P99 9.07 ms at 50%
   contention; B1R is within 6%; file-level OCC takes 106 ms. A purge of
   1,000,000 rows merges in 16.4 s with 4.9 GB of memory (linear in the rows changed).
4. **Storage ledger (M3).** A merge that publishes 10,000 changed deletion vectors issues 8 requests
   and writes 3.92 MB, the same as B1R; copy-on-write issues 20,015 requests (153 s with 32
   parallel requests against 2.06 s). The request count is a property of row-level commits with deletion
   vectors, not of the semantic check. Live S3 at 10K files: 3.42 s for SemLake,
   3.11 s for B1R, 156 s for parallel copy-on-write.
5. **Scan overhead (M4).** TPC-H Q6 on SF 1 with 1% of rows deleted and left in a deletion vector:
   +2.0% (+1.3% to +2.9%) when the deletes are localized, +15.5%
   (+13.1% to +16.6%) when they are uniform (30 paired processes, bootstrap interval).
6. **Scale-out.** Real threads: optimistic publishing peaks at 1.7 commits/s with
   8 writers and falls to 0.5 at 64; the commit service reaches
   6.1 commits/s at 32 writers, and 5.5 for file-level OCC with the
   same service. No published update is lost. Simulated swarm, 64 agents, 5 seeds: SemLake commits
   0 hazards (B1 143, B1R 67, B2 234), and
   throughput with the commit service is 12.1 commits/s against 13.2 for
   B1R: the throughput comes from batching, the difference is correctness.

Limits stated in the paper: soundness covers declared reads (undeclared reads fail closed); precision
on joins is 0.30 because the projection keeps single-table conditions only; the prototype keeps
branch deltas in memory; B2 and B6 are rule models; the 64-agent results are simulated.
