# SemLake artifact

Everything the paper reports is rebuilt from the raw result files in `benchmarks/results/`
by one command. Nothing in `paper/generated/` or `paper/figures/` is edited by hand.

## One command

```bash
./reproduce.sh              # numbers, tables and vector figures from the committed raw results
./reproduce.sh check        # the above, plus the unit tests and a decision check of the labelled suite
./reproduce.sh quick        # smoke-run every local experiment at a small size, then build tables/figures
./reproduce.sh rerun        # rerun every experiment that needs no cloud account, then build tables/figures
./reproduce.sh docker       # build the Docker image and run the default mode inside it (output in ./out)
```

or, with Docker only:

```bash
docker build -t semlake .
docker run --rm -v "$PWD/out:/out" semlake            # paper | check | quick | rerun
```

The output is `out/figures/` (vector PDF and a PNG preview of each figure) and `out/generated/`
(`numbers.tex` and the generated tables).

| Mode | What runs | Time on 4 cores |
| --- | --- | --- |
| `paper` | `paper_numbers.py`, `paper_experiments.py figures` | under 1 minute |
| `check` | `pytest tests`, `verify_committed.py --quick`, then `paper` | about 3 minutes |
| `quick` | every local experiment with `--quick` in a scratch copy, then `paper` | about 10 minutes |
| `rerun` | `check`, every local experiment at full size (overwrites `benchmarks/results/`), then `paper` | 3 to 4 hours |

## From raw data to each figure and table

Raw results are JSON or CSV in `benchmarks/results/`. Every file written by the revision
experiments starts with an `environment` block (CPU model, cores, memory, OS, Python,
DuckDB and pyarrow versions).

| In the paper | Raw result file | Produced by | Drawn or typeset by |
| --- | --- | --- | --- |
| Table "Full TPC-H queries as declared reads" (Section 7.2) | `tpch_full_queries.json` | `tpch_full_queries.py` | `revision_numbers.full_queries` |
| Randomized oracle paragraph (7.2) | `oracle_pairs.json` | `oracle_experiments.py` | `paper_numbers.oracle` |
| Table "Conflict detection on labelled scenarios and TPC-H pairs" (7.2) | `m1s_cases.csv`, `tpch_pairs.json`, `tpch_update_pairs.json`, `tpch_pairs_sf0.1.json`, `lakefs.json`, `delta_spark_pairs.json` | `run_all.py`, `tpch_experiments.py pairs` / `updates`, `lakefs_runs.py`, `delta_spark_baseline.py` | `paper_numbers.correctness`, `updates`, `scale_up` |
| Figure "False-block rate as a purge grows" (7.2) | `selectivity.json` | `tpch_experiments.py selectivity` | `paper_experiments.fig_selectivity` |
| Crash-recovery paragraph (7.2) | `recovery.json` | `revision_experiments.py recovery` | `revision_numbers.recovery` |
| Figure "Merge CPU time, P50 with a band to P99" (7.3) | `tails.json` | `revision_experiments.py tails` | `revision_figures.fig_scalability` |
| Figure "Delta-size sweep" (7.3) | `delta_sweep.json`, `prune.json` | `revision_experiments.py sweep`, `prune` | `revision_figures.fig_sweep` |
| Table "Ledger of one W1 merge" and figure "W1 merge ... requests and end-to-end time" (7.4) | `ledger.json` | `revision_experiments.py ledger` | `revision_numbers.ledger`, `revision_figures.fig_ledger` |
| Live S3 paragraph (7.4) | `s3_run_v3.json` | `s3_run.py` (optional profile) | `revision_numbers.s3_v3` |
| Table "TPC-H Q6 on SF 1 lineitem with deletes left in a DV" (7.5) | `tpch_scan.json` | `tpch_experiments.py scan` | `revision_numbers.scan_lifecycle` |
| Lifecycle paragraph (7.5) | `compaction.json`, `cost_efficiency.json` | `tpch_experiments.py compaction`, `cost_efficiency.py` | `paper_numbers.compaction`, `revision_numbers.scan_lifecycle` |
| Table "Ablations" (7.6) | the files above, plus `packing.json` | as above, `paper_experiments.py packing` | `revision_numbers.ablation` |
| Figure "Real threads against one catalog" (7.7) | `threads.json` | `revision_experiments.py threads` | `revision_figures.fig_threads` |
| Figure and table "Simulated swarm" (7.7) | `scion_sim_calibrated.csv`, `scion_sim_calibrated_summary.json` | `scion_sim.py --calibrated` | `revision_figures.fig_swarm`, `revision_numbers.swarm` |
| Figures 1 and 2, Algorithms 1 and 2, Tables 1 to 4 | none (TikZ and LaTeX in `paper/semlake_pvldb.tex`) | | |

`paper_numbers.py` writes one LaTeX macro per number into `paper/generated/numbers.tex`.
Every ratio in the text is computed there from the two values it compares, and the
statements the text makes in words ("the same requests and bytes as B1R", "no lost update",
"pruning never hides a conflict") are assertions in `revision_numbers.py` that stop the build
if a result file contradicts them.

## What is rerun locally and what is recorded

| Experiment | `rerun` | Why |
| --- | --- | --- |
| Storage ledger, tail latency, delta sweep, threads, fault injection, pruning | yes | in-memory object store |
| Full TPC-H queries, random oracle, selectivity, scan overhead, compaction | yes | `tpchgen-cli` generates the data (`benchmarks/data/`, not committed) |
| Swarm simulation | yes | discrete-event simulation on the real engine |
| Labelled suite and TPC-H pairs | checked | `verify_committed.py` recomputes every decision of SemLake, its ablations, the rule models and the reimplemented baselines and compares it with the committed file, which also holds the Dolt column |
| Live S3 (`s3_run_v3.json`) | recorded | needs an AWS account: `SEMLAKE_S3_BUCKET=<bucket> python3 benchmarks/s3_run.py --depths 1000 10000 --out s3_run_v3.json` (about USD 0.50 of requests) |
| lakeFS 1.88 (`lakefs.json`) | recorded | needs a lakeFS server: `python3 benchmarks/lakefs_runs.py` |
| Delta Lake 4.0 on Spark 4.0 (`delta_spark_pairs.json`) | recorded | needs Java 21, `pyspark==4.0.1`, `delta-spark==4.0.1`: `python3 benchmarks/delta_spark_baseline.py` |
| Dolt (column `dolt` in `m1s_cases.csv` and `tpch_pairs.json`) | recorded | needs the `dolt` binary on `PATH`: `python3 benchmarks/tpch_experiments.py pairs` |
| SF 10 scan (`tpch_scan_sf10.json`) | recorded | 60M rows, about 16 GB of memory: `python3 benchmarks/tpch_experiments.py scan10` |

Timings depend on the machine. Decisions (conflict or not), request counts and byte counts
do not: a rerun must reproduce them exactly, and `verify_committed.py` and the assertions
in `revision_numbers.py` check the ones the paper relies on.

## Tests

```bash
python3 -m pytest -q tests        # unit tests, fault injection, 200-seed fuzzer against a brute-force oracle
```

`tests/test_revision.py` holds the tests added for the revision: the corrected shared-file
merge (both branches' deletion vectors are in the merged snapshot), purge-wins, read capture,
the join and subquery projection, pre-image pruning, choice resolution and the publish gate,
lossless widening, delta manifests, and crash recovery at every write.
