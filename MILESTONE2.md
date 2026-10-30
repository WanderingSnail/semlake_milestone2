# Milestone 2 & PVLDB Artifact Guide

Author: Xiaofan Cao

This document maps the repository deliverables to the implementation modules, test suites, and benchmark outputs backing both the Milestone 2 prototype and the PVLDB paper (`paper/semlake_pvldb.pdf`).

## Deliverables

| Deliverable | Location |
| --- | --- |
| Git repository | `https://github.com/WanderingSnail/semlake_milestone2` (mirrored in `SemLake`) |
| Core engine (`semlake/`) | `metadata.py`, `storage.py`, `merge.py`, `verification.py`, `sql_predicates.py`, `reader.py`, `s3store.py`, `baselines.py` |
| Automated benchmark harness | `benchmarks/run_all.py`, `benchmarks/tpch_experiments.py`, `benchmarks/scion_sim.py`, `benchmarks/cost_efficiency.py`, `benchmarks/s3_run.py`, `benchmarks/paper_experiments.py`, `benchmarks/paper_numbers.py` |
| Raw execution traces & results | `benchmarks/results/` (`benchmark_run_summary.csv`, `m1s_cases.csv`, `tpch_pairs.json`, `tpch_update_pairs.json`, `selectivity.json`, `tpch_scan.json`, `tpch_scan_sf10.json`, `compaction.json`, `cost_efficiency.json`, `packing.json`, `scion_sim_summary.json`, `s3_run.json`) |
| Publication figures & tables | `paper/figures/*.pdf`, `paper/generated/numbers.tex`, `paper/generated/tab_correctness.tex`, `paper/generated/tab_scan.tex`, `paper/generated/tab_swarm.tex` |
| Test suites | `tests/test_core.py`, `tests/test_phase_bc.py`, `tests/test_fuzz.py` |

## Reproduction Commands

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
python benchmarks/run_all.py --quick
python benchmarks/paper_numbers.py
```

## Summary of Results Against System Objectives ($M_1$-$M_4$)

| Metric | Target | Measured Result | Status |
| --- | --- | --- | --- |
| $M_1$ False aborts on shared files ($W_1$) | 0% | SemLake **0%** across 1 to 1,024 rows/file; $B_1$ climbs to **100%** | Met |
| $M_1$ Semantic conflict recall & precision ($W_3$) | 100% recall | **1.00 recall, 1.00 precision** across 34 scenarios (18 conflicts), 42 TPC-H delete-refresh pairs (22 conflicts), and 35 TPC-H update/insert pairs (10 conflicts) | Met |
| $M_2$ Merge CPU latency at 10K files ($W_2, W_4$) | Sub-second CPU, $\ge 10\times$ faster than $B_1/B_2$ | $W_4$ at 10K files: SemLake **3.22 ms** vs. $B_1$ **147.0 ms**, $B_2$ **129.8 ms**, $B_3$ **171.4 ms** (40-53$\times$ faster). $W_2$ at 10K files: **9.02 ms** ($\rho=0\%$) and **8.12 ms** ($\rho=20\%$) vs. $B_1$ **227.3 ms** and **203.2 ms** | Met |
| $M_2$ Remote S3 wall-clock latency (1K files, `eu-north-1`) | Characterize WAN round-trips vs. CPU | SemLake merges $W_1$ in **1.76 s** and $W_4$ in **1.50 s** (7 S3 requests) vs. sequential $B_2$ **472 s** (2,007 requests, **268$\times$ faster**); WAN RTTs averaged 235 ms/request | Characterized on live S3 |
| $M_3$ S3 request cost vs. CoW $B_2$ ($W_1$) | $\ge 95\%$ reduction | **99.96% reduction** at 10K files (7 requests / \$0.0000212 vs. 20,007 requests / \$0.054012); cheaper than CoW up to **446-3,094** full scans/merge | Met |
| $M_4$ Localized post-merge scan overhead ($W_1$) | $< 8\%$ | **+0.9%** median overhead at TPC-H SF 1 (95% CI: $-3.4\%$ to $+6.9\%$) and **$-1.4\%$** at SF 10; uniform 1% delete overhead is **+6.0%** (SF 1) and **+5.6%** (SF 10) | Met |
| $W_{\text{swarm}}$ Concurrent agent commit throughput | Hazard-free fan-in scaling | At 64 agents (10 ms catalog RTT), group commit ($G=8$) reaches **17.12 commits/s** (vs. **4.81 commits/s** single CAS) with **0 committed hazards** | Met |
