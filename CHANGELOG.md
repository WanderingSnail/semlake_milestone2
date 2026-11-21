# Author Response and Revision Changelog

**Paper:** *SemLake: Zero-Copy Branching and Three-Way Semantic Merging for Data Lakehouses* (`SemLake-XiaofanCao.pdf`)  
**Author:** Xiaofan Cao (Qatar University)

Thank you to both reviewers for the careful reading and concrete feedback. Below is a point-by-point summary of how we addressed each comment in the revised manuscript and codebase, followed by the claims we corrected from the 2 November draft and a short note positioning SemLake against the four course papers (P1–P4).

## 1. Point-by-Point Response to Reviewers

### Reviewer 1

| ID | Comment | Response and Revision |
| :--- | :--- | :--- |
| **R1-W1, R1-S1** | In-memory prototype; `SF 0.01` merge tests are too small for scalability. | Because verification reads only commit summaries and not base Parquet files, merge cost depends on changed rows ($|\Delta_{\text{rows}}|$) and predicate count ($|\Pi|$). We added a delta-size sweep (**Section 7.3, Figure 5**) from $10^3$ to $10^6$ changed rows and 1 to 64 read clauses (at $10^6$ deleted rows: **16.4 s** P50, **4.9 GB** peak RSS, **213 MB** summary; linear in $|\Delta_{\text{rows}}|$), plus a vectorized DuckDB/Arrow Tier-2 pass (`semlake/verification.py`). All 77 TPC-H pairs are also run at `SF 0.1` (**Section 7.2**), and **Section 9** notes the in-memory row-overlay limit of the Python prototype. |
| **R1-W2, R1-W4, R1-S2** | Joins and subqueries are not handled; measure false blocks on complex queries outside $\mathcal{F}$. | Added per-query-block single-table projection $\pi_R(b)$ for inner, outer, semi, and anti joins and subqueries (**Definition 3.6, Lemma 5.1**, `semlake/sql_predicates.py`). Evaluated all **17 full TPC-H queries reading `lineitem`** plus `count(*)` and filtered `SELECT *` against 15 refreshes (**285 pairs**, labelled strictly by whether the query result changes, **Section 7.2, Table 6**): **1.00 recall**, **0.41 precision**, and **31% false blocks** (0% single-table, 27% inner joins with local filters, 14% subqueries, 62% whole-table joins), compared to **69% false blocks** for $B_1\text{R}$ and **80%** without block projection (**Table 9**). |
| **R1-W3, R1-S3** | Clarify fairness between re-implemented baselines and native systems. | Added **Table 5 (Section 7.1)** listing each baseline's type and native cross-check: **lakeFS 1.88** (matches $B_1$ on 111/111 pairs), **Spark 4.0 + Delta Lake 4.0** (run on 77 TPC-H pairs alongside $B_1\text{R}$), native **Dolt 2.5.0**, live **Amazon S3**, rule models on our shared storage path ($B_1, B_1\text{R}, B_2, B_6$), and layout reimplementations ($B_3, B_4$). `benchmarks/verify_committed.py` checks all rule-model decisions against committed results. |

### Reviewer 2

| ID | Comment | Response and Revision |
| :--- | :--- | :--- |
| **R2-MC1** | 1.00/1.00 scores only confirm Definition 3.5; measure precision on full queries and handle undeclared reads. | Rebuilt **Section 7.2** around two query-result oracles: **285 full TPC-H query pairs** (**Table 6**: 1.00 recall, 0.41 precision, 31% false blocks) and **300 random single-table pairs** (1.00 recall, 0.74 precision; all 37 false blocks are `max` queries, 1.00 without `max`). Moved the 34-case suite and 77 filter pairs to a single paragraph explicitly labelled as a consistency check against Definition 3.4. Added fail-closed read capture (**Section 4.2, Theorem 5.2**): commits with `reads_captured = false` default to whole-table reads under `strict_reads=True` (`tests/test_revision.py::ReadCapture`). Abstract and Section 1 scope claims to declared reads and quote precision alongside recall. |
| **R2-MC2** | $B_1\text{R}$ missing from $M_2$–$M_3$; sequential $B_2$; group commit only for SemLake. | Implemented `b1r_storage_merge` on the same DV and delta-manifest path and added $B_1\text{R}$ to all latency, ledger, live-S3, and swarm plots/tables (**Figs. 4, 6, 8; Tables 7, 10**). Measured $B_2$ with **32 parallel requests** as well as sequentially, reporting end-to-end and live-S3 times (10K files: **3.42 s** SemLake, **3.11 s** $B_1\text{R}$, **156 s** parallel $B_2$). Gave the same group-commit service ($G=8$) to every baseline in both the real-thread benchmark (**Fig. 7**) and the 64-agent swarm (**Fig. 8, Table 10**). **Sections 7.4 and 7.7** state that the 8-request count is shared with $B_1\text{R}$ and throughput comes from batching; SemLake's gain is **0 committed hazards** (vs. 67–234) and **0.0 false blocks/run** at 64 agents (vs. 43.6 for $B_1\text{R}$). |
| **R2-MC3** | Reconcile 380 B merge ledger; include compaction in WAF; give crossover as a CI range. | Investigating the 380 B figure exposed a bug in `workload_w1s` (it touched file entries without row updates, writing an empty manifest). Fixed `workload_w1s` to update 1 row in each of 10,000 files (`SharedFileBenchmark` test) and moved commits to delta manifests. **Table 7 (Section 7.4)** reports per-object commit and merge counts/bytes: **8 requests** and **3.92 MB** at 10K files (same as $B_1\text{R}$). Withdrew $\text{WAF}\approx 1$; **Section 7.5** reports 40-merge lifecycle row rewrites including compaction (**15** localized / **372** uniform rows per deleted row, vs. 17 / 1,000 for $B_2$) and CI crossover ranges (**1,434–3,138** localized; **247–314** uniform). |
| **R2-MC4** | Complexity bound mismatch; sweep rows and predicates; explain Fig. 4b growth. | Updated **Section 5.2** to the exact worst-case bound $O(\|\Pi^D\|\cdot\|\Pi^R\| + \|\Pi^R\|\cdot\|\Delta_{\text{rows}}\|)$. **Fig. 5** sweeps rows ($10^3$–$10^6$) and read clauses (1–64). **Section 7.3** explains Fig. 4b: switching commits from full listings to delta manifests cut 1K–10K merge CPU to **0.81–3.55 ms**, with the remaining $4.4\times$ growth coming from `MemoryObjectStore.list()` scanning its 10K-key dict. |
| **R2-MC5** | VSA choice visibility/resolution, update vs. delete on purges, post-block remediation, pre-image pruning. | Specified in **Criterion 2, Sections 4.2 & 4.5, and Theorem 5.2**: (1) open choices show the **fork value** on unprotected branches; protected branches (`main`) reject $\mathcal{V}\neq\emptyset$ at CAS (`OpenChoicesError`) until closed by `resolve_choices` (1,509 choices opened at $\rho=0.5$, resolved in 1.13 ms median, 0 open); (2) **purge-wins**: deletes marked `purge=True` override concurrent updates and log them in the merge summary (`PurgeWins` test); (3) **Section 4.5** describes zero-undo rebase remediation after a block; (4) `prune_preimages(c, K)` and `prunable` strip deleted-row pre-images while Theorem 5.2 proves soundness (undecidable pruned clauses count as conflicts; full pruning cuts summary size by **86%** once all open heads contain $c$). |
| **R2-m1** | `int64 -> double` is lossy above $2^{53}$. | Removed `int64 -> double` from **Criterion 4** (`semlake/metadata.py`); it now returns `schema_conflict` (`LosslessWidening` test at $2^{53}+1$). |
| **R2-m2** | State Table 4 scan overhead estimator; add interleaved runs. | **Section 7.5 & Table 8** state the estimator (median of **30 interleaved paired processes** with a bootstrap 95% CI): **+2.0% (+1.3% to +2.9%)** localized, **+15.5% (+13.1% to +16.6%)** uniform. |
| **R2-m3** | Clarify Fig. 3 x-axis and define "localized". | **Figure 3** plots realized deleted-row fraction sorted monotonically; **Section 7.5** defines *localized* as the smallest contiguous run of row groups holding the deletes. |
| **R2-m4** | Derived ratios, purge-heavy swarm table, and hardware specs. | All ratios are generated from their operands in `revision_numbers.py` ($W_2$ $29\times$ and $W_4$ $30\times$ reported separately); purge-heavy swarm rows are in **Table 10**; **Section 7.1** lists full VM hardware/software specs (`AMD EPYC 7763`, 4 vCPUs, 16 GB RAM, Ubuntu 24.04, Python 3.13.18, DuckDB 2.0.1). |

---

## 2. Claims Corrected from the 2 November Draft

| 2 November Draft | Revised Paper (`SemLake-XiaofanCao.pdf`) | Reason |
| :--- | :--- | :--- |
| 10K-file $W_1$ merge: 380 B, 7 requests | **8 requests, 3.92 MB** (~392 B/touched file) | Fixed empty-merge bug in `workload_w1s` |
| Merge CPU $<4$ ms at 10K touched files | **263 ms** median (10K changed DVs); **3.55 ms** fixed delta | Real per-touched-file DV union/serialization |
| $\text{WAF} \approx 1$ | Metadata $21\times$ data on narrow updates; lifecycle **15** (loc.) / **372** (uni.) rows/deleted row | Summaries, manifests, and compaction counted |
| 99.96% lower S3 cost as SemLake's gain | 99.95% below $B_2$, **identical to $B_1\text{R}$** | Shared property of DV + delta manifests |
| 1.00 recall and 1.00 precision headline | **1.00 recall**; **0.41 precision** (full TPC-H), **0.74** (random) | Evaluated under independent result oracles |
| 12.1 vs. 1.6 commits/s at 64 agents | **12.1** (SemLake) vs. **13.2** ($B_1\text{R}$) with shared service | Group commit given to all baselines; 0 vs. 67–234 hazards |
| Purged row vs. update $\to$ choice node | **Purge-wins:** delete applied, update logged | Erased row never remains readable |
| `int64 -> double` lossless widening | `schema_conflict` | Doubles exact only up to $2^{53}$ |
| Scan overhead +0.9% loc., +6.0% uni. | **+2.0% (+1.3 to +2.9%)** loc., **+15.5% (+13.1 to +16.6%)** uni. | 30 interleaved paired processes, bootstrap CI, DuckDB 2.0.1 |
| Live S3 10K: 2.15 s vs. 148.6 s ($69\times$) | **3.42 s** (SemLake), **3.11 s** ($B_1\text{R}$), **156 s** parallel $B_2$ ($46\times$) | Corrected 3.92 MB merge + $B_1\text{R}$ on live S3 |
| Crossover 3,094 loc. / 446 uni. | **1,434–3,138** loc. / **247–314** uni. | Range propagated from Table 8 95% CI |
| Bound $O(\|\Pi_A\|\cdot\|\Pi_B\| + \|\Delta_{\text{rows}}\|)$ | $O(\|\Pi^D\|\cdot\|\Pi^R\| + \|\Pi^R\|\cdot\|\Delta_{\text{rows}}\|)$ | Worst-case Tier-2 dependence on $\|\Pi^R\|$ |

---

## 3. Positioning Against Course Assigned Papers (P1–P4)

To keep `SemLake-XiaofanCao.pdf` focused as a venue-ready PVLDB manuscript, the paper cites these works in context without course labels (`P1`–`P4`). For course grading, the table below maps SemLake's novelty directly against P1–P4:

| Assigned Paper | What It Covers | What SemLake Adds |
| :--- | :--- | :--- |
| **P1:** Cloud-native databases survey (Dong et al., TKDE 2024) | Disaggregated OLTP/OLAP, metadata pruning, zero-copy cloning, multi-writer challenges | Adds a three-way merge operator for diverged clones with a formal purge-safety contract, single-CAS commit protocol, and crash-recovery invariants **I1–I5** (tested across 2,400 injected crashes in **Section 7.2**). |
| **P2:** Generic version control (Yilmaz & Dittrich, CIDR 2025) | Vision unifying Git branches and MVCC transactions with pluggable `detect_conflicts` and `reconcile` | Provides a working, measured instance on cloud object storage with a contract proved incomparable to serializability (**Proposition 3.5**), post-commit summary verification, and Roaring DV physical reconciliation. |
| **P3:** Velox (Pedreira et al., PVLDB 2022) | Vectorized query execution library with filter pushdown | Complementary read layer: our $M_4$ DuckDB SQL mask-join overhead (**Table 8**) is an upper bound on a vectorized engine that pushes Roaring DV bitmaps down into the scan operator. |
| **P4:** xNVMe I/O for DBMSs (Houlborg et al., 2026) | High-throughput NVMe I/O paths (`io_uring`, SPDK) for DuckDB | Complementary device layer: SemLake merges read no data files and are bounded by object-store requests; faster local/NVMe I/O speeds up post-merge scans and compaction. |

---

## 4. Verification and Reproducibility

- **Unit & fuzz tests:** 85 passing tests (`tests/test_core.py`, `tests/test_phase_bc.py`, `tests/test_edge_cases.py`, `tests/test_fuzz.py`, and 35 new revision tests in `tests/test_revision.py`).
- **One-command rebuild:** `./reproduce.sh` (or `docker build -t semlake . && docker run --rm -v "$PWD/out:/out" semlake`) regenerates all macros, tables, vector figures, and the PDF from `benchmarks/results/`, and verifies decisions (`verify_committed.py`) and PDF formatting (`check_pdf.py`).
