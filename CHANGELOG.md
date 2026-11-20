# Author changelog: Milestone 4 revision

Revision of *SemLake: Purge-Safe Three-Way Merge for Lakehouse Branches* in answer to two peer
reviews (Reviewer 1, Reviewer 2) and the Milestone 4 rubric. The reviewed version is
`SemLake-XiaofanCao.pdf` (2 November). Section, theorem and table numbers below refer to the
revised manuscript, `paper/semlake_pvldb.tex`.

Reading guide: Section 1 lists what changed per review comment. Section 2 lists the claims of the
reviewed version that were wrong and are withdrawn. Section 3 lists the code changes. Section 4
lists where the revision does less than the response letter promised, and why. Section 5 says how
to verify each item.

## 1. Changes per review comment

| Comment | What changed in the paper | What changed in the code and artifact |
| --- | --- | --- |
| **R1-W1, R1-S1** merge tests too small | Section 7.3 adds a delta-size sweep: purges and updates of 10^3 to 10^6 rows, and 1 to 64 read clauses, with CPU time, peak memory and summary bytes (Figure "Delta-size sweep"). Section 5.2 states that merge cost follows the rows changed and not the table size; Section 9 states the in-memory limit | `revision_experiments.py sweep`; `delta_sweep.json` |
| **R1-W2, R1-W4, R1-S2, R2-MC1** joins and subqueries; 1.00 figures only confirm the definition | Section 7.2 is rebuilt around a result oracle. New table "Full TPC-H queries as declared reads": 17 full TPC-H queries plus `count(*)` and `SELECT *`, against 15 refreshes (285 pairs), labelled by whether the query result changes. Results per read class. Definition 3.6 and Lemma 5.1 give the per-block projection of joins and subqueries. Definition-based labels move to one paragraph that is named a consistency check. Abstract and Section 1 scope the correctness claim to declared reads and quote precision next to recall | `semlake/sql_predicates.py` (per-block projection), `benchmarks/tpch_full_queries.py`, `tests/test_revision.py::JoinProjection`; `tpch_full_queries.json` |
| **R2-MC1** undeclared reads | Section 4.2 "Declared reads": a commit whose reads were not captured is treated as a read of the whole table (fail-closed). `strict_reads=False` restores the old behaviour and is named unsound | `reads_captured` flag on commits; `tests/test_revision.py::ReadCapture` |
| **R1-W3, R1-S3** fairness of reimplemented baselines | Section 7.1, new table "Baselines and how each was obtained": native system, rule model checked against a native system, or reimplementation, per baseline | `verify_committed.py` recomputes every decision of the rule models |
| **R2-MC2** B1R missing; group commit only for SemLake | B1R is in every latency, request, byte and cost figure and table. Latency is reported end to end (CPU plus priced requests) and CPU-only. Copy-on-write is reported with 32 parallel requests and sequentially. Every system in the swarm and thread experiments runs with one CAS per commit and with the same commit service. Section 7.7 leads with committed hazards and false blocks; throughput is stated as parity with B1R | `rule_baselines.b1r_storage_merge`; `scion_sim.py` (service for all systems); `revision_experiments.py threads` |
| **R2-MC3** ledger, 380-byte merge, WAF | Section 7.4, new table "Ledger of one W1 merge": count and bytes per object class for commit and merge, every system. Section 5.2 "Bytes" and Section 7.5 "Lifecycle" replace the WAF = 1 claim; the compaction crossover is a range derived from the scan interval | Shared-file benchmark rewritten with real row updates; regression test `SharedFileBenchmark`; delta manifests for commits (`ManifestCheckpoint` every 16); `revision_experiments.py ledger`; `ledger.json` |
| **R2-MC4** scalability dimensions; bound of Section 5.2; Figure 4b | Section 5.2 gives the worst case O(\|Π^D\|·\|Π^R\| + \|Π^R\|·\|Δrows\|) and says the earlier bound assumed a constant number of read clauses. Section 7.3 sweeps rows and clauses. The file-count growth is explained: the old version parsed a full listing per merge (eightfold growth); with delta manifests the remaining 4.4× comes from the in-memory store's LIST | Vectorized row-value tier (`VECTOR_THRESHOLD = 2000`); delta manifests |
| **R2-MC5** choice nodes, update against delete, pruning, remediation | Criterion 2 fixes the semantics of an open choice (readers see the fork value; protected branches refuse open choices) and adds purge-wins. Section 4.5 "After a block" describes the rebase path. Section 4.2 "Pre-image pruning" defines `prune_preimages(c, K)`; Theorem 5.2 covers pruned pre-images (unknown counts as a violation). Section 7.3 counts choices opened, resolved and left open under the W2 sweep | `resolve_choices`, `OpenChoicesError` on protected branches, purge mark on commits, `prune_preimages`, `prunable`; tests `PurgeWins`, `ChoiceResolution`, `PreimagePruning`; `prune.json` |
| **R2-m1** int64 → double is lossy | Criterion 4 keeps int32→int64, int32→double, float→double only | `tests/test_revision.py::LosslessWidening` (covers 2^53 + 1) |
| **R2-m2** scan overhead estimator | Section 7.5 states the estimator: 30 processes, base and DV scans interleaved, median of paired ratios with a bootstrap 95% interval | `tpch_experiments.py scan` |
| **R2-m3** Figure 3 | The x-axis is the realized fraction of rows deleted, points are sorted, and the caption is corrected. "Localized" is defined in Section 7.5 | `paper_experiments.fig_selectivity` |
| **R2-m4** derived ratios; purge-heavy runs; hardware | Every ratio is a generated macro computed from its two operands. W2 and W4 ratios are stated separately. Purge-heavy swarm runs are rows of the swarm table. Section 7.1 lists the hardware, and each new result file records it | `paper_numbers.py`, `revision_numbers.py`, `environment` block in result files |
| **Rubric: contribution** | Section 2.4 with a novelty table against the four assigned papers (P1 cloud-native survey, P2 generic version control, P3 Velox, P4 xNVMe) and the lakehouse systems. Section 4.6 gives the commit protocol and invariants I1 to I5. Figure 2 shows the write path and the CAS | `tests/test_revision.py::CrashRecovery`, `revision_experiments.py recovery` (2,400 crash points, 0 violations) |
| **Rubric: evaluation** | P50 and P99 for every timed merge (60 merges per point), P99 bands in the figures, one ablation table (Section 7.6), scale-up (7.3) and scale-out with real threads (7.7) | `revision_experiments.py tails`, `threads`; `revision_figures.py` |
| **Rubric: reproducibility** | | `reproduce.sh`, `Dockerfile`, `requirements.lock`, `ARTIFACT.md`, `benchmarks/check_pdf.py`, `benchmarks/verify_committed.py` |
| **Rubric: format and related work** | 12 pages of body plus references in the PVLDB template; all figures are vector PDF with embedded fonts; Section 8 adds Iceberg branches and tags, Nessie, Delta shallow clones, predicate and precision locks, and scan-set validation | The build fails on a raster image, a font that is not embedded, an undefined reference or a body over 12 pages |

## 2. Claims of the reviewed version that are withdrawn

| Reviewed version | Revised version | Reason |
| --- | --- | --- |
| A W1 merge writes 380 bytes in 7 requests | 8 requests and 3.92 MB at 10K touched files (392 bytes per touched file) | The old benchmark marked files as changed without row operations, so the merge published an empty manifest and no deletion vector. Found while answering R2-MC3 |
| Merge CPU under 4 ms at 10K files | 263 ms at the median for 10K touched files; 3.6 ms when the delta is fixed and only the manifest grows | Same bug; a real merge does work per touched file |
| Write amplification about 1 | Metadata is 21× the changed bytes for narrow updates; lifecycle rewrite with compaction is 15 rows per deleted row (localized) and 372 (uniform), against 17 and 1,000 for copy-on-write | Summaries and manifests were not counted, and compaction was excluded |
| 99.96% lower request cost, presented as SemLake's gain | 99.95% below copy-on-write, and equal to B1R | The request count is a property of row-level commits with deletion vectors, not of the semantic check. The paper now says so in italics |
| Recall and precision 1.00 | Recall 1.00 on every oracle. Precision 0.41 on full TPC-H queries (B1R 0.24) and 0.74 on random single-table pairs | 1.00 precision held only against labels taken from our own definition |
| 12.1 commits/s against 1.6 for baselines at 64 agents | 12.1 against 13.2 for B1R when both use the commit service; 1.6 for all without it. With real threads: 6.1 against 5.5 for B1 at the peak | Batching is not SemLake's contribution |
| A purged row that the other branch updated stays readable (whole-row choice) | Purge-wins: the delete is applied and the update is listed in the merge summary | Wrong default for an erasure. Found while answering R2-MC5 |
| int64 → double is a lossless widening | Schema conflict | Doubles are exact only up to 2^53 |
| Scan overhead of localized 1% deletes +0.9% (interval −3.4% to +6.9%), uniform +6.0% | +2.0% (+1.3% to +2.9%) localized, +15.5% (+13.1% to +16.6%) uniform | 30 paired processes in place of 5, bootstrap interval on the median, DuckDB 2.0.1. The localized overhead is small but no longer indistinguishable from zero |
| Live S3 at 10K files: 2.15 s against 148.6 s for parallel copy-on-write (69×) | 3.42 s for SemLake, 3.11 s for B1R, 156 s for parallel copy-on-write (46×) | The corrected merge uploads 3.9 MB of summary and manifest |
| Compaction crossover of 3,094 scans per merge (localized) and 446 (uniform) | 1,434 to 3,138 and 247 to 314 | Given as a range from the scan interval, as R2-MC3 asked |
| Merge cost O(\|Π_A\|·\|Π_B\| + \|Δrows\|) | O(\|Π^D\|·\|Π^R\| + \|Π^R\|·\|Δrows\|) | The old bound assumed a constant number of read clauses |

## 3. Code changes

**Bugs found and fixed**

1. *Empty merge in the shared-file benchmark* (`benchmarks/workloads.py`, `workload_w1s`). The branches now carry real row updates. `tests/test_revision.py::SharedFileBenchmark` asserts that the merged snapshot holds both branches' deletion vectors.
2. *Update against a purge* (`semlake/merge.py`). Purge-wins, as above.
3. *Lossy widening* (`semlake/metadata.py`). `int64 → double` removed.
4. *Lost update in the optimistic thread benchmark.* Two increments of one row computed from the same base wrote the same value and merged as an agreement. An increment now declares the read of its row, so the race is a semantic conflict and the loser is redone. The benchmark asserts that every published increment is on the head.
5. *Per-row DuckDB sessions for clauses outside the fragment* (`semlake/verification.py`). Such clauses are now evaluated in one pass; the full-query run fell from hours to minutes.
6. *Full file listing on every commit* (`semlake/metadata.py`). Commits and merges write a delta manifest and a manifest list; every 16th commit writes a full listing.

**New mechanisms**: per-block projection of joins and subqueries; `reads_captured` and `strict_reads`; purge mark; `resolve_choices` and the publish gate on protected branches; `prune_preimages` and `prunable`; crash injection (`FaultyStore`) and `collect_garbage` with registered in-flight snapshots; group commit for B1, B1R and B2 in the simulator and the thread benchmark.

**New experiments** (`benchmarks/`): `revision_experiments.py` (`ledger`, `tails`, `sweep`, `prune`, `recovery`, `threads`), `tpch_full_queries.py`, `verify_committed.py`, `check_pdf.py`, `revision_numbers.py`, `revision_figures.py`. `s3_run.py` now runs B1R and parallel copy-on-write and saves after every row. `tpch_experiments.py scan` uses 30 interleaved processes and a bootstrap interval.

**Tests**: 87 tests (83 pass; 4 are skipped unless the `dolt` binary is installed). `tests/test_revision.py` is new.

## 4. Where the revision does less than the response letter said

| Promised in the response | Delivered | Reason |
| --- | --- | --- |
| int64 → double accepted when the column's range fits in ±2^53 | Rejected in every case | No per-column range statistic is kept in the schema; rejecting is the sound choice |
| Unmarked delete against update blocks the merge | It stays a whole-row choice, which the publish gate keeps off protected branches | A block would discard the mergeable rest of the branch; the gate gives the same guarantee for `main` |
| Pruning with a salted hash and a retention limit | `prune_preimages(c, K)` with a column set, and `prunable` for the point where a full prune is free | The hash helps equality clauses only; the retention rule needs a branch-expiry policy that the prototype does not have |
| W1 and W4 merges run natively on delta-rs and Spark Delta | Not run. Delta Lake 4.0 is measured on the conflict decisions only; storage comparisons use the B1R model on our own format | delta-rs has no three-way branch merge to run; a commit-against-commit comparison would measure a different operation |
| Merge pairs at SF 1 and SF 10 | Delta sweep to 10^6 changed rows; correctness pairs at SF 0.01 and SF 0.1 | The prototype keeps row state in memory. The paper states this limit |
| SF 10 scan rerun | The SF 10 result file of 30 October is kept as recorded and is not used by a number in the paper | 16 GB of memory needed |
| TPC-H Q1, Q3, Q4, Q6, Q7, Q10, Q12, Q14, Q17, Q18 | All 17 TPC-H queries that read `lineitem` | More than promised |
| 1 to 128 writer threads | 1 to 64 | Four cores; 128 threads measure the scheduler |
| Latency CDFs for the swarm | P50, P95 and P99 in the result files; P99 in the text | Space |
| Live S3 at 10K files for every system | SemLake, B1R and parallel copy-on-write, 3 runs each; all systems at 1K files, 5 runs (sequential copy-on-write: one run) | Sequential copy-on-write takes over an hour per run |
| Verify or remove two vendor citations | Removed | They could not be opened |

Not updated: the Chinese translation `paper/semlake_paper_zh.md` still describes the 2 November version.

## 5. How to verify

```bash
./reproduce.sh              # rebuild every number, table, figure and the PDF from benchmarks/results/
./reproduce.sh check        # the above, plus the unit tests and a decision check against the committed results
./reproduce.sh quick        # smoke-run every local experiment at a small size
./reproduce.sh rerun        # rerun every local experiment at full size (hours)
```

`ARTIFACT.md` maps every figure and table to its raw result file and the command that produces
it. `CORRECTNESS.md` maps every claim to its test. The statements the paper makes in words
("the same requests and bytes as B1R", "no lost update", "pruning never hides a conflict") are
assertions in `benchmarks/revision_numbers.py` that stop the build if a result file contradicts
them.
