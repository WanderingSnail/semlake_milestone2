# Correctness of the SemLake three-way semantic merge

Numbers refer to the revised manuscript (`paper/semlake_pvldb.tex`).

## 1. Purge-safe merge (Definition 3.4)

Let $T_A$ and $T_B$ be two branches with fork $T_0 = \text{LCA}(T_A, T_B)$. A merge is
**purge-safe** if, for $(X, Y) \in \{(A, B), (B, A)\}$:

1. **Reads against deletes.** For every $\texttt{reads\_where}(\varphi_R, C) \in \Pi(Y)$ and every
   row $r$ deleted on $X$, neither the row's value at the fork, $\rho_0(r)$, nor its pre-image at
   delete time, $\sigma_X(r)$, satisfies $\varphi_R$.
2. **Row liveness and foreign keys.** For every $\texttt{rows\_live}(I)$ or
   $\texttt{foreign\_key}(I) \in \Pi(Y)$, $D(X) \cap I = \emptyset$.
3. **Domain invariants.** For every $\texttt{domain}(\varphi_{\text{dom}}) \in \Pi(Y)$, every value
   $X$ assigns satisfies $\varphi_{\text{dom}}$.
4. **Read stability under updates and inserts.** For every $\texttt{reads\_where}(\varphi_R, C) \in \Pi(Y)$
   and every row $r$ that $X$ updates or inserts, $r$ stays on the same side of $\varphi_R$
   (an inserted row counts as outside at the fork), and if it is inside, $\rho_0(r)$ and
   $\rho_X(r)$ agree on the columns $C$ the reader uses outside the clause.

## 2. Incomparable with two-branch serializability (Proposition 3.5)

- **Serializable, not purge-safe.** $T_B$ reads the rows under $\varphi_R$ and writes an aggregate;
  $T_A$ deletes rows under $\varphi_D$ and reads nothing of $T_B$. The dependency graph has one
  antidependency $T_B \to T_A$ and no cycle, so SSI accepts the order "report, then purge". The
  merged snapshot nevertheless publishes the erasure and an aggregate over the erased rows.
- **Purge-safe, not serializable.** Both branches set one cell to different values with no
  predicates. Purge-safety holds; SemLake keeps both values as a choice node, which no serial
  order produces.

## 3. Semantics that the definition leaves to the merge (Criterion 2)

- **Open choices.** A reader of a snapshot with an open choice sees the *fork* value of the cell
  (or the fork row for a whole-row choice). A branch declared protected refuses a head with open
  choices (`cas_head` raises `OpenChoicesError`). `resolve_choices` closes choices in one ordinary
  commit, which is itself subject to the merge check.
- **Purge-wins.** If one branch updates a row and a commit marked as a purge deletes it on the
  other, the delete is applied and the overridden update is listed in the merge summary. A purged
  row is never readable in the merged snapshot. Without the purge mark, update against delete is a
  whole-row choice, which the publish gate holds back.
- **Lossless widening only.** `int32 -> int64`, `int32 -> double`, `float -> double`. `int64 -> double`
  is a schema conflict: a double represents integers exactly only up to $2^{53}$.

## 4. Declared reads and the projection of joins and subqueries (Definition 3.6, Lemma 5.1)

Soundness covers declared reads. A commit whose reads could not be captured is marked
`reads_captured = false` and is treated as a read of the whole table with every column, so any
change on the other branch blocks (fail-closed; `strict_reads=False` restores the old behaviour).

A query with joins or subqueries is reduced to one single-table clause per query block:
the block's `WHERE` and `ON` condition with every sub-condition that mentions another table, a
subquery or an outer reference replaced by `TRUE` (a conjunct disappears; a disjunction with such
a branch becomes `TRUE` as a whole). A block with an outer, semi or anti join on the merged table
projects to `TRUE`. The columns $C$ are those the query uses anywhere except inside a conjunct kept
unchanged.

*Lemma 5.1.* If no changed row satisfies a block's projected clause before or after the change,
except rows that satisfy it in both states and agree on $C$, the query returns the same result.
Replacing a condition by `TRUE` under conjunction and disjunction only widens the rows a block may
see, so each block's input is contained in the rows that satisfy its projection.

One predicate per block, and not their disjunction, is required for updates: a row that leaves one
block's range for another's has crossed a clause (`test_update_that_moves_a_row_between_blocks_conflicts`).

## 5. Soundness of the two-tier check (Theorem 5.2)

Assumptions: (a) every delete carries a `deletes_where` clause its rows satisfied at delete time;
(b) summaries record pre-images, except for rows listed as pruned; (c) every read of a commit is
declared, or the commit is marked `reads_captured = false`.

- **Static tier (`where_overlap`).** For clauses in the conjunctive fragment $\mathcal{F}$, an
  unsatisfiable $\varphi_D \land \varphi_R$ proves that no delete-time pre-image satisfies
  $\varphi_R$; rows the deleting branch changed before deleting them are still checked at their
  fork value.
- **Row-value tier.** Otherwise $\varphi_R$ is evaluated on the fork value and the pre-image of
  every deleted row (one DuckDB pass per clause for clauses outside $\mathcal{F}$ and for row sets
  of 2,000 or more).
- **Update check (`Unstable`).** Every updated or inserted row is evaluated at the fork and at the
  tip; a row that crosses the clause, changes a column of $C$ inside it, or cannot be evaluated is
  a violation.
- **Pruned pre-images.** `prune_preimages(c, K)` keeps only the columns $K$ of the pre-images of
  the rows commit $c$ deleted. A clause that the kept columns cannot decide evaluates to *unknown*,
  and *unknown* is a violation: pruning loses precision, never soundness. A full prune ($K = \emptyset$)
  is free of precision cost once every open branch contains the commit (`prunable`).

## 6. Exact replay since the fork (Lemma 5.3)

`ReplaySinceFork` folds every non-merge commit of $\text{anc}(t) \setminus \text{anc}(b)$ exactly
once in topological order, folds no commit of $\text{anc}(b)$, and keeps predicates and open
choices across inner merges (`StripPrefix` removes the delta shared at an inner fork).

## 7. Commit protocol and recovery invariants (Section 4.6)

A commit or merge writes its data objects, its summary, and its delta manifest and manifest list
under keys that contain the new snapshot's identifier, and then moves the branch head with one
catalog compare-and-swap. The CAS is the commit point.

| | Invariant | Follows from |
| --- | --- | --- |
| I1 | A branch head names a snapshot whose summary, manifests and data objects all exist, and so do its ancestors' | the write order |
| I2 | A crash before the CAS changes no head; it leaves only objects no head reaches | unique keys and the single commit point |
| I3 | Re-running a crashed commit or merge from the same parents yields a snapshot with the same rows | determinism of the merge |
| I4 | A group commit moves the head once: all merges of a batch become visible, or none | one CAS per batch |
| I5 | Garbage collection deletes an object iff no head and no registered in-flight snapshot reaches it | reachability computed before deleting |

Recovery is to do nothing: there is no log to replay, and `collect_garbage` reclaims orphans.

## 8. How each claim is tested

| Claim | Test or experiment |
| --- | --- |
| Theorem 5.2 and Lemma 5.3 against a brute-force evaluator of Definition 3.4 | `tests/test_fuzz.py` (200 seeds), `tests/test_edge_cases.py` |
| Projection (Lemma 5.1) | `tests/test_revision.py::JoinProjection`; `benchmarks/tpch_full_queries.py` labels 285 pairs of full TPC-H queries by their results (recall 1.00) |
| Result oracle on single-table reads | `benchmarks/oracle_experiments.py` (300 pairs, recall 1.00) |
| Purge-wins, choice semantics, publish gate, read capture, lossless widening | `tests/test_revision.py` (`PurgeWins`, `ChoiceResolution`, `ReadCapture`, `LosslessWidening`) |
| Pruning keeps soundness | `tests/test_revision.py::PreimagePruning`; `revision_experiments.py prune` (a conflicting report is blocked in every pre-image setting) |
| I1 to I5 | `tests/test_revision.py::CrashRecovery`; `revision_experiments.py recovery` crashes before every PUT of commits, merges and group commits (2,400 crash points, 0 violations) |
| The merged snapshot of the shared-file benchmark holds both branches' deletion vectors | `tests/test_revision.py::SharedFileBenchmark` |
| No lost update under concurrent writers | `revision_experiments.py threads` (every published increment is on the head; an increment declares the read of its row) |

The label used by the labelled suite and the 77 TPC-H filter pairs is Definition 3.4 itself, so
those two experiments check that the code implements the definition. Whether the definition blocks
the right merges is measured by the two result-oracle experiments, where precision is below 1.
