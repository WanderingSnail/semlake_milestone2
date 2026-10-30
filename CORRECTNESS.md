# Correctness of the SemLake Three-Way Semantic Merge

## 1. Purge-Safe Merge Invariant (Definition 3.5)

Let $T_A$ and $T_B$ be two branches with fork point (lowest common ancestor) $T_0 = \text{LCA}(T_A, T_B)$. A merged snapshot $T_M = \text{merge}(T_0, T_A, T_B)$ is **purge-safe** if, symmetrically for $(X, Y) \in \{(A, B), (B, A)\}$:

1. **Read-Delete Safety (`reads_where` vs. `deletes_where`)**: For every $\texttt{reads\_where}(\varphi_R) \in \Pi(Y)$ and every row $r \in D(X)$ deleted on branch $X$, neither the row's value at the fork ($\rho_0(r)$, when $r$ existed in $T_0$) nor its pre-image at delete time ($\sigma_X(r)$) satisfies $\varphi_R$.
2. **Row-Lineage and Foreign-Key Preservation (`rows_live`, `foreign_key`)**: For every $\texttt{rows\_live}(I)$ or $\texttt{foreign\_key}(I) \in \Pi(Y)$, $D(X) \cap I = \emptyset$.
3. **Domain Invariant Preservation (`domain`)**: For every $\texttt{domain}(\varphi_{\text{dom}}) \in \Pi(Y)$ and every `insert` or `update` assignment $(r, v) \in \text{ops}_X$, $v \models \varphi_{\text{dom}}$.
4. **Read Stability under Updates and Inserts (`Unstable`)**: For every $\texttt{reads\_where}(\varphi_R) \in \Pi(Y)$ and every row $r \in U_X$ that branch $X$ updates or inserts without deleting (with tip value $\rho_X(r)$), $r$ stays on the same side of $\varphi_R$, namely $\rho_0(r) \models \varphi_R \Leftrightarrow \rho_X(r) \models \varphi_R$ (treating an inserted row as outside $\varphi_R$ at $T_0$), and whenever $r$ satisfies $\varphi_R$ in both states, $\rho_0(r)$ and $\rho_X(r)$ agree on the projected columns $\text{cols}(\varphi_R)$ referenced outside the `WHERE` clause.

## 2. Incomparability with Two-Branch Serializability (Proposition 3.6)

Purge-safety and classical two-branch conflict serializability ($B_6$, SSI) are incomparable:

- **Serializable but not purge-safe**: Suppose branch $T_B$ reads VIP rows ($\varphi_R$) from $T_0$ and writes a summary row into a new file, while branch $T_A$ deletes VIP rows ($\varphi_D$) from $T_0$ and reads nothing from $T_B$. In Adya's dependency graph there is only a single antidependency edge $T_B \xrightarrow{\mathit{rw}} T_A$ and no cycle, so the schedule $T_B \prec T_A$ is conflict-serializable and accepted by SSI. However, in the merged snapshot $T_M$, both the erasure of VIP records and the pre-erase aggregate coexist in the published table state, violating Condition 1 of purge-safety.
- **Purge-safe but not serializable**: Suppose $T_A$ and $T_B$ assign different values to the same column of the same row with no attached predicates. Purge-safety holds, and SemLake preserves both candidate values in a VSA choice node (`choice(r, c, [(A, v_A), (B, v_B)])`), whereas any serial execution order overwrites one of the two updates.

## 3. Soundness of the Two-Tier Verifier (Theorem 5.1)

Algorithm 2 (`VerifyMergeSemantics` in `semlake/verification.py`) enforces Conditions 1-4 in metadata time using only the folded commit summaries $\Delta_A, \Delta_B$ and fork row view $\rho_0$:

- **Tier-1 Static Solver (`WhereOverlap`)**: When both $\varphi_D$ and $\varphi_R$ belong to the conjunctive fragment $\mathcal{F}$ (conjunctions of binary comparisons $=, \neq, <>, <, \le, >, \ge$, finite set membership `IN`, and closed intervals `BETWEEN` against literals), constraints are grouped by column. If any column has an empty candidate intersection or empty interval, $\varphi_D \land \varphi_R$ is unsatisfiable and no delete-time pre-image $\sigma_X(r)$ can satisfy $\varphi_R$. Line 8 then checks $\rho_0(r)$ only on the subset $\mathit{moved}_X = \{r \in D_X \mid \sigma_X(r) \neq \rho_0(r)\}$ of rows that branch $X$ modified before deleting.
- **Tier-2 Deleted-Row Value Check (`AnyMatch`)**: Whenever `WhereOverlap` returns `True` (either because a clause lies outside $\mathcal{F}$, such as `OR` or `NOT`, or because the static intervals overlap), Lines 9-10 evaluate $\varphi_R$ directly against $\rho_0(r)$ and $\sigma_X(r)$ for every $r \in D_X$. Single-table clauses outside $\mathcal{F}$ are evaluated in memory via DuckDB over the candidate rows; multi-table joins, subqueries, or rows missing pre-images conservatively retain the conflict.
- **Read-Stability Check (`Unstable`)**: Line 14 evaluates $\varphi_R$ on $\rho_0(r)$ and $\rho_X(r)$ for every updated or inserted row $r \in U_X$, flagging any row that crosses the predicate boundary or alters $\text{cols}(\varphi_R)$ while inside $\varphi_R$.

## 4. Exact DAG Replay Since the Fork (Lemma 5.2)

Algorithm 1 (`ReplaySinceFork` in `semlake/merge.py`) performs a post-order DFS from branch tip $t$ halting at $\text{anc}(b)$ where $b = \text{LCA}(T_A, T_B)$. Every non-merge commit in $\text{anc}(t) \setminus \text{anc}(b)$ is folded once in topological order, while internal merge diamonds strip their shared prefix (`StripPrefix`) relative to their internal LCA so shared history is never double-counted and predicates from earlier clean merges are preserved. We cross-check Theorem 5.1 and Lemma 5.2 across 200 randomized branch-merge seeds (`tests/test_fuzz.py`) against a brute-force row-state oracle.
