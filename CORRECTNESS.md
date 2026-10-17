# Correctness of the supported merge

## Invariant

A committed merge does not leave a read predicate satisfied by a row that a concurrent branch deleted under a delete predicate, when both predicates are in the supported fragment. The fragment is a conjunction of comparisons, `IN`, and `BETWEEN` against literals. `OR`, `NOT`, functions, and joins are outside it.

This is stricter than serializability. A report that reads a row, followed by a purge of that row, is a serial order. The purge-safe mode rejects it anyway, because a committed aggregate must not depend on rows a concurrent branch removed.

## Why a parsed overlap is never dropped

`where_overlap` returns no overlap only when some column has no value that satisfies both clauses. If it returns an overlap and both clauses parsed, the merge is blocked. The row-level post-check does not run on that result, so a parsed conflict is not cleared by looking at the current rows.

## Why the post-check does not add a false negative inside the fragment

The post-check runs only when a clause did not parse. It drops the conflict when every deleted row fails the other clause and that other clause does parse. The values are the ones the row had when it was deleted, including updates on the deleting branch. A missing row, or a read clause that also does not parse, keeps the conflict.

## Why a shared prefix is not a conflict

Each merge is replayed from its own fork point. Commits that both parents already contain are applied once, in order. A later commit on one side replaces the earlier value. It is not compared with that earlier value as if the two were concurrent. An open VSA choice stays open until a later commit writes the same row.

## What is not claimed

The checker does not derive predicates from a query planner. A writer either attaches a predicate or the SQL extractor attaches the WHERE text of one SELECT or DELETE. Baselines that have no predicate, and Dolt's declared constraints, are measured separately. Precision on the labelled suite is an empirical number, not part of the no-false-negative claim.
