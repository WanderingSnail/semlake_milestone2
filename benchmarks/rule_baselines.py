"""Rule models of B1R (Delta-style row concurrency) and B6 (serializable snapshot isolation).

Both take the same arguments as ``three_way_merge`` and return a ``MergeResult``,
so the labelled suite, the TPC-H pairs and the selectivity sweep can score them
next to SemLake on identical branch pairs.

B1R follows Delta Lake's documented conflict checks for row-level concurrency
with deletion vectors: two commits that change the same row conflict, and a
transaction that read a file conflicts when a concurrent commit deleted or
changed rows in that file (``ConcurrentDeleteReadException``). A branch reads
the files holding at least one base row that matches its ``reads_where``
clause (the best case for B1R: perfect data skipping), plus the files of its
``rows_live`` rows. Domain and foreign-key rules are outside these checks.

B6 treats each branch as one serializable-snapshot-isolation transaction on
the fork snapshot. Rows read are the base rows that match ``reads_where``,
plus ``rows_live`` rows and referenced foreign-key parents. Writes are the
rows each branch updated, deleted or inserted. Two writes to one row abort the
second committer. A read of a row (or a predicate range that a new row falls
into) that the other branch wrote is a read-write antidependency. With two
transactions, SSI aborts only when the antidependencies run both ways.

Neither model is the real system. They encode the rule each system uses to
decide a lost race, so the comparison is about rules, not engines.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa

from semlake.baselines import WriteConflictException
from semlake.merge import MergeResult, load_delta


def _changes(repo, base, tip) -> tuple[set[int], set[int], dict[int, dict]]:
    """Rows the tip updated or deleted (existing at base), and rows it inserted."""
    base_rows = base.rows
    tip_rows = tip.rows
    written: set[int] = set()
    for rid in base_rows:
        if rid not in tip_rows or tip_rows[rid] != base_rows[rid]:
            written.add(rid)
    inserted = {rid: tip_rows[rid] for rid in tip_rows if rid not in base_rows}
    deleted = {rid for rid in base_rows if rid not in tip_rows}
    return written, deleted, inserted


def rows_matching(where: str, rows) -> set[int] | None:
    """Row ids whose values satisfy ``where``, evaluated by DuckDB. None if it cannot run."""
    items = list(rows.items())
    if not items:
        return set()
    columns = sorted({key for _rid, row in items for key in row})
    data = {"__rid": [rid for rid, _row in items]}
    for col in columns:
        data[col] = [row.get(col) for _rid, row in items]
    try:
        table = pa.table(data)
        con = duckdb.connect()
        con.register("t", table)
        found = con.execute(f"SELECT __rid FROM t WHERE {where}").fetchall()
        con.close()
    except Exception:  # noqa: BLE001 - an unknown column or syntax means "could read anything"
        return None
    return {int(row[0]) for row in found}


def _reads(base, predicates, include_fk: bool) -> tuple[set[int], list[str], bool]:
    """Rows a branch read at the fork, the WHERE clauses it read, and whether it read everything."""
    rows: set[int] = set()
    clauses: list[str] = []
    everything = False
    for pred in predicates:
        if pred.kind == "reads_where":
            clauses.append(pred.where)
            hit = rows_matching(pred.where, base.rows)
            if hit is None:
                everything = True
            else:
                rows |= hit
        elif pred.kind == "rows_live" or (include_fk and pred.kind == "foreign_key"):
            rows |= set(pred.row_ids)
    return rows, clauses, everything


def _deltas(repo, store, left_id: str, right_id: str):
    base_id = repo.graph.lca(left_id, right_id)
    base = repo.snapshots[base_id]
    sides = {}
    for name, tip_id in (("A", left_id), ("B", right_id)):
        tip = repo.snapshots[tip_id]
        written, deleted, inserted = _changes(repo, base, tip)
        delta = load_delta(repo, store, base_id, tip_id)
        sides[name] = {"written": written, "deleted": deleted, "inserted": inserted,
                       "predicates": delta.predicates}
    return base, sides


def b1r_merge(repo, left_id: str, right_id: str, store) -> MergeResult:
    base, sides = _deltas(repo, store, left_id, right_id)
    same_rows = sides["A"]["written"] & sides["B"]["written"]
    if same_rows:
        raise WriteConflictException(sorted(base.row_file[r] for r in same_rows))
    for reader, writer in (("A", "B"), ("B", "A")):
        rows, _clauses, everything = _reads(base, sides[reader]["predicates"], include_fk=False)
        read_files = set(base.files) if everything else {base.row_file[r] for r in rows if r in base.row_file}
        touched = {base.row_file[r] for r in sides[writer]["written"] if r in base.row_file}
        hit = read_files & touched
        if hit:
            return MergeResult(
                system="b1r",
                status="semantic_conflict",
                message=f"ConcurrentDeleteReadException: branch {writer} changed {len(hit)} file(s) branch {reader} read",
            )
    return MergeResult(system="b1r", status="clean", message="")


def ssi_merge(repo, left_id: str, right_id: str, store) -> MergeResult:
    base, sides = _deltas(repo, store, left_id, right_id)
    same_rows = sides["A"]["written"] & sides["B"]["written"]
    if same_rows:
        raise WriteConflictException(sorted(base.row_file.get(r, "?") for r in same_rows))
    edges = set()
    for reader, writer in (("A", "B"), ("B", "A")):
        rows, clauses, everything = _reads(base, sides[reader]["predicates"], include_fk=True)
        written = sides[writer]["written"]
        hit = everything and bool(written or sides[writer]["inserted"])
        hit = hit or bool(rows & written)
        if not hit and sides[writer]["inserted"]:
            # A new row inside a range the reader scanned is a phantom: also an rw-antidependency.
            for clause in clauses:
                matched = rows_matching(clause, sides[writer]["inserted"])
                if matched is None or matched:
                    hit = True
                    break
        if hit:
            edges.add((reader, writer))
    if {("A", "B"), ("B", "A")} <= edges:
        return MergeResult(system="ssi", status="semantic_conflict",
                           message="serialization failure: rw-antidependencies in both directions")
    return MergeResult(system="ssi", status="clean", message=f"rw edges {sorted(edges)}")
