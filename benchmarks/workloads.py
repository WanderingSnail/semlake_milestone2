"""Synthetic workloads (W1, W1S, W2, W3, W3C, W4) and the M4 scan.

Each scenario forks two branches from one published base snapshot.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass

from semlake.metadata import Repository, Schema, SchemaField, Snapshot
from semlake.storage import MemoryObjectStore
from semlake.verification import Predicate

# Row id for the single appended row in W4. It is far above any generated id.
APPENDED_ROW_ID = 10**12


@dataclass
class Scenario:
    workload: str
    repo: Repository
    base_id: str
    left_id: str
    right_id: str
    manifest_depth: int
    contention: float
    notes: str


def _scenario(workload: str, repo: Repository, base: Snapshot, left: Snapshot, right: Snapshot,
              manifest_depth: int, notes: str) -> Scenario:
    return Scenario(
        workload=workload,
        repo=repo,
        base_id=base.snapshot_id,
        left_id=left.snapshot_id,
        right_id=right.snapshot_id,
        manifest_depth=manifest_depth,
        contention=0.0,
        notes=notes,
    )


def _user_schema() -> Schema:
    return Schema(
        (
            SchemaField("id", "int64"),
            SchemaField("kind", "string"),
            SchemaField("value", "int64"),
        )
    )


def _amount_schema() -> Schema:
    return Schema((SchemaField("id", "int64"), SchemaField("amount", "double")))


def workload_w1(n_rows: int = 1500) -> Scenario:
    """Update disjoint rows of one Parquet file on each branch."""
    repo = Repository(MemoryObjectStore())
    rows = {i: {"id": i, "kind": "user", "value": i} for i in range(n_rows)}
    base = repo.create_table("main", _user_schema(), rows=rows, rows_per_file=n_rows)
    low = 0
    high = n_rows // 2
    left_updates = {i: {"value": rows[i]["value"] + 1} for i in range(low, low + 10)}
    right_updates = {i: {"value": rows[i]["value"] + 2} for i in range(high, high + 10)}
    left = repo.apply(base, "A", updates=left_updates, message=f"update rows {low}-{low + 9}")
    right = repo.apply(base, "B", updates=right_updates, message=f"update rows {high}-{high + 9}")
    return _scenario("W1", repo, base, left, right, 1, "same file, disjoint rows")


def workload_w1s(n_files: int, file_size: int) -> Scenario:
    """Change a different half of the files on each branch. CoW has to rewrite all of them."""
    repo = Repository(MemoryObjectStore())
    base = repo.create_table("main", _amount_schema(), n_files=n_files, file_size=file_size)
    file_ids = sorted(base.files)
    mid = len(file_ids) // 2
    left = repo.mark_files_changed(base, "A", file_ids[:mid], "change the first half")
    right = repo.mark_files_changed(base, "B", file_ids[mid:], "change the second half")
    return _scenario(
        "W1S", repo, base, left, right, n_files,
        "disjoint files; copy-on-write rewrites each changed file",
    )


def workload_w2(n_files: int, rows_per_file: int, rhos: list[float], trials: int, seed: int = 7):
    """Build one base table and ``trials`` fork pairs at each contention rate.

    Return the repository, the base id, and ``{rho: [(left_id, right_id), ...]}``.
    """
    repo = Repository(MemoryObjectStore())
    n_rows = n_files * rows_per_file
    rows = {i: {"id": i, "kind": "user", "value": i % 97} for i in range(n_rows)}
    base = repo.create_table("main", _user_schema(), rows=rows, rows_per_file=rows_per_file)
    pairs: dict[float, list[tuple[str, str]]] = {}
    for rho in rhos:
        rng = random.Random(seed + int(rho * 10000))
        pairs[rho] = []
        for _ in range(trials):
            left_updates, right_updates = _w2_updates(rng, rho, n_files, rows_per_file, rows)
            left = repo.apply(base, "A", updates=left_updates, message="w2 left")
            right = repo.apply(base, "B", updates=right_updates, message="w2 right")
            pairs[rho].append((left.snapshot_id, right.snapshot_id))
    return repo, base.snapshot_id, pairs


def _w2_updates(rng: random.Random, rho: float, n_files: int, rows_per_file: int, rows: dict):
    """With probability rho both branches set the same cell. Otherwise they use two files."""
    if rho > 0 and rng.random() < rho:
        rid = rng.randrange(n_files) * rows_per_file
        return (
            {rid: {"value": rows[rid]["value"] + 1}},
            {rid: {"value": rows[rid]["value"] + 2}},
        )
    left_file, right_file = rng.sample(range(n_files), 2)
    left_rid = left_file * rows_per_file
    right_rid = right_file * rows_per_file
    return (
        {left_rid: {"value": rows[left_rid]["value"] + 1}},
        {right_rid: {"value": rows[right_rid]["value"] + 2}},
    )


def workload_w3(n_users: int = 400, n_vip: int = 80, control: bool = False) -> Scenario:
    """Run a DELETE on branch A and an aggregate over a WHERE clause on branch B.

    Both commits record only their SQL WHERE text, and the checker compares
    the two clauses. In the control case A deletes a different segment, so
    the merge has to stay clean.
    """
    repo = Repository(MemoryObjectStore())
    n_spam = n_vip // 2
    rows = {}
    for i in range(n_users):
        if i < n_vip:
            kind = "vip"
        elif i < n_vip + n_spam:
            kind = "spam"
        else:
            kind = "user"
        rows[i] = {"id": i, "kind": kind, "value": 10 + (i % 7)}
    base = repo.create_table("main", _user_schema(), rows=rows, rows_per_file=n_users)

    target = "spam" if control else "vip"
    delete_where = f"kind = '{target}'"
    deleted = [rid for rid, row in rows.items() if row["kind"] == target]
    left = repo.apply(
        base,
        "A",
        deletes=deleted,
        predicates=(Predicate("privacy_delete", "deletes_where", where=delete_where),),
        message=f"DELETE FROM users WHERE {delete_where}",
    )

    read_where = "kind = 'vip' AND value >= 10"
    total = sum(row["value"] for row in rows.values() if row["kind"] == "vip" and row["value"] >= 10)
    report_id = n_users + 1
    right = repo.apply(
        base,
        "B",
        inserts={report_id: {"id": report_id, "kind": "report", "value": total}},
        predicates=(
            Predicate(
                "vip_retention_report",
                "reads_where",
                where=read_where,
                detail="SELECT sum(value) FROM users WHERE " + read_where,
            ),
        ),
        message="write retention report",
    )
    if control:
        return _scenario("W3C", repo, base, left, right, 2,
                         "control: delete and aggregate WHERE clauses are disjoint")
    return _scenario("W3", repo, base, left, right, 2,
                     "delete and aggregate touch different files; WHERE clauses overlap")


def workload_w4(n_files: int, file_size: int) -> Scenario:
    """Add a column on branch A and append one file on branch B, over N files."""
    repo = Repository(MemoryObjectStore())
    schema = _amount_schema()
    base = repo.create_table("main", schema, n_files=n_files, file_size=file_size)
    widened = Schema(schema.fields + (SchemaField("promo_flag", "int64", True),))
    left = repo.apply(base, "A", schema=widened, message="add promo_flag")
    right = repo.apply(
        base,
        "B",
        inserts={APPENDED_ROW_ID: {"id": APPENDED_ROW_ID, "amount": 1.0}},
        message="append one file",
    )
    return _scenario("W4", repo, base, left, right, n_files,
                     "schema add is metadata-only; one appended file")


def measure_scan(n_rows: int, repeats: int) -> dict:
    """Time a Q6-style scan on a compacted table and on the same table with a DV mask.

    Deleted positions come from a roaring bitmap (every 20th row), turned into
    a boolean mask before timing starts. ``scan_base_s`` is the compacted
    table, ``scan_nodv_s`` reads every row with no mask, and ``scan_dv_s``
    reads every row plus the mask column and drops the masked rows. DuckDB
    has no Python hook for an external selection vector, so the mask has to
    be a real column that every row reads.
    """
    import duckdb
    import numpy as np
    import pyarrow as pa
    from pyroaring import BitMap

    deleted = BitMap(range(0, n_rows, 20))
    live = np.ones(n_rows, dtype=np.bool_)
    live[np.fromiter(deleted, dtype=np.int64, count=len(deleted))] = False

    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute(
        """
        CREATE TABLE lineitem AS
        SELECT
            i AS row_id,
            DATE '1993-01-01' + CAST(i % 900 AS INTEGER) AS l_shipdate,
            CASE i % 5
                WHEN 0 THEN 0.01
                WHEN 1 THEN 0.05
                WHEN 2 THEN 0.06
                WHEN 3 THEN 0.07
                ELSE 0.10
            END AS l_discount,
            (i % 50) + 1 AS l_quantity,
            (i % 1000) / 10.0 AS l_extendedprice,
            i % 1000 AS l_partkey,
            (i % 7) * 0.01 AS l_tax
        FROM range(0, ?) t(i)
        """,
        [n_rows],
    )
    con.register(
        "dv_mask",
        pa.table(
            {
                "row_id": pa.array(np.arange(n_rows, dtype=np.int64)),
                "live": pa.array(live),
            }
        ),
    )
    con.execute(
        """
        CREATE TABLE tagged AS
        SELECT lineitem.*, dv_mask.live
        FROM lineitem
        JOIN dv_mask USING (row_id)
        """
    )
    con.execute("CREATE TABLE compact AS SELECT * EXCLUDE (live) FROM tagged WHERE live")
    con.execute("CREATE TABLE nodv AS SELECT * EXCLUDE (live) FROM tagged")
    q6 = """
        SELECT sum(l_extendedprice * l_discount * (1 + l_tax))
        FROM {table}
        WHERE l_shipdate >= DATE '1994-01-01'
          AND l_shipdate < DATE '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24
          {extra}
    """
    base_s, nodv_s, dv_s = _median_query_times(
        con,
        [
            q6.format(table="compact", extra=""),
            q6.format(table="nodv", extra=""),
            q6.format(table="tagged", extra="AND live"),
        ],
        repeats,
    )
    return {
        "scan_base_s": base_s,
        "scan_nodv_s": nodv_s,
        "scan_dv_s": dv_s,
        "scan_overhead": (dv_s - base_s) / base_s if base_s > 0 else 0.0,
        "deleted_rows": len(deleted),
        "scan_rows": n_rows,
    }


def _median_query_times(con, queries: list[str], repeats: int, batch: int = 20) -> list[float]:
    """Return the median time per query for each query.

    The queries take turns inside every round, so drift during the run is
    spread across all of them.
    """
    for sql in queries:
        con.execute(sql).fetchall()
    samples: list[list[float]] = [[] for _ in queries]
    for _ in range(repeats):
        for index, sql in enumerate(queries):
            started = time.perf_counter()
            for _ in range(batch):
                con.execute(sql).fetchall()
            samples[index].append((time.perf_counter() - started) / batch)
    return [sorted(times)[len(times) // 2] for times in samples]
