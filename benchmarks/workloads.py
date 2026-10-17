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


# (layout, density). "w1" deletes 1% of the first row group and leaves the rest clean.
SCAN_LAYOUTS = (
    ("uniform", 0.001),
    ("uniform", 0.01),
    ("uniform", 0.05),
    ("w1", 0.01),
)


def measure_scan(n_rows: int, repeats: int, processes: int = 1, group_rows: int = 100_000) -> list[dict]:
    """Time a Q6-style scan over Parquet, once per delete layout.

    Rows are written in ``row_id`` order. The base Parquet files are not
    rewritten. A row group with an empty deletion vector is scanned as it
    is. A dirty group is joined, by position, to a one-column mask file.
    Each fresh process builds its own files, and the processes run one after
    another. The ``w1`` layout deletes 1% of a single row group.
    """
    width = max(1, min(group_rows, n_rows // 5 if n_rows >= 5 else n_rows))
    tasks = [
        {"n_rows": n_rows, "repeats": repeats, "group_rows": width, "layout": layout, "density": density}
        for layout, density in SCAN_LAYOUTS
        for _ in range(max(1, processes))
    ]
    if processes <= 1:
        samples = [_scan_once(task) for task in tasks]
    else:
        samples = [_scan_in_process(task) for task in tasks]
    grouped: dict[tuple, list[dict]] = {}
    for sample in samples:
        grouped.setdefault((sample["layout"], sample["density"]), []).append(sample)
    return [_summarize_scan(grouped[key]) for key in ((layout, density) for layout, density in SCAN_LAYOUTS)]


def _scan_in_process(task: dict) -> dict:
    """Run one timing sample in its own process, after the previous sample has exited."""
    import multiprocessing

    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    proc = ctx.Process(target=_scan_worker, args=(task, queue))
    proc.start()
    try:
        result = queue.get(timeout=600)
    finally:
        proc.join()
    return result


def _scan_worker(task: dict, queue) -> None:
    queue.put(_scan_once(task))


def _scan_once(task: dict) -> dict:
    """Build one Parquet table in this process and return median scan times."""
    import datetime
    import tempfile
    from pathlib import Path

    import duckdb
    import numpy as np
    import pyarrow as pa

    n_rows = task["n_rows"]
    group_rows = task["group_rows"]
    layout = task["layout"]
    density = task["density"]
    deleted = _delete_positions(n_rows, group_rows, layout, density)
    deleted_set = set(deleted)
    row_id = np.arange(n_rows, dtype=np.int64)
    origin = (datetime.date(1993, 1, 1) - datetime.date(1970, 1, 1)).days
    ship_days = np.asarray(origin + (row_id % 900), dtype=np.int32)
    columns = {
        "row_id": row_id,
        "l_shipdate": pa.array(ship_days, type=pa.int32()).cast(pa.date32()),
        "l_discount": np.take(np.array([0.01, 0.05, 0.06, 0.07, 0.10]), row_id % 5),
        "l_quantity": (row_id % 50) + 1,
        "l_extendedprice": (row_id % 1000) / 10.0,
        "l_partkey": row_id % 1000,
        "l_tax": (row_id % 7) * 0.01,
    }
    n_groups = (n_rows + group_rows - 1) // group_rows
    dirty_groups = {pos // group_rows for pos in deleted}
    clean_idx = []
    dirty_idx = []
    for group in range(n_groups):
        start = group * group_rows
        stop = min(n_rows, start + group_rows)
        chosen = np.arange(start, stop)
        if group in dirty_groups:
            dirty_idx.append(chosen)
        else:
            clean_idx.append(chosen)
    live_idx = np.array([pos for pos in range(n_rows) if pos not in deleted_set], dtype=np.int64)

    with tempfile.TemporaryDirectory(prefix="semlake-m4-") as tmp:
        root = Path(tmp)
        _write_scan_file(root / "compact.parquet", columns, live_idx, group_rows)
        _write_scan_file(root / "nodv.parquet", columns, np.arange(n_rows, dtype=np.int64), group_rows)
        parts = []
        if clean_idx:
            idx = np.concatenate(clean_idx)
            _write_scan_file(root / "clean.parquet", columns, idx, group_rows)
            parts.append(f"SELECT * FROM read_parquet('{(root / 'clean.parquet').as_posix()}')")
        if dirty_idx:
            idx = np.concatenate(dirty_idx)
            live = np.array([pos not in deleted_set for pos in idx])
            _write_scan_file(root / "dirty.parquet", columns, idx, group_rows)
            _write_mask(root / "mask.parquet", live)
            data_path = (root / "dirty.parquet").as_posix()
            mask_path = (root / "mask.parquet").as_posix()
            parts.append(
                "SELECT * EXCLUDE (live) FROM "
                f"read_parquet('{data_path}') POSITIONAL JOIN read_parquet('{mask_path}') "
                "WHERE live"
            )
        masked_from = " UNION ALL ".join(parts)
        q6 = """
            SELECT sum(l_extendedprice * l_discount * (1 + l_tax))
            FROM ({src}) t
            WHERE l_shipdate >= DATE '1994-01-01'
              AND l_shipdate < DATE '1995-01-01'
              AND l_discount BETWEEN 0.05 AND 0.07
              AND l_quantity < 24
        """
        con = duckdb.connect()
        con.execute("SET threads=1")
        base_s, nodv_s, dv_s = _median_query_times(
            con,
            [
                q6.format(src=f"SELECT * FROM read_parquet('{(root / 'compact.parquet').as_posix()}')"),
                q6.format(src=f"SELECT * FROM read_parquet('{(root / 'nodv.parquet').as_posix()}')"),
                q6.format(src=masked_from),
            ],
            task["repeats"],
        )
    overhead = (dv_s - base_s) / base_s if base_s > 0 else 0.0
    return {
        "layout": layout,
        "density": density,
        "scan_base_s": base_s,
        "scan_nodv_s": nodv_s,
        "scan_dv_s": dv_s,
        "scan_overhead": overhead,
        "deleted_rows": len(deleted),
        "scan_rows": n_rows,
        "clean_groups": n_groups - len(dirty_groups),
        "groups": n_groups,
    }


def _delete_positions(n_rows: int, group_rows: int, layout: str, density: float) -> list[int]:
    if layout == "w1":
        count = max(1, round(group_rows * density))
        return list(range(min(count, n_rows)))
    step = max(1, round(1 / density))
    return list(range(0, n_rows, step))


def _write_mask(path, live) -> None:
    """Write a one-column mask aligned with a dirty data file. The data file stays unchanged."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.table({"live": pa.array(live)}), path, compression="none")


def _write_scan_file(path, columns: dict, idx, group_rows: int, live=None) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    names = list(columns)
    arrays = []
    for name in names:
        values = columns[name]
        arrays.append(values.take(idx) if isinstance(values, pa.Array) else pa.array(values[idx]))
    if live is not None:
        names.append("live")
        arrays.append(pa.array(live))
    table = pa.Table.from_arrays(arrays, names=names)
    pq.write_table(table, path, compression="none", row_group_size=group_rows)


def _summarize_scan(samples: list[dict]) -> dict:
    """Median of the per-process medians, plus a 95% interval on the overhead."""
    import statistics

    first = samples[0]
    summary = dict(first)
    for key in ("scan_base_s", "scan_nodv_s", "scan_dv_s", "scan_overhead"):
        ordered = sorted(sample[key] for sample in samples)
        summary[key] = ordered[len(ordered) // 2]
    overs = [sample["scan_overhead"] for sample in samples]
    if len(overs) >= 2:
        mean = statistics.fmean(overs)
        half = 1.96 * statistics.stdev(overs) / (len(overs) ** 0.5)
        summary["overhead_low"] = mean - half
        summary["overhead_high"] = mean + half
    else:
        summary["overhead_low"] = summary["scan_overhead"]
        summary["overhead_high"] = summary["scan_overhead"]
    summary["processes"] = len(samples)
    return summary


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


def _fresh_users():
    """A 10-row table with vip and user kinds and values on both sides of zero."""
    repo = Repository(MemoryObjectStore())
    rows = {
        i: {"id": i, "kind": "vip" if i < 4 else "user", "value": i - 2}
        for i in range(10)
    }
    base = repo.create_table("main", _user_schema(), rows=rows, rows_per_file=10)
    return repo, base


def semantic_cases() -> list[dict]:
    """Labelled cases. ``expect`` is 1 when the predicates really do conflict."""
    cases = []

    def add(name: str, expect: int, left_kw: dict, right_kw: dict) -> None:
        repo, base = _fresh_users()
        for kw in (left_kw, right_kw):
            # A DELETE removes exactly the rows its WHERE clause matches, so derive them.
            clauses = [p.where for p in kw.get("predicates", ()) if p.kind == "deletes_where"]
            if clauses and "deletes" in kw:
                from benchmarks.rule_baselines import rows_matching

                kw["deletes"] = sorted(set().union(*(rows_matching(c, base.rows) or set() for c in clauses)))
        left = repo.apply(base, "A", **left_kw)
        right = repo.apply(base, "B", **right_kw)
        cases.append({
            "name": name,
            "expect": expect,
            "repo": repo,
            "left_id": left.snapshot_id,
            "right_id": right.snapshot_id,
        })

    delete = Predicate
    add("eq overlap", 1,
        {"deletes": [0, 1, 2, 3], "predicates": (delete("d", "deletes_where", where="kind = 'vip'"),)},
        {"predicates": (delete("r", "reads_where", where="kind = 'vip'"),)})
    add("range overlap", 1,
        {"deletes": [0, 1, 2], "predicates": (delete("d", "deletes_where", where="value < 3"),)},
        {"predicates": (delete("r", "reads_where", where="value >= 0"),)})
    add("between overlap", 1,
        {"deletes": [2, 3, 4], "predicates": (delete("d", "deletes_where", where="value BETWEEN 0 AND 4"),)},
        {"predicates": (delete("r", "reads_where", where="value >= 4"),)})
    add("in overlap", 1,
        {"deletes": [0], "predicates": (delete("d", "deletes_where", where="kind IN ('vip', 'other')"),)},
        {"predicates": (delete("r", "reads_where", where="kind = 'vip'"),)})
    add("and overlap", 1,
        {"deletes": [2, 3], "predicates": (delete("d", "deletes_where", where="kind = 'vip' AND value >= 0"),)},
        {"predicates": (delete("r", "reads_where", where="kind = 'vip' AND value < 10"),)})
    add("domain violation", 1,
        {"predicates": (delete("non_negative", "domain", where="value >= 0"),)},
        {"updates": {5: {"value": -5}}})
    add("foreign key", 1,
        {"deletes": [1]},
        {"inserts": {100: {"id": 100, "kind": "order", "value": 1}},
         "predicates": (delete("user_fk", "foreign_key", frozenset({1}), "orders.user_id -> users.id"),)})
    add("rows live", 1,
        {"deletes": [1, 2]},
        {"predicates": (delete("keep", "rows_live", frozenset({1}), "report"),)})
    add("boundary overlap", 1,
        {"deletes": [5], "predicates": (delete("d", "deletes_where", where="value <= 3"),)},
        {"predicates": (delete("r", "reads_where", where="value >= 3"),)})
    add("not-equal overlap", 1,
        {"deletes": [0], "predicates": (delete("d", "deletes_where", where="kind != 'user'"),)},
        {"predicates": (delete("r", "reads_where", where="kind = 'vip'"),)})
    add("unparsed not that overlaps", 1,
        {"deletes": [0], "predicates": (delete("d", "deletes_where", where="NOT kind = 'user'"),)},
        {"predicates": (delete("r", "reads_where", where="kind = 'vip'"),)})
    add("high range overlap", 1,
        {"deletes": [6], "predicates": (delete("d", "deletes_where", where="value > 0"),)},
        {"predicates": (delete("r", "reads_where", where="value > 2"),)})
    add("disjoint kinds", 0,
        {"deletes": [4, 5], "predicates": (delete("d", "deletes_where", where="kind = 'user'"),)},
        {"predicates": (delete("r", "reads_where", where="kind = 'vip'"),)})
    add("disjoint ranges", 0,
        {"deletes": [0], "predicates": (delete("d", "deletes_where", where="value < 0"),)},
        {"predicates": (delete("r", "reads_where", where="value >= 0"),)})
    add("between past the end", 0,
        {"deletes": [2], "predicates": (delete("d", "deletes_where", where="value BETWEEN 0 AND 2"),)},
        {"predicates": (delete("r", "reads_where", where="value > 2"),)})
    add("in miss", 0,
        {"deletes": [4], "predicates": (delete("d", "deletes_where", where="kind IN ('nope', 'nada')"),)},
        {"predicates": (delete("r", "reads_where", where="kind = 'vip'"),)})
    add("domain holds", 0,
        {"predicates": (delete("non_negative", "domain", where="value >= 0"),)},
        {"updates": {6: {"value": 4}}})
    add("foreign key miss", 0,
        {"deletes": [8]},
        {"inserts": {101: {"id": 101, "kind": "order", "value": 1}},
         "predicates": (delete("user_fk", "foreign_key", frozenset({1}), "orders.user_id -> users.id"),)})
    add("rows live miss", 0,
        {"deletes": [8]},
        {"predicates": (delete("keep", "rows_live", frozenset({1}), "report"),)})
    add("disjoint updates", 0,
        {"updates": {0: {"kind": "vip"}}},
        {"updates": {1: {"value": 9}}})
    widened = Schema(_user_schema().fields + (SchemaField("flag", "int64", True),))
    add("column add", 0,
        {"schema": widened},
        {"inserts": {50: {"id": 50, "kind": "user", "value": 1}}})
    add("or is not a real overlap", 0,
        {"deletes": [0], "predicates": (delete("d", "deletes_where", where="kind = 'vip' OR kind = 'other'"),)},
        {"predicates": (delete("r", "reads_where", where="kind = 'user'"),)})
    add("not the same value", 0,
        {"deletes": [4], "predicates": (delete("d", "deletes_where", where="NOT kind = 'vip'"),)},
        {"predicates": (delete("r", "reads_where", where="kind = 'vip'"),)})
    add("far ranges", 0,
        {"deletes": [9], "predicates": (delete("d", "deletes_where", where="value > 100"),)},
        {"predicates": (delete("r", "reads_where", where="value < 0"),)})

    # score is 0 on every base row, so a table-level CHECK (score >= 0) can be
    # installed. The other branch then assigns -1.
    scored = Schema(_user_schema().fields + (SchemaField("score", "int64", True),))
    repo = Repository(MemoryObjectStore())
    scored_rows = {
        i: {"id": i, "kind": "vip" if i < 4 else "user", "value": i - 2, "score": 0}
        for i in range(10)
    }
    base = repo.create_table("main", scored, rows=scored_rows, rows_per_file=10)
    left = repo.apply(
        base, "A", predicates=(delete("non_negative_score", "domain", where="score >= 0"),)
    )
    right = repo.apply(base, "B", updates={5: {"score": -1}})
    cases.append({
        "name": "check on a satisfied column",
        "expect": 1,
        "repo": repo,
        "left_id": left.snapshot_id,
        "right_id": right.snapshot_id,
    })
    cases.extend(_tpch_sql_cases())
    return cases


def _tpch_sql_cases() -> list[dict]:
    """Two lineitem cases whose predicates come from the SQL text."""
    from semlake.sql_predicates import predicates_from_sql

    schema = Schema(
        (
            SchemaField("id", "int64"),
            SchemaField("l_shipdate", "int64"),
            SchemaField("l_discount", "double"),
            SchemaField("l_quantity", "int64"),
        )
    )
    q6 = (
        "SELECT SUM(l_extendedprice * l_discount) FROM lineitem "
        "WHERE l_shipdate >= 19930101 AND l_shipdate < 19940101 "
        "AND l_discount >= 0.05 AND l_quantity < 24"
    )
    built = []
    for name, expect, delete_sql, deleted in (
        ("tpch q6 overlap", 1, "DELETE FROM lineitem WHERE l_shipdate < 19930201", None),
        ("tpch q6 before the window", 0, "DELETE FROM lineitem WHERE l_shipdate < 19920101", None),
    ):
        repo = Repository(MemoryObjectStore())
        rows = {
            i: {"id": i, "l_shipdate": 19930101 + i * 30, "l_discount": 0.06, "l_quantity": 10}
            for i in range(8)
        }
        base = repo.create_table("main", schema, rows=rows, rows_per_file=8)
        from benchmarks.rule_baselines import rows_matching

        clause = delete_sql.split(" WHERE ", 1)[1]
        deleted = sorted(rows_matching(clause, base.rows))  # what the DELETE really removes
        left = repo.apply(base, "rf1", deletes=deleted, predicates=predicates_from_sql(delete_sql, name="rf1"))
        right = repo.apply(base, "q6", predicates=predicates_from_sql(q6, name="q6"))
        built.append({
            "name": name,
            "expect": expect,
            "repo": repo,
            "left_id": left.snapshot_id,
            "right_id": right.snapshot_id,
        })
    return built


def run_swarm(n_writers: int, rounds: int, rho: float, n_rows: int = 32, seed: int = 3) -> list[dict]:
    """N writers commit to main with a compare-and-swap on the branch head.

    Each row lives in its own file, so file-level OCC aborts only when two
    writers touch the same row. A lost race is a 3-way merge for SemLake.
    Throughput counts applied writes. A merge that still holds a VSA choice
    is parked and is not counted in ``tps``. B1 commits the file-level merge
    when the files differ, and reapplies the intended value from the new head
    when they do not.
    """
    from semlake.baselines import WriteConflictException, file_level_occ
    from semlake.merge import three_way_merge

    results = []
    for system in ("semlake", "b1"):
        repo = Repository(MemoryObjectStore())
        rows = {i: {"id": i, "kind": "user", "value": i} for i in range(n_rows)}
        repo.create_table("main", _user_schema(), rows=rows, rows_per_file=1)
        rng = random.Random(seed + n_writers + int(rho * 100))
        samples: list[float] = []
        applied = 0
        parked = 0
        attempts = 0
        started = time.perf_counter()
        for _round in range(rounds):
            head = repo.heads["main"]
            jobs = []
            for writer in range(n_writers):
                parent = repo.snapshots[head]
                rid = 0 if rng.random() < rho else (writer % (n_rows - 1)) + 1
                intended = parent.rows[rid]["value"] + 1 + writer
                local = repo.apply(parent, f"w{writer}", updates={rid: {"value": intended}})
                jobs.append((rid, intended, local.snapshot_id, time.perf_counter()))
            for rid, intended, current_id, t0 in jobs:
                expected = head
                pending_parked = False
                guard = 0
                while guard < n_writers + 3:
                    guard += 1
                    attempts += 1
                    if repo.cas_head("main", expected, current_id):
                        if pending_parked:
                            parked += 1
                        else:
                            applied += 1
                        samples.append(time.perf_counter() - t0)
                        break
                    new_head = repo.heads["main"]
                    if system == "semlake":
                        merged = three_way_merge(repo, current_id, new_head, repo.store)
                        if merged.status in ("clean", "vsa") and merged.snapshot_id:
                            current_id = merged.snapshot_id
                            pending_parked = merged.status == "vsa"
                            expected = new_head
                            continue
                    else:
                        try:
                            merged = file_level_occ(repo, current_id, new_head, repo.store.clone())
                        except WriteConflictException:
                            merged = None
                        if merged is not None and merged.snapshot_id:
                            current_id = merged.snapshot_id
                            pending_parked = False
                            expected = new_head
                            continue
                    parent = repo.snapshots[new_head]
                    rebuilt = repo.apply(parent, "retry", updates={rid: {"value": intended}})
                    current_id = rebuilt.snapshot_id
                    pending_parked = False
                    expected = new_head
        elapsed = time.perf_counter() - started
        successes = applied + parked
        ordered = sorted(samples) or [0.0]
        p99 = ordered[min(len(ordered) - 1, max(0, int(round(0.99 * (len(ordered) - 1)))))]
        results.append({
            "system": system,
            "writers": n_writers,
            "contention": rho,
            "tps": applied / elapsed if elapsed else 0.0,
            "retries_per_commit": (attempts - successes) / successes if successes else 0.0,
            "p99_s": p99,
            "successes": successes,
            "applied": applied,
            "parked": parked,
            "rounds": rounds,
        })
    return results


def measure_history(marks: list[int]) -> list[dict]:
    """Time LCA and a 3-way merge as the commit chain grows."""
    from semlake.merge import three_way_merge

    repo = Repository(MemoryObjectStore())
    rows = {i: {"id": i, "kind": "user", "value": i} for i in range(4)}
    head = repo.create_table("main", _user_schema(), rows=rows, rows_per_file=4)
    pending = sorted(set(marks))
    found = []
    while pending:
        left = repo.apply(head, "A", updates={0: {"value": head.rows[0]["value"] + 1}})
        right = repo.apply(head, "B", updates={1: {"value": head.rows[1]["value"] + 1}})
        before = len(repo.snapshots)
        if before < pending[0]:
            merged = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store)
            head = repo.snapshots[merged.snapshot_id]
            continue
        started = time.perf_counter()
        for _ in range(30):
            repo.graph.lca(left.snapshot_id, right.snapshot_id)
        lca_s = (time.perf_counter() - started) / 30
        # Time the merge on store copies so repeats do not grow the history.
        times = []
        for _ in range(5):
            scratch = repo.store.clone()
            started = time.perf_counter()
            three_way_merge(repo, left.snapshot_id, right.snapshot_id, scratch)
            times.append(time.perf_counter() - started)
        times.sort()
        merged = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store)
        head = repo.snapshots[merged.snapshot_id]
        found.append({
            "snapshots": before,
            "lca_s": lca_s,
            "merge_s": times[len(times) // 2],
            "merge_low_s": times[0],
            "merge_high_s": times[-1],
        })
        pending.pop(0)
    return found


def workload_tpch_refresh(n_rows: int = 200):
    """One TPC-H refresh-shaped delete against a Q6-shaped read.

    The refresh deletes rows in a ship-date window. The report reads a window
    that overlaps it. The table has 200 synthetic rows.
    """
    schema = Schema(
        (
            SchemaField("l_orderkey", "int64"),
            SchemaField("l_shipdate", "int64"),
            SchemaField("l_discount", "double"),
            SchemaField("l_quantity", "int64"),
            SchemaField("l_extendedprice", "double"),
        )
    )
    repo = Repository(MemoryObjectStore())
    rows = {
        i: {
            "l_orderkey": i,
            "l_shipdate": 19930101 + (i % 365),
            "l_discount": 0.05,
            "l_quantity": (i % 50) + 1,
            "l_extendedprice": float(i),
        }
        for i in range(n_rows)
    }
    from semlake.sql_predicates import predicates_from_sql

    base = repo.create_table("main", schema, rows=rows, rows_per_file=50)
    deleted = [i for i in range(n_rows) if rows[i]["l_shipdate"] < 19930201]
    refresh = repo.apply(
        base,
        "rf1",
        deletes=deleted,
        predicates=predicates_from_sql(
            "DELETE FROM lineitem WHERE l_shipdate < 19930201",
            name="rf1",
        ),
    )
    report = repo.apply(
        base,
        "q6",
        predicates=predicates_from_sql(
            "SELECT SUM(l_extendedprice * l_discount) FROM lineitem "
            "WHERE l_shipdate >= 19930101 AND l_shipdate < 19940101 "
            "AND l_discount >= 0.05 AND l_quantity < 24",
            name="q6",
        ),
    )
    return repo, refresh, report


def workload_branchbench_curation():
    """Two curation branches. Disjoint edits merge. The same cell stays open.

    The row shape follows BranchBench's curation workload. Neon, DoltgreSQL
    and the other systems in that benchmark are not started.
    """
    repo = Repository(MemoryObjectStore())
    rows = {i: {"id": i, "kind": "raw", "value": i} for i in range(16)}
    base = repo.create_table("main", _user_schema(), rows=rows, rows_per_file=4)
    left = repo.apply(base, "curate-a", updates={1: {"kind": "keep"}})
    right = repo.apply(base, "curate-b", updates={2: {"kind": "drop"}})
    clash = repo.apply(base, "curate-c", updates={1: {"kind": "drop"}})
    return repo, left, right, clash


def workload_branchbench_swe():
    """Two repair branches edit different paths. The same path stays open.

    The row shape follows BranchBench's software-engineering workload. The
    systems in that benchmark are not started.
    """
    repo = Repository(MemoryObjectStore())
    rows = {i: {"id": i, "kind": f"src/{i}.py", "value": 1} for i in range(8)}
    base = repo.create_table("main", _user_schema(), rows=rows, rows_per_file=4)
    left = repo.apply(base, "fix-a", updates={0: {"value": 2}})
    right = repo.apply(base, "fix-b", updates={1: {"value": 3}})
    clash = repo.apply(base, "fix-c", updates={0: {"value": 4}})
    return repo, left, right, clash
