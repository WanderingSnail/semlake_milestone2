"""Full TPC-H queries as declared reads, labelled by a result oracle (reviewers R1.2 and MC1).

    python benchmarks/tpch_full_queries.py              # SF 0.01
    python benchmarks/tpch_full_queries.py --sf 0.1
    python benchmarks/tpch_full_queries.py --quick      # 3 refreshes, smoke test

The 17 TPC-H queries that read ``lineitem`` are used with their full text
(joins, subqueries, grouping, ORDER BY and LIMIT; validation parameters), plus a
``count(*)`` and a ``SELECT *`` read. Each query is one branch that declares
the query as its read. The other branch is a refresh of ``lineitem``: a delete,
an update or an insert.

Ground truth does not come from any predicate. A pair is a conflict when the
query, run by DuckDB over all eight tables, returns a different result on the
refreshed ``lineitem`` than it returned at the fork. The other seven tables do
not change.

Dates are stored as ISO strings (``'1994-01-01'``), which order the same way as
dates, and decimals as doubles; the engine has no date or decimal type.
"""

from __future__ import annotations

import argparse
import datetime
import json
import math
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATA = ROOT / "benchmarks" / "data" / "tpch_full"
RESULTS = ROOT / "benchmarks" / "results"
ROWS_PER_FILE = 2000

LINEITEM = (
    ("id", "int64"), ("l_orderkey", "int64"), ("l_partkey", "int64"), ("l_suppkey", "int64"),
    ("l_linenumber", "int64"), ("l_quantity", "double"), ("l_extendedprice", "double"),
    ("l_discount", "double"), ("l_tax", "double"), ("l_returnflag", "string"), ("l_linestatus", "string"),
    ("l_shipdate", "string"), ("l_commitdate", "string"), ("l_receiptdate", "string"),
    ("l_shipinstruct", "string"), ("l_shipmode", "string"), ("l_comment", "string"),
)
COLUMNS = tuple(name for name, _kind in LINEITEM)
OTHER_TABLES = ("orders", "customer", "part", "supplier", "partsupp", "nation", "region")

_Q19_GROUP = ("(p_partkey = l_partkey AND p_brand = '{brand}' AND p_container IN ({boxes}) "
              "AND l_quantity >= {low} AND l_quantity <= {high} AND p_size BETWEEN 1 AND {size} "
              "AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON')")

QUERIES = {
    "Q1": """SELECT l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice),
        sum(l_extendedprice * (1 - l_discount)), sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
        avg(l_quantity), avg(l_extendedprice), avg(l_discount), count(*)
        FROM lineitem WHERE l_shipdate <= '1998-09-02'
        GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus""",
    "Q3": """SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey
          AND o_orderdate < '1995-03-15' AND l_shipdate > '1995-03-15'
        GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10""",
    "Q4": """SELECT o_orderpriority, count(*) AS order_count FROM orders
        WHERE o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01'
          AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
        GROUP BY o_orderpriority ORDER BY o_orderpriority""",
    "Q5": """SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey
          AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
          AND r_name = 'ASIA' AND o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01'
        GROUP BY n_name ORDER BY revenue DESC""",
    "Q6": """SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem
        WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24""",
    "Q7": """SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue FROM (
          SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, substr(l_shipdate, 1, 4) AS l_year,
                 l_extendedprice * (1 - l_discount) AS volume
          FROM supplier, lineitem, orders, customer, nation n1, nation n2
          WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
            AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
            AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
            AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31') AS shipping
        GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year""",
    "Q8": """SELECT o_year, sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share FROM (
          SELECT substr(o_orderdate, 1, 4) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation
          FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
          WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey
            AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
            AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
            AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31' AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
        GROUP BY o_year ORDER BY o_year""",
    "Q9": """SELECT nation, o_year, sum(amount) AS sum_profit FROM (
          SELECT n_name AS nation, substr(o_orderdate, 1, 4) AS o_year,
                 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
          FROM part, supplier, lineitem, partsupp, orders, nation
          WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
            AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
            AND p_name LIKE '%green%') AS profit
        GROUP BY nation, o_year ORDER BY nation, o_year DESC""",
    "Q10": """SELECT c_custkey, c_name, sum(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name,
               c_address, c_phone, c_comment
        FROM customer, orders, lineitem, nation
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= '1993-10-01'
          AND o_orderdate < '1994-01-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey
        GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        ORDER BY revenue DESC LIMIT 20""",
    "Q12": """SELECT l_shipmode,
               sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
               sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
        FROM orders, lineitem
        WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate
          AND l_shipdate < l_commitdate AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'
        GROUP BY l_shipmode ORDER BY l_shipmode""",
    "Q14": """SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
               / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
        FROM lineitem, part
        WHERE l_partkey = p_partkey AND l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'""",
    "Q15": """WITH revenue AS (
          SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue
          FROM lineitem WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01' GROUP BY l_suppkey)
        SELECT s_suppkey, s_name, s_address, s_phone, total_revenue FROM supplier, revenue
        WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM revenue)
        ORDER BY s_suppkey""",
    "Q17": """SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem, part
        WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX'
          AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)""",
    "Q18": """SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
        FROM customer, orders, lineitem
        WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
          AND c_custkey = o_custkey AND o_orderkey = l_orderkey
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
        ORDER BY o_totalprice DESC, o_orderdate LIMIT 100""",
    "Q19": "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem, part WHERE " + " OR ".join((
        _Q19_GROUP.format(brand="Brand#12", boxes="'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'", low=1, high=11, size=5),
        _Q19_GROUP.format(brand="Brand#23", boxes="'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'", low=10, high=20, size=10),
        _Q19_GROUP.format(brand="Brand#34", boxes="'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'", low=20, high=30, size=15),
    )),
    "Q20": """SELECT s_name, s_address FROM supplier, nation
        WHERE s_suppkey IN (
            SELECT ps_suppkey FROM partsupp
            WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
              AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM lineitem
                                 WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                                   AND l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'))
          AND s_nationkey = n_nationkey AND n_name = 'CANADA' ORDER BY s_name""",
    "Q21": """SELECT s_name, count(*) AS numwait FROM supplier, lineitem l1, orders, nation
        WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F'
          AND l1.l_receiptdate > l1.l_commitdate
          AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
          AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey
                          AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
          AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
        GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100""",
    "count": "SELECT count(*) FROM lineitem",
    "star": "SELECT * FROM lineitem WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1994-02-01'",
}
QUERIES = {name: " ".join(text.split()) for name, text in QUERIES.items()}


# ---------------------------------------------------------------------------
# Data


def generate(sf: float) -> Path:
    """Write all eight TPC-H tables at ``sf`` with tpchgen-cli unless they are already there."""
    out = DATA / f"sf{sf:g}"
    if not (out / "lineitem.parquet").exists() or not (out / "region.parquet").exists():
        binary = shutil.which("tpchgen-cli")
        if binary is None:
            raise SystemExit("tpchgen-cli is not installed: pip install tpchgen-cli")
        out.mkdir(parents=True, exist_ok=True)
        new = subprocess.run([binary, "parquet", "-s", f"{sf:g}", "--output-dir", str(out)], capture_output=True)
        if new.returncode != 0:
            subprocess.run([binary, "-s", f"{sf:g}", "--format", "parquet", "--output-dir", str(out)],
                           check=True, capture_output=True)
    return out


def connect(sf: float):
    """A DuckDB connection holding the seven static tables, dates as text and decimals as doubles."""
    import duckdb

    out = generate(sf)
    con = duckdb.connect()
    con.execute("SET threads = 2")
    for table in OTHER_TABLES:
        described = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{(out / table).as_posix()}.parquet')").fetchall()
        cols = []
        for name, kind, *_rest in described:
            if kind == "DATE":
                cols.append(f"CAST({name} AS VARCHAR) AS {name}")
            elif kind.startswith("DECIMAL"):
                cols.append(f"CAST({name} AS DOUBLE) AS {name}")
            else:
                cols.append(name)
        con.execute(f"CREATE TABLE {table} AS SELECT {', '.join(cols)} FROM read_parquet('{(out / table).as_posix()}.parquet')")
    return con


def lineitem_rows(sf: float) -> dict[int, dict]:
    """All 16 lineitem columns as a row map ordered by ship date."""
    import duckdb

    path = generate(sf) / "lineitem.parquet"
    con = duckdb.connect()
    fetched = con.execute(f"""
        SELECT row_number() OVER (ORDER BY l_shipdate, l_orderkey, l_linenumber) - 1 AS id,
               l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity::DOUBLE, l_extendedprice::DOUBLE,
               l_discount::DOUBLE, l_tax::DOUBLE, l_returnflag, l_linestatus,
               CAST(l_shipdate AS VARCHAR), CAST(l_commitdate AS VARCHAR), CAST(l_receiptdate AS VARCHAR),
               l_shipinstruct, l_shipmode, l_comment
        FROM read_parquet('{path.as_posix()}') ORDER BY id""").fetchall()
    con.close()
    return {row[0]: dict(zip(COLUMNS, row)) for row in fetched}


def _register(con, rows: dict[int, dict]) -> None:
    import pyarrow as pa

    items = list(rows.values())
    con.register("lineitem", pa.table({col: [row[col] for row in items] for col in COLUMNS}))


def _norm(value):
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        return float(f"{value:.9g}")
    return value


def run_queries(con, rows: dict[int, dict]) -> dict[str, list]:
    """Each query's result as a sorted list of rows with floats rounded to nine digits."""
    _register(con, rows)
    out = {}
    for name, sql in QUERIES.items():
        fetched = con.execute(sql).fetchall()
        out[name] = sorted((tuple(_norm(v) for v in row) for row in fetched), key=repr)
    con.unregister("lineitem")
    return out


# ---------------------------------------------------------------------------
# Refresh branches


def _shift(day: str, days: int) -> str:
    return (datetime.date.fromisoformat(day) + datetime.timedelta(days=days)).isoformat()


def refreshes(rows: dict[int, dict]) -> dict[str, dict]:
    """Delete, update and insert refreshes of lineitem: {name: {kind, sql, deletes, updates, inserts}}."""
    from benchmarks.rule_baselines import rows_matching

    rng = random.Random(2028)
    orderkeys = sorted({row["l_orderkey"] for row in rows.values()})
    partkeys = sorted({row["l_partkey"] for row in rows.values()})
    by_order: dict[int, list[int]] = {}
    for rid, row in rows.items():
        by_order.setdefault(row["l_orderkey"], []).append(rid)

    def where(clause: str) -> list[int]:
        found = rows_matching(clause, rows)
        assert found is not None, clause
        return sorted(found)

    out: dict[str, dict] = {}

    def delete(name: str, clause: str) -> None:
        out[name] = {"kind": "delete", "sql": f"DELETE FROM lineitem WHERE {clause}", "where": clause,
                     "deletes": where(clause), "updates": {}, "inserts": {}}

    def update(name: str, sql: str, changes: dict[int, dict]) -> None:
        out[name] = {"kind": "update", "sql": sql, "where": "", "deletes": [], "updates": changes, "inserts": {}}

    delete("purge_1992", "l_shipdate < '1993-01-01'")
    delete("purge_1994q1", "l_shipdate BETWEEN '1994-01-01' AND '1994-03-31'")
    delete("purge_1995", "l_shipdate >= '1995-01-01' AND l_shipdate < '1996-01-01'")
    delete("purge_late1998", "l_shipdate > '1998-12-01'")
    for label in ("rf2_a", "rf2_b"):
        keys = sorted(rng.sample(orderkeys, 15))
        delete(label, f"l_orderkey IN ({', '.join(map(str, keys))})")
    keys = sorted(rng.sample(partkeys, 5))
    delete("purge_5_parts", f"l_partkey IN ({', '.join(map(str, keys))})")

    hit = where("l_shipdate BETWEEN '1994-01-01' AND '1994-03-31'")
    update("discount_fix_1994q1",
           "UPDATE lineitem SET l_discount = l_discount + 0.01 WHERE l_shipdate BETWEEN '1994-01-01' AND '1994-03-31'",
           {rid: {"l_discount": round(rows[rid]["l_discount"] + 0.01, 2)} for rid in hit})
    hit = where("l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'")
    update("reship_sep1995",
           "UPDATE lineitem SET l_shipdate = l_shipdate + 30 WHERE l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'",
           {rid: {"l_shipdate": _shift(rows[rid]["l_shipdate"], 30)} for rid in hit})
    keys = rng.sample(orderkeys, 40)
    hit = [rid for key in keys for rid in by_order[key] if rows[rid]["l_returnflag"] == "N"]
    update("return_40_orders", "UPDATE lineitem SET l_returnflag = 'R' WHERE l_orderkey IN (40 keys) AND l_returnflag = 'N'",
           {rid: {"l_returnflag": "R"} for rid in sorted(hit)})
    keys = rng.sample(orderkeys, 40)
    hit = [rid for key in keys for rid in by_order[key]]
    update("requantity_40_orders", "UPDATE lineitem SET l_quantity = l_quantity + 1 WHERE l_orderkey IN (40 keys)",
           {rid: {"l_quantity": rows[rid]["l_quantity"] + 1} for rid in sorted(hit)})
    hit = where("l_shipmode = 'REG AIR' AND l_shipdate >= '1993-01-01' AND l_shipdate < '1994-01-01'")
    update("shipmode_fix_1993",
           "UPDATE lineitem SET l_shipmode = 'AIR' WHERE l_shipmode = 'REG AIR' AND l_shipdate >= '1993-01-01' "
           "AND l_shipdate < '1994-01-01'",
           {rid: {"l_shipmode": "AIR"} for rid in hit})
    hit = where("l_shipdate >= '1996-01-01' AND l_shipdate < '1997-01-01'")
    update("comment_touch_1996",
           "UPDATE lineitem SET l_comment = 'checked' WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1997-01-01'",
           {rid: {"l_comment": "checked"} for rid in hit})
    hit = where("l_receiptdate >= '1997-06-01' AND l_receiptdate < '1997-07-01'")
    update("late_receipt_jun1997",
           "UPDATE lineitem SET l_receiptdate = l_receiptdate + 20 WHERE l_receiptdate >= '1997-06-01' "
           "AND l_receiptdate < '1997-07-01'",
           {rid: {"l_receiptdate": _shift(rows[rid]["l_receiptdate"], 20)} for rid in hit})

    # RF1 as the specification writes it for lineitem: new line items for orders that exist.
    keys = rng.sample(orderkeys, 15)
    new_rows = {}
    next_id = max(rows) + 1
    for key in keys:
        template = rows[by_order[key][0]]
        line = max(rows[rid]["l_linenumber"] for rid in by_order[key]) + 1
        new_rows[next_id] = {**template, "id": next_id, "l_linenumber": line}
        next_id += 1
    out["insert_15_lines"] = {"kind": "insert", "sql": "INSERT INTO lineitem (one new line for each of 15 orders)",
                              "where": "", "deletes": [], "updates": {}, "inserts": new_rows}
    return out


# ---------------------------------------------------------------------------
# Systems


def _flagged(fn, repo, left_id: str, right_id: str) -> bool:
    from semlake.baselines import WriteConflictException

    try:
        result = fn(repo, left_id, right_id, repo.store.clone())
    except WriteConflictException:
        return False
    return result.status == "semantic_conflict"


def systems() -> dict:
    """{name: (which read declaration it gets, merge function)}."""
    from benchmarks.rule_baselines import b1r_merge, ssi_merge
    from semlake.baselines import copy_on_write
    from semlake.merge import three_way_merge

    return {
        "semlake": ("projected", three_way_merge),
        # Tier 1 only: no row-value check on the recorded pre-images.
        "semlake_static": ("projected", lambda repo, l, r, s: three_way_merge(repo, l, r, s, post_check=False)),
        # Projection off: a join or subquery read keeps its text and cannot be evaluated on rows.
        "semlake_noproject": ("text", three_way_merge),
        # File-level read check on the same pushed-down filters, and appends that match them (Delta-style).
        "b1r": ("projected", lambda repo, l, r, s: b1r_merge(repo, l, r, s, check_appends=True)),
        "b6": ("projected", ssi_merge),
        "b2": ("projected", copy_on_write),
    }


def read_class(preds) -> str:
    from semlake.sql_predicates import TABLE_WIDE

    if any(pred.where == "" for pred in preds):
        return TABLE_WIDE
    return preds[0].detail


def _score(bucket: dict) -> dict:
    tp, fp, fn, tn = bucket["tp"], bucket["fp"], bucket["fn"], bucket["tn"]
    return {**bucket,
            "precision": round(tp / (tp + fp), 3) if tp + fp else None,
            "recall": round(tp / (tp + fn), 3) if tp + fn else None,
            "false_block_rate": round(fp / (fp + tn), 3) if fp + tn else None}


def run(sf: float = 0.01, quick: bool = False, only: list[str] | None = None, out_name: str | None = None) -> dict:
    from semlake.metadata import Repository, Schema, SchemaField
    from semlake.sql_predicates import predicates_from_sql
    from semlake.storage import MemoryObjectStore
    from semlake.verification import Predicate

    rows = lineitem_rows(sf)
    con = connect(sf)
    at_fork = run_queries(con, rows)
    schema = Schema(tuple(SchemaField(name, kind) for name, kind in LINEITEM))
    repo = Repository(MemoryObjectStore())
    base = repo.create_table("main", schema, rows=rows, rows_per_file=ROWS_PER_FILE)
    owned = frozenset(COLUMNS)

    reads: dict[str, dict] = {}
    for q_name, sql in QUERIES.items():
        projected = predicates_from_sql(sql, q_name, table="lineitem", table_columns=owned)
        text = predicates_from_sql(sql, q_name, table="lineitem", table_columns=owned, project=False)
        reads[q_name] = {
            "class": read_class(projected),
            "shape": projected[0].detail,
            "clauses": [pred.where for pred in projected],
            "columns": sorted(projected[0].columns) if projected[0].columns is not None else None,
            "projected": repo.apply(base, f"{q_name}-projected", predicates=projected).snapshot_id,
            "text": repo.apply(base, f"{q_name}-text", predicates=text).snapshot_id,
        }
        print(f"  {q_name:<6} {reads[q_name]['class']:<10} {reads[q_name]['clauses']}", flush=True)

    chosen = refreshes(rows)
    if quick:
        chosen = {name: chosen[name] for name in ("purge_1994q1", "discount_fix_1994q1", "insert_15_lines")}
    if only:
        chosen = {name: spec for name, spec in chosen.items() if name in only}
    table = systems()
    cases = []
    for rf_name, spec in chosen.items():
        after = {rid: {**row, **spec["updates"].get(rid, {})} for rid, row in rows.items() if rid not in set(spec["deletes"])}
        after.update(spec["inserts"])
        refreshed = run_queries(con, after)
        preds = (Predicate(rf_name, "deletes_where", where=spec["where"]),) if spec["kind"] == "delete" else ()
        left = repo.apply(base, rf_name, deletes=spec["deletes"] or None, updates=spec["updates"] or None,
                          inserts=spec["inserts"] or None, predicates=preds)
        for q_name in QUERIES:
            truth = at_fork[q_name] != refreshed[q_name]
            case = {"refresh": rf_name, "kind": spec["kind"], "query": q_name, "class": reads[q_name]["class"],
                    "shape": reads[q_name]["shape"], "conflict": int(truth),
                    "changed_rows": len(spec["deletes"]) + len(spec["updates"]) + len(spec["inserts"])}
            for name, (declaration, fn) in table.items():
                started = time.perf_counter()
                case[name] = int(_flagged(fn, repo, left.snapshot_id, reads[q_name][declaration]))
                case[f"{name}_s"] = round(time.perf_counter() - started, 4)
            cases.append(case)
            print(f"  {rf_name:<22} {q_name:<6} {case['class']:<10} truth={int(truth)} "
                  + " ".join(f"{n}={case[n]}" for n in table), flush=True)

    def tally(selected: list[dict]) -> dict:
        out = {}
        for name in table:
            bucket = {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
            for case in selected:
                flagged, truth = case[name], case["conflict"]
                bucket[("tp" if truth else "fp") if flagged else ("fn" if truth else "tn")] += 1
            out[name] = _score(bucket)
        return out

    classes = sorted({case["class"] for case in cases})
    kinds = sorted({case["kind"] for case in cases})
    doc = {
        "scale_factor": sf, "rows": len(rows), "rows_per_file": ROWS_PER_FILE, "lineitem_columns": len(COLUMNS) - 1,
        "truth": "the full query's result over all eight tables differs between the fork and the refreshed lineitem",
        "queries": QUERIES,
        "reads": {name: {k: v for k, v in info.items() if k in ("class", "shape", "clauses", "columns")}
                  for name, info in reads.items()},
        "refreshes": {name: {"kind": spec["kind"], "sql": spec["sql"],
                             "rows": len(spec["deletes"]) + len(spec["updates"]) + len(spec["inserts"])}
                      for name, spec in chosen.items()},
        "systems": list(table),
        "pairs": len(cases), "conflicts": sum(case["conflict"] for case in cases),
        "summary": tally(cases),
        "by_class": {cls: {"pairs": sum(1 for c in cases if c["class"] == cls),
                           **tally([c for c in cases if c["class"] == cls])} for cls in classes},
        "by_kind": {kind: {"pairs": sum(1 for c in cases if c["kind"] == kind),
                           **tally([c for c in cases if c["kind"] == kind])} for kind in kinds},
        "cases": cases,
    }
    name = out_name or ("tpch_full_queries.json" if sf == 0.01 else f"tpch_full_queries_sf{sf:g}.json")
    if not quick:
        RESULTS.mkdir(parents=True, exist_ok=True)
        (RESULTS / name).write_text(json.dumps(doc, indent=1) + "\n")
        print(f"wrote {RESULTS / name}")
    for system, score in doc["summary"].items():
        print(f"  {system:<18} recall={score['recall']} precision={score['precision']} "
              f"false blocks={score['false_block_rate']} {score['tp']}/{score['fp']}/{score['fn']}/{score['tn']}")
    return doc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sf", type=float, default=0.01)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--only", nargs="*")
    parser.add_argument("--out")
    args = parser.parse_args()
    run(args.sf, args.quick, args.only, args.out)


if __name__ == "__main__":
    main()
