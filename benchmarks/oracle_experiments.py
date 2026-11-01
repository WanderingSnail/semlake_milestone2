"""Randomized refresh-versus-report pairs labelled by a result oracle.

The labelled suite and the TPC-H pairs mark a pair as a conflict when a deleted
row satisfies the report's filter, which is SemLake's own definition. Here the
label comes from the data instead: a pair conflicts when the report, run on the
refreshed table, returns a different value than it returned at the fork. The
refreshes and reports are drawn at random from clause templates over TPC-H
lineitem (conjunctive, NOT, OR, and column-to-column comparisons), so the
pairs were not written with any system's rule in mind.

Run: python -m benchmarks.oracle_experiments [--pairs 300] [--sf 0.01]
"""

from __future__ import annotations

import argparse
import random
import time

from benchmarks.tpch_experiments import (_flagged, _query_value, _repo, _score, _systems, _write,
                                         lineitem_rows)

AGGREGATES = ("sum(l_quantity)", "sum(l_discount)", "count(*)", "max(l_quantity)", "avg(l_discount)")
SHIPMODES = ("AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK")
FLAGS = ("A", "N", "R")


def _date(rng: random.Random, low: int = 1992, high: int = 1998) -> int:
    return rng.randint(low, high) * 10000 + rng.randint(1, 12) * 100 + rng.randint(1, 28)


def _shift(day: int, days: int) -> int:
    """Add ``days`` to a yyyymmdd integer, keeping it a valid date."""
    import datetime as dt

    value = dt.date(day // 10000, day // 100 % 100, day % 100) + dt.timedelta(days=days)
    return value.year * 10000 + value.month * 100 + value.day


def _atom(rng: random.Random, narrow: bool, orderkeys: list[int]) -> str:
    """One random condition. ``narrow`` atoms select few rows (refreshes); wide ones suit reports."""
    kind = rng.choice(("date", "date", "discount", "quantity", "flag", "mode", "order") if narrow
                      else ("date", "date", "discount", "quantity", "flag", "mode", "late"))
    if kind == "date":
        start = _date(rng)
        width = rng.randint(1, 20) if narrow else rng.randint(30, 720)
        return f"l_shipdate BETWEEN {start} AND {_shift(start, width)}"
    if kind == "discount":
        low = rng.randint(0, 9) / 100
        return f"l_discount BETWEEN {low:.2f} AND {low + (0 if narrow else rng.randint(1, 4) / 100):.2f}"
    if kind == "quantity":
        cut = rng.randint(1, 50)
        return f"l_quantity {'=' if narrow else rng.choice(('<', '>='))} {cut}"
    if kind == "flag":
        return f"l_returnflag = '{rng.choice(FLAGS)}'"
    if kind == "mode":
        return f"l_shipmode IN ({', '.join(repr(m) for m in rng.sample(SHIPMODES, rng.randint(1, 3)))})"
    if kind == "order":
        return f"l_orderkey IN ({', '.join(map(str, sorted(rng.sample(orderkeys, rng.randint(1, 10)))))})"
    return "l_commitdate < l_receiptdate"


def _clause(rng: random.Random, narrow: bool, orderkeys: list[int]) -> tuple[str, str]:
    """A WHERE clause and its form: conjunctive, NOT or OR."""
    form = rng.choices(("and", "not", "or"), weights=(6, 2, 2))[0]
    if form == "or":
        return f"({_atom(rng, narrow, orderkeys)}) OR ({_atom(rng, narrow, orderkeys)})", form
    atoms = [_atom(rng, narrow, orderkeys) for _ in range(rng.randint(1, 2 if narrow else 3))]
    if form == "not":
        # Negate a wide atom and keep a narrow one so the refresh stays small.
        atoms[0] = f"NOT ({_atom(rng, False, orderkeys)})"
        if len(atoms) == 1:
            atoms.append(_atom(rng, True, orderkeys))
    return " AND ".join(atoms), form


def _refresh(rng, rows, orderkeys, by_order, next_ids):
    """A random refresh: (name, kind, sql, updates, deletes, inserts)."""
    from benchmarks.rule_baselines import rows_matching

    kind = rng.choices(("delete", "update", "insert"), weights=(6, 3, 1))[0]
    if kind == "insert":
        keys = rng.sample(orderkeys, rng.randint(1, 10))
        inserts = {}
        for key in keys:
            for rid in by_order[key]:
                inserts[next_ids[0]] = {**rows[rid], "id": next_ids[0], "l_orderkey": next_ids[1]}
                next_ids[0] += 1
            next_ids[1] += 1
        return kind, f"INSERT INTO lineitem (copies of {len(keys)} orders)", {}, [], inserts
    for _ in range(50):
        where, _form = _clause(rng, True, orderkeys)
        hit = rows_matching(where, rows)
        if hit and len(hit) <= 0.02 * len(rows):
            break
    else:
        where, hit = "l_orderkey IN (1)", rows_matching("l_orderkey IN (1)", rows)
    hit = sorted(hit)
    if kind == "delete":
        return kind, f"DELETE FROM lineitem WHERE {where}", {}, hit, {}
    col = rng.choice(("l_discount", "l_quantity", "l_shipdate", "l_returnflag"))
    if col == "l_discount":
        updates = {rid: {col: round(min(rows[rid][col] + 0.01, 0.10), 2)} for rid in hit}
        expr = "least(l_discount + 0.01, 0.10)"
    elif col == "l_quantity":
        updates = {rid: {col: rows[rid][col] + 1} for rid in hit}
        expr = "l_quantity + 1"
    elif col == "l_shipdate":
        days = rng.randint(1, 60)
        updates = {rid: {col: _shift(rows[rid][col], days)} for rid in hit}
        expr = f"l_shipdate + {days} days"
    else:
        updates = {rid: {col: "R"} for rid in hit}
        expr = "'R'"
    updates = {rid: cols for rid, cols in updates.items() if rows[rid][col] != cols[col]}
    return kind, f"UPDATE lineitem SET {col} = {expr} WHERE {where}", updates, [], {}


def run_random_pairs(sf: float = 0.01, pairs: int = 300, seed: int = 2028, refreshes: int = 30) -> dict:
    """``refreshes`` random refresh branches, each raced by ``pairs // refreshes`` random reports."""
    from semlake.sql_predicates import predicates_from_sql

    rows = lineitem_rows(sf)
    repo, base = _repo(rows)
    rng = random.Random(seed)
    orderkeys = sorted({row["l_orderkey"] for row in rows.values()})
    by_order: dict[int, list[int]] = {}
    for rid, row in rows.items():
        by_order.setdefault(row["l_orderkey"], []).append(rid)
    next_ids = [max(rows) + 1, max(orderkeys) + 1]
    systems = _systems(include_dolt=False)
    buckets = {name: {"tp": 0, "fp": 0, "fn": 0, "tn": 0} for name in systems}
    by_kind = {kind: {name: {"tp": 0, "fp": 0, "fn": 0, "tn": 0} for name in systems}
               for kind in ("delete", "update", "insert")}
    cases = []
    per_refresh = max(1, pairs // refreshes)
    started = time.perf_counter()
    for i in range(refreshes):
        kind, rf_sql, updates, deletes, inserts = _refresh(rng, rows, orderkeys, by_order, next_ids)
        preds = predicates_from_sql(rf_sql, f"rf{i}") if kind == "delete" else []
        left = repo.apply(base, f"rf{i}", updates=updates or None, deletes=deletes or None,
                          inserts=inserts or None, predicates=preds)
        after = {rid: {**row, **updates.get(rid, {})} for rid, row in rows.items() if rid not in set(deletes)}
        after.update(inserts)
        for j in range(per_refresh):
            where, form = _clause(rng, False, orderkeys)
            q_sql = f"SELECT {rng.choice(AGGREGATES)} FROM lineitem WHERE {where}"
            before_value = _query_value(q_sql, rows)
            after_value = _query_value(q_sql, after)
            if isinstance(before_value, float) or isinstance(after_value, float):
                truth = (before_value is None) != (after_value is None) or (
                    before_value is not None and abs(before_value - after_value) > 1e-9)
            else:
                truth = before_value != after_value
            right = repo.apply(base, f"q{i}_{j}", predicates=predicates_from_sql(q_sql, f"q{i}_{j}"))
            case = {"refresh": rf_sql[:160], "kind": kind, "query": q_sql, "form": form,
                    "conflict": int(truth), "changed_rows": len(updates) + len(deletes) + len(inserts)}
            for name, fn in systems.items():
                flagged = _flagged(fn, repo, left.snapshot_id, right.snapshot_id)
                case[name] = int(flagged)
                key = ("tp" if truth else "fp") if flagged else ("fn" if truth else "tn")
                buckets[name][key] += 1
                by_kind[kind][name][key] += 1
            cases.append(case)
        print(f"  refresh {i:>2} {kind:<6} changed={cases[-1]['changed_rows']:<5} "
              f"conflicts={sum(c['conflict'] for c in cases[-per_refresh:])}/{per_refresh} "
              f"semlake_fp={sum(c['semlake'] and not c['conflict'] for c in cases[-per_refresh:])} "
              f"semlake_fn={sum(c['conflict'] and not c['semlake'] for c in cases[-per_refresh:])}", flush=True)
    out = {
        "scale_factor": sf, "rows": len(rows), "seed": seed, "pairs": len(cases),
        "truth": "the report's value on the refreshed table differs from its value at the fork",
        "systems": list(systems),
        "summary": {name: _score(bucket) for name, bucket in buckets.items()},
        "by_kind": {kind: {name: _score(b) for name, b in d.items()} for kind, d in by_kind.items()},
        "seconds": round(time.perf_counter() - started, 1),
        "cases": cases,
    }
    _write("oracle_pairs.json", out)
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sf", type=float, default=0.01)
    parser.add_argument("--pairs", type=int, default=300)
    parser.add_argument("--seed", type=int, default=2028)
    args = parser.parse_args()
    result = run_random_pairs(args.sf, args.pairs, args.seed)
    for name, score in result["summary"].items():
        print(f"{name:<22} {score}")
