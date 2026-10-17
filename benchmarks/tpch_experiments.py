"""Experiments on real TPC-H lineitem data from ``tpchgen-cli`` (a TPC-H dbgen port).

    python benchmarks/tpch_experiments.py pairs        # refresh-vs-query branch pairs (Table 3b)
    python benchmarks/tpch_experiments.py selectivity  # false blocks vs delete selectivity (Fig. 3)
    python benchmarks/tpch_experiments.py scan         # Q6 scan overhead at SF 1 (Table 4)
    python benchmarks/tpch_experiments.py compaction   # overhead as merges accumulate (Fig. 7)
    python benchmarks/tpch_experiments.py all

Data: ``tpchgen-cli`` writes ``lineitem.parquet`` under ``benchmarks/data/tpch/sf<SF>/``.
The merge experiments use SF 0.01 (60,175 rows) because the engine is a Python
prototype that keeps row values in memory. The scan experiments use SF 1 (6,001,215 rows).

Layout: the merge experiments cluster lineitem by ``l_shipdate`` (a common lakehouse
layout), 2,000 rows per file. Query predicates are the lineitem-side WHERE clauses of
TPC-H Q1, Q3, Q6, Q7, Q10, Q12 and Q14 with the validation parameters; filters on
other tables are dropped, because the prototype tracks one table.
"""

from __future__ import annotations

import json
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATA = ROOT / "benchmarks" / "data" / "tpch"
RESULTS = ROOT / "benchmarks" / "results"
ROWS_PER_FILE = 2000

COLUMNS = ("id", "l_orderkey", "l_partkey", "l_quantity", "l_discount", "l_returnflag",
           "l_shipmode", "l_shipdate", "l_commitdate", "l_receiptdate")
TYPES = ("int64", "int64", "int64", "int64", "double", "string", "string", "int64", "int64", "int64")

QUERIES = {
    "Q1": "SELECT sum(l_quantity) FROM lineitem WHERE l_shipdate <= 19980902",
    "Q3": "SELECT sum(l_discount) FROM lineitem WHERE l_shipdate > 19950315",
    "Q6": ("SELECT sum(l_discount) FROM lineitem WHERE l_shipdate >= 19940101 AND l_shipdate < 19950101 "
           "AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"),
    "Q7": "SELECT sum(l_discount) FROM lineitem WHERE l_shipdate BETWEEN 19950101 AND 19961231",
    "Q10": "SELECT sum(l_discount) FROM lineitem WHERE l_returnflag = 'R'",
    "Q12": ("SELECT count(*) FROM lineitem WHERE l_shipmode IN ('MAIL', 'SHIP') "
            "AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate "
            "AND l_receiptdate >= 19940101 AND l_receiptdate < 19950101"),
    "Q14": "SELECT sum(l_discount) FROM lineitem WHERE l_shipdate >= 19950901 AND l_shipdate < 19951001",
}


# ---------------------------------------------------------------------------
# Data


def lineitem_path(sf: float) -> Path:
    """Generate lineitem at scale factor ``sf`` with tpchgen-cli unless it is already there."""
    out = DATA / f"sf{sf:g}"
    path = out / "lineitem.parquet"
    if not path.exists():
        binary = shutil.which("tpchgen-cli")
        if binary is None:
            raise SystemExit("tpchgen-cli is not installed: pip install tpchgen-cli")
        out.mkdir(parents=True, exist_ok=True)
        subprocess.run([binary, "-s", f"{sf:g}", "--tables", "lineitem", "--format", "parquet",
                        "--output-dir", str(out)], check=True, capture_output=True)
    return path


def lineitem_rows(sf: float) -> dict[int, dict]:
    """Lineitem as a row map ordered by ship date; dates become yyyymmdd integers."""
    import duckdb

    path = lineitem_path(sf)
    con = duckdb.connect()
    fetched = con.execute(f"""
        SELECT row_number() OVER (ORDER BY l_shipdate, l_orderkey, l_linenumber) - 1 AS id,
               l_orderkey, l_partkey, l_quantity::BIGINT, l_discount::DOUBLE, l_returnflag, l_shipmode,
               CAST(strftime(l_shipdate, '%Y%m%d') AS BIGINT),
               CAST(strftime(l_commitdate, '%Y%m%d') AS BIGINT),
               CAST(strftime(l_receiptdate, '%Y%m%d') AS BIGINT)
        FROM read_parquet('{path.as_posix()}') ORDER BY id""").fetchall()
    con.close()
    return {row[0]: dict(zip(COLUMNS, row)) for row in fetched}


def _repo(rows: dict[int, dict]):
    from semlake.metadata import Repository, Schema, SchemaField
    from semlake.storage import MemoryObjectStore

    schema = Schema(tuple(SchemaField(name, kind) for name, kind in zip(COLUMNS, TYPES)))
    repo = Repository(MemoryObjectStore())
    base = repo.create_table("main", schema, rows=rows, rows_per_file=ROWS_PER_FILE)
    return repo, base


def _systems(include_dolt: bool) -> dict:
    from benchmarks.rule_baselines import b1r_merge, ssi_merge
    from semlake.baselines import copy_on_write, crdt_merge, file_level_occ, tuple_version_merge
    from semlake.merge import three_way_merge

    systems = {
        "semlake": three_way_merge,
        "semlake_static": lambda repo, l, r, s: three_way_merge(repo, l, r, s, post_check=False),
        "b1": file_level_occ,
        "b1r": b1r_merge,
        "b2": copy_on_write,
        "b3": tuple_version_merge,
        "b4": crdt_merge,
        "b6": ssi_merge,
    }
    if include_dolt:
        from benchmarks.dolt_baseline import dolt_binary, dolt_merge

        if dolt_binary():
            systems["dolt"] = dolt_merge
    return systems


def _flagged(fn, repo, left_id: str, right_id: str) -> bool:
    """True when the system refuses the merge because of a semantic or read conflict."""
    from semlake.baselines import WriteConflictException

    try:
        result = fn(repo, left_id, right_id, repo.store.clone())
    except WriteConflictException:
        return False
    return result.status == "semantic_conflict"


def _score(bucket: dict) -> dict:
    tp, fp, fn, tn = bucket["tp"], bucket["fp"], bucket["fn"], bucket["tn"]
    return {**bucket,
            "precision": round(tp / (tp + fp), 3) if tp + fp else None,
            "recall": round(tp / (tp + fn), 3) if tp + fn else None}


# ---------------------------------------------------------------------------
# Table 3b: refresh-versus-query branch pairs


def run_pairs(sf: float = 0.01, include_dolt: bool = True) -> dict:
    from benchmarks.rule_baselines import rows_matching
    from semlake.sql_predicates import predicates_from_sql

    rows = lineitem_rows(sf)
    repo, base = _repo(rows)
    rng = random.Random(2026)
    orderkeys = sorted({row["l_orderkey"] for row in rows.values()})
    rf2_a = sorted(rng.sample(orderkeys, 15))
    rf2_b = sorted(rng.sample(orderkeys, 15))
    refreshes = {
        "purge_1992": "DELETE FROM lineitem WHERE l_shipdate < 19930101",
        "purge_1994q1": "DELETE FROM lineitem WHERE l_shipdate BETWEEN 19940101 AND 19940331",
        "purge_1995": "DELETE FROM lineitem WHERE l_shipdate >= 19950101 AND l_shipdate < 19960101",
        "purge_late1998": "DELETE FROM lineitem WHERE l_shipdate > 19981201",
        "rf2_a": f"DELETE FROM lineitem WHERE l_orderkey IN ({', '.join(map(str, rf2_a))})",
        "rf2_b": f"DELETE FROM lineitem WHERE l_orderkey IN ({', '.join(map(str, rf2_b))})",
    }
    systems = _systems(include_dolt)
    buckets = {name: {"tp": 0, "fp": 0, "fn": 0, "tn": 0} for name in systems}
    cases = []
    for rf_name, rf_sql in refreshes.items():
        rf_preds = predicates_from_sql(rf_sql, rf_name)
        deleted = rows_matching(rf_preds[0].where, rows)
        left = repo.apply(base, rf_name, deletes=sorted(deleted), predicates=rf_preds)
        for q_name, q_sql in QUERIES.items():
            q_preds = predicates_from_sql(q_sql, q_name)
            read = rows_matching(q_preds[0].where, rows)
            truth = bool(deleted & read)
            right = repo.apply(base, q_name, predicates=q_preds)
            case = {"refresh": rf_name, "query": q_name, "conflict": int(truth),
                    "deleted_rows": len(deleted), "read_rows": len(read)}
            for name, fn in systems.items():
                started = time.perf_counter()
                flagged = _flagged(fn, repo, left.snapshot_id, right.snapshot_id)
                case[name] = int(flagged)
                case[f"{name}_s"] = time.perf_counter() - started
                key = ("tp" if truth else "fp") if flagged else ("fn" if truth else "tn")
                buckets[name][key] += 1
            cases.append(case)
            print(f"  {rf_name:<15} {q_name:<4} truth={int(truth)} "
                  + " ".join(f"{n}={case[n]}" for n in systems), flush=True)
    out = {
        "scale_factor": sf, "rows": len(rows), "rows_per_file": ROWS_PER_FILE, "layout": "sorted by l_shipdate",
        "queries": QUERIES, "refreshes": refreshes, "systems": list(systems),
        "summary": {name: _score(bucket) for name, bucket in buckets.items()},
        "cases": cases,
    }
    _write("tpch_pairs.json", out)
    return out


# ---------------------------------------------------------------------------
# Fig. 3: false blocks against delete selectivity


def run_selectivity(sf: float = 0.01, windows: int = 10) -> dict:
    """A Q6 report races a purge whose rows never satisfy Q6 (discount below 0.05).

    The purge selects a contiguous ``l_partkey`` window sized to the target fraction
    of the table, written in three forms: a conjunction inside the static fragment,
    the same clause under NOT, and an OR form. Every flag is a false block.
    """
    from benchmarks.rule_baselines import rows_matching
    from semlake.sql_predicates import predicates_from_sql

    rows = lineitem_rows(sf)
    repo, base = _repo(rows)
    systems = {k: v for k, v in _systems(include_dolt=False).items()
               if k in ("semlake", "semlake_static", "b1", "b1r", "b6")}
    q6 = predicates_from_sql(QUERIES["Q6"], "Q6")
    q6_rows = rows_matching(q6[0].where, rows)
    report = repo.apply(base, "report", predicates=q6)
    q6_files = {base.row_file[r] for r in q6_rows}
    low_discount = sorted(rid for rid, row in rows.items() if row["l_discount"] < 0.04)
    by_part = sorted(low_discount, key=lambda rid: (rows[rid]["l_partkey"], rid))
    parts = [rows[rid]["l_partkey"] for rid in by_part]
    forms = {
        "conjunctive": "l_discount < 0.04 AND l_partkey >= {lo} AND l_partkey < {hi}",
        "not": "NOT (l_discount >= 0.04) AND l_partkey >= {lo} AND l_partkey < {hi}",
        "or": "(l_discount < 0.02 OR l_discount = 0.02 OR l_discount = 0.03) AND l_partkey >= {lo} AND l_partkey < {hi}",
    }
    fractions = [0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.2]
    rng = random.Random(7)
    points = []
    for frac in fractions:
        want = max(1, round(frac * len(rows)))
        flags = {name: 0 for name in systems}
        trials = 0
        achieved = []
        for _ in range(windows):
            start = rng.randrange(0, max(1, len(by_part) - want))
            lo = parts[start]
            hi = parts[min(len(parts) - 1, start + want)] + (1 if start + want >= len(parts) else 0)
            for form, template in forms.items():
                sql = f"DELETE FROM lineitem WHERE {template.format(lo=lo, hi=hi)}"
                preds = predicates_from_sql(sql, f"purge_{form}")
                deleted = rows_matching(preds[0].where, rows)
                assert deleted is not None and not (deleted & q6_rows), "purge must not overlap Q6"
                if not deleted:
                    continue
                achieved.append(len(deleted) / len(rows))
                purge = repo.apply(base, "purge", deletes=sorted(deleted), predicates=preds)
                trials += 1
                for name, fn in systems.items():
                    flags[name] += _flagged(fn, repo, purge.snapshot_id, report.snapshot_id)
        point = {"target_fraction": frac, "mean_fraction": statistics.fmean(achieved), "trials": trials,
                 **{f"{name}_false_block_rate": flags[name] / trials for name in systems}}
        points.append(point)
        print("  " + json.dumps(point), flush=True)
    out = {"scale_factor": sf, "rows": len(rows), "files": len(base.files), "q6_files": len(q6_files),
           "q6_rows": len(q6_rows), "forms": forms, "windows_per_point": windows, "points": points}
    _write("selectivity.json", out)
    return out


# ---------------------------------------------------------------------------
# Scan experiments (Table 4 and Fig. 7)

Q6_SQL = """
    SELECT sum(l_extendedprice * l_discount)
    FROM ({src}) t
    WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
      AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
"""
SCAN_COLS = "l_shipdate, l_discount, l_quantity, l_extendedprice"
GROUP_ROWS = 100_000


def _load_scan_table(sf: float):
    import duckdb

    con = duckdb.connect()
    table = con.execute(
        f"SELECT {SCAN_COLS} FROM read_parquet('{lineitem_path(sf).as_posix()}')").arrow()
    con.close()
    if hasattr(table, "read_all"):
        table = table.read_all()
    return table


class ScanState:
    """Base row groups that are never rewritten, plus deletion masks and compaction.

    A clean row group is read as it is. A dirty row group keeps its data file and is
    joined, by position, with a one-column mask of live rows (its deletion vector).
    Compacting a group rewrites its file without the deleted rows and clears the mask.
    """

    def __init__(self, table, root: Path) -> None:
        import numpy as np

        self.table = table
        self.root = root
        self.n = table.num_rows
        self.groups = (self.n + GROUP_ROWS - 1) // GROUP_ROWS
        sizes = [min(GROUP_ROWS, self.n - g * GROUP_ROWS) for g in range(self.groups)]
        self.deleted = [np.zeros(size, dtype=bool) for size in sizes]  # every delete so far
        self.in_file = [np.zeros(size, dtype=bool) for size in sizes]  # deletes folded into the file

    def group_slice(self, g: int):
        start = g * GROUP_ROWS
        return self.table.slice(start, min(GROUP_ROWS, self.n - start))

    def pending(self, g: int) -> int:
        return int(self.deleted[g].sum() - self.in_file[g].sum())

    def dirty(self, g: int) -> bool:
        return self.pending(g) > 0

    def compact(self, g: int) -> int:
        """Rewrite group ``g`` without its deleted rows; return the rows written."""
        self.in_file[g] = self.deleted[g].copy()
        return int((~self.deleted[g]).sum())

    def write(self) -> tuple[str, str]:
        """Write the current state; return (masked source, fully compacted source)."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        for old in self.root.glob("*.parquet"):
            old.unlink()
        clean_parts, dirty_parts, live_flags, compact_parts = [], [], [], []
        for g in range(self.groups):
            data = self.group_slice(g)
            compact_parts.append(data.filter(pa.array(~self.deleted[g])))
            stored = ~self.in_file[g]
            file_rows = data.filter(pa.array(stored)) if self.in_file[g].any() else data
            if self.dirty(g):
                dirty_parts.append(file_rows)
                live_flags.append(pa.array(~self.deleted[g][stored]))
            else:
                clean_parts.append(file_rows)
        sources = []
        if clean_parts:
            path = self.root / "clean.parquet"
            pq.write_table(pa.concat_tables(clean_parts), path, compression="none", row_group_size=GROUP_ROWS)
            sources.append(f"SELECT * FROM read_parquet('{path.as_posix()}')")
        if dirty_parts:
            data_path = self.root / "dirty.parquet"
            mask_path = self.root / "mask.parquet"
            pq.write_table(pa.concat_tables(dirty_parts), data_path, compression="none", row_group_size=GROUP_ROWS)
            pq.write_table(pa.table({"live": pa.concat_arrays(live_flags)}), mask_path, compression="none",
                           row_group_size=GROUP_ROWS)
            sources.append(
                f"SELECT * EXCLUDE (live) FROM read_parquet('{data_path.as_posix()}') "
                f"POSITIONAL JOIN read_parquet('{mask_path.as_posix()}') WHERE live")
        compact_path = self.root / "compact.parquet"
        pq.write_table(pa.concat_tables(compact_parts), compact_path, compression="none", row_group_size=GROUP_ROWS)
        return " UNION ALL ".join(sources), f"SELECT * FROM read_parquet('{compact_path.as_posix()}')"


def _time_pair(masked: str, compacted: str, repeats: int, batch: int) -> tuple[float, float]:
    import duckdb

    from benchmarks.workloads import _median_query_times

    con = duckdb.connect()
    con.execute("SET threads=1")
    dv_s, base_s = _median_query_times(
        con, [Q6_SQL.format(src=masked), Q6_SQL.format(src=compacted)], repeats, batch)
    con.close()
    return dv_s, base_s


def _scan_sample(task: dict) -> dict:
    """One process: build the deleted layout and time Q6 against the compacted table."""
    import numpy as np

    table = _load_scan_table(task["sf"])
    rng = np.random.default_rng(task["seed"])
    with tempfile.TemporaryDirectory(prefix="semlake-q6-") as tmp:
        state = ScanState(table, Path(tmp))
        k = int(round(task["density"] * state.n))
        if task["layout"] == "localized":
            g = int(rng.integers(state.groups))
            size = len(state.deleted[g])
            state.deleted[g][rng.choice(size, size=min(size, k), replace=False)] = True
        else:
            for pos in rng.choice(state.n, size=k, replace=False):
                state.deleted[pos // GROUP_ROWS][pos % GROUP_ROWS] = True
        masked, compacted = state.write()
        dv_s, base_s = _time_pair(masked, compacted, task["repeats"], task["batch"])
        clean = sum(1 for g in range(state.groups) if not state.dirty(g))
    return {**task, "dv_s": dv_s, "base_s": base_s, "overhead": dv_s / base_s - 1.0,
            "clean_groups": clean, "groups": state.groups}


def _in_process(fn, task: dict) -> dict:
    import multiprocessing

    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(1) as pool:
        return pool.apply(fn, (task,))


def run_scan(sf: float = 1.0, processes: int = 5, repeats: int = 5, batch: int = 3) -> dict:
    """Table 4: Q6 over real lineitem with deletion masks, one process at a time."""
    layouts = [("localized", 0.01), ("uniform", 0.001), ("uniform", 0.01), ("uniform", 0.05)]
    results = []
    for layout, density in layouts:
        samples = [_in_process(_scan_sample, {"sf": sf, "layout": layout, "density": density, "seed": 100 + i,
                                              "repeats": repeats, "batch": batch})
                   for i in range(processes)]
        overs = sorted(s["overhead"] for s in samples)
        mean = statistics.fmean(overs)
        half = 2.776 * statistics.stdev(overs) / len(overs) ** 0.5 if len(overs) > 1 else 0.0
        entry = {"layout": layout, "density": density, "processes": processes,
                 "base_ms": statistics.median(s["base_s"] for s in samples) * 1e3,
                 "dv_ms": statistics.median(s["dv_s"] for s in samples) * 1e3,
                 "overhead_median": overs[len(overs) // 2], "overhead_mean": mean,
                 "overhead_ci_low": mean - half, "overhead_ci_high": mean + half,
                 "clean_groups": samples[0]["clean_groups"], "groups": samples[0]["groups"],
                 "samples": samples}
        results.append(entry)
        print(f"  {layout:<9} {density:.1%} base={entry['base_ms']:.1f}ms dv={entry['dv_ms']:.1f}ms "
              f"overhead={entry['overhead_median'] * 100:+.1f}% "
              f"(95% CI {entry['overhead_ci_low'] * 100:+.1f} to {entry['overhead_ci_high'] * 100:+.1f}%)", flush=True)
    out = {"scale_factor": sf, "group_rows": GROUP_ROWS, "query": "TPC-H Q6", "duckdb_threads": 1,
           "results": results}
    _write("tpch_scan.json", out)
    return out


def _compaction_trace(task: dict) -> dict:
    """Run ``merges`` merges that each delete 0.1% of rows, compacting under one policy."""
    import numpy as np

    table = _load_scan_table(task["sf"])
    rng = np.random.default_rng(task["seed"])
    trace = []
    with tempfile.TemporaryDirectory(prefix="semlake-compact-") as tmp:
        state = ScanState(table, Path(tmp))
        per_merge = int(round(0.001 * state.n))
        for step in range(1, task["merges"] + 1):
            if task["layout"] == "localized":
                g = int(rng.integers(state.groups))
                free = np.flatnonzero(~state.deleted[g])
                state.deleted[g][rng.choice(free, size=min(len(free), per_merge), replace=False)] = True
            else:
                for pos in rng.choice(state.n, size=per_merge, replace=False):
                    state.deleted[pos // GROUP_ROWS][pos % GROUP_ROWS] = True
            rewritten = 0
            threshold = task["threshold"]
            if threshold is not None:
                for g in range(state.groups):
                    if state.pending(g) and state.pending(g) / len(state.deleted[g]) >= threshold:
                        rewritten += state.compact(g)
            masked, compacted = state.write()
            dv_s, base_s = _time_pair(masked, compacted, task["repeats"], task["batch"])
            dirty = sum(1 for g in range(state.groups) if state.dirty(g))
            trace.append({"merge": step, "overhead": dv_s / base_s - 1.0, "dirty_groups": dirty,
                          "rows_rewritten": rewritten})
    return {**task, "trace": trace}


def run_compaction(sf: float = 1.0, merges: int = 40) -> dict:
    """Fig. 7: Q6 overhead after each merge under a per-row-group compaction threshold."""
    policies = [("localized", None), ("localized", 0.05), ("localized", 0.02),
                ("uniform", None), ("uniform", 0.005), ("uniform", 0.002)]
    runs = []
    for layout, threshold in policies:
        result = _in_process(_compaction_trace, {"sf": sf, "layout": layout, "threshold": threshold, "seed": 5,
                                                 "merges": merges, "repeats": 3, "batch": 2})
        overs = [step["overhead"] for step in result["trace"]]
        rewritten = sum(step["rows_rewritten"] for step in result["trace"])
        result["mean_overhead"] = statistics.fmean(overs)
        result["rows_rewritten_per_merge"] = rewritten / merges
        runs.append(result)
        print(f"  {layout:<9} threshold={threshold} mean overhead={result['mean_overhead'] * 100:+.1f}% "
              f"rows rewritten per merge={result['rows_rewritten_per_merge']:.0f}", flush=True)
    out = {"scale_factor": sf, "delete_per_merge": 0.001, "group_rows": GROUP_ROWS, "runs": runs}
    _write("compaction.json", out)
    return out


def _write(name: str, payload: dict) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / name).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"wrote {RESULTS / name}", flush=True)


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("pairs", "all"):
        run_pairs()
    if which in ("selectivity", "all"):
        run_selectivity()
    if which in ("scan", "all"):
        run_scan()
    if which in ("compaction", "all"):
        run_compaction()
