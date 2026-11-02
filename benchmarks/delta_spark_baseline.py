"""Score real Delta Lake (Spark) on the TPC-H branch pairs, as a measured B1R.

B1R models Delta's row-level concurrency: deletes and updates write deletion
vectors, and a transaction that read a file a concurrent commit removed fails
with ConcurrentDeleteReadException. Here the real engine decides. For each pair
we copy the base table, open a report transaction, run the report's SQL inside
it (so Delta records the files it scanned, after data skipping), commit the
refresh with Spark SQL, then commit the report's one-row output file. A
concurrent-modification exception on that commit is a block.

Delta has no branches, so this is the closest real analogue: the report's
transaction stands for the report branch and the refresh commits first. The
table uses the same rows, order and file size as the other TPC-H runs
(lineitem sorted by ship date, 2,000 rows per file), with deletion vectors on.

Needs pyspark 4.0 and delta-spark 4.0 with Java 17+:

    python benchmarks/delta_spark_baseline.py            # writes results/delta_spark_pairs.json
"""

from __future__ import annotations

import json
import random
import shutil
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmarks.tpch_experiments import COLUMNS, QUERIES, RESULTS, ROWS_PER_FILE, lineitem_rows  # noqa: E402


def _spark():
    import pyspark
    from delta import configure_spark_with_delta_pip

    builder = (pyspark.sql.SparkSession.builder.master("local[2]").appName("semlake-delta-b1r")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


class DeltaRunner:
    def __init__(self, spark, work: Path) -> None:
        self.spark = spark
        self.jvm = spark._jvm
        self.work = work
        self.delta_log = getattr(self.jvm, "org.apache.spark.sql.delta.DeltaLog")
        self.active = getattr(getattr(self.jvm, "org.apache.spark.sql.delta.OptimisticTransaction$"), "MODULE$")
        self.action = getattr(getattr(self.jvm, "org.apache.spark.sql.delta.actions.Action$"), "MODULE$")
        self.manual = getattr(getattr(self.jvm, "org.apache.spark.sql.delta.DeltaOperations$ManualUpdate$"), "MODULE$")
        self.count = 0

    def _seq(self, items):
        lst = self.jvm.java.util.ArrayList()
        for item in items:
            lst.add(item)
        return self.jvm.scala.jdk.javaapi.CollectionConverters.asScala(lst).toSeq()

    def write_base(self, rows: dict[int, dict], path: Path) -> None:
        import pandas as pd

        frame = pd.DataFrame([rows[rid] for rid in sorted(rows)], columns=list(COLUMNS))
        (self.spark.createDataFrame(frame).coalesce(1).write.format("delta")
         .option("maxRecordsPerFile", ROWS_PER_FILE).option("delta.enableDeletionVectors", "true")
         .save(str(path)))

    def _output_file(self, table: Path) -> object:
        """Write the report's one-row output as a Parquet file inside ``table`` and return its AddFile."""
        self.count += 1
        scratch = self.work / f"out{self.count}"
        self.spark.createDataFrame([(-self.count,)], "id long").write.format("delta").save(str(scratch))
        entry = next(json.loads(line) for line in (scratch / "_delta_log" / "00000000000000000000.json")
                     .read_text().splitlines() if '"add"' in line)
        shutil.copy(scratch / entry["add"]["path"], table / entry["add"]["path"])
        return self.action.fromJson(json.dumps(entry))

    def blocked(self, base: Path, report_sql: str, refresh) -> tuple[bool, str]:
        """Run the report in a transaction, commit ``refresh`` concurrently, then commit the report."""
        self.count += 1
        table = self.work / f"t{self.count}"
        shutil.copytree(base, table)
        name = f"delta.`{table}`"
        txn = self.delta_log.forTable(self.spark._jsparkSession, str(table)).startTransaction()
        self.active.setActive(txn)
        try:
            self.spark.sql(report_sql.replace("FROM lineitem", f"FROM {name}")).collect()
        finally:
            self.active.clearActive()
        refresh(name, table)
        add = self._output_file(table)
        try:
            txn.commit(self._seq([add]), self.manual)
            return False, "committed"
        except Exception as exc:  # noqa: BLE001 - py4j wraps the Delta exception
            text = str(exc)
            at = text.find("io.delta.exceptions.")
            return True, text[at:at + 120].split(":")[0] if at >= 0 else text[:120]
        finally:
            shutil.rmtree(table, ignore_errors=True)


def _delete_refreshes(rows) -> dict[str, str]:
    """The six delete refreshes of tpch_experiments.run_pairs, with the same samples."""
    rng = random.Random(2026)
    orderkeys = sorted({row["l_orderkey"] for row in rows.values()})
    rf2_a = sorted(rng.sample(orderkeys, 15))
    rf2_b = sorted(rng.sample(orderkeys, 15))
    return {
        "purge_1992": "l_shipdate < 19930101",
        "purge_1994q1": "l_shipdate BETWEEN 19940101 AND 19940331",
        "purge_1995": "l_shipdate >= 19950101 AND l_shipdate < 19960101",
        "purge_late1998": "l_shipdate > 19981201",
        "rf2_a": f"l_orderkey IN ({', '.join(map(str, rf2_a))})",
        "rf2_b": f"l_orderkey IN ({', '.join(map(str, rf2_b))})",
    }


def _update_refreshes(rows, spark):
    """The five update and insert refreshes of run_update_pairs, with the same samples."""
    import pandas as pd

    rng = random.Random(2027)
    orderkeys = sorted({row["l_orderkey"] for row in rows.values()})
    by_order: dict[int, list[int]] = {}
    for rid, row in rows.items():
        by_order.setdefault(row["l_orderkey"], []).append(rid)
    keys_ret = rng.sample(orderkeys, 40)
    keys_qty = rng.sample(orderkeys, 40)
    keys_new = rng.sample(orderkeys, 15)
    new_rows = []
    next_id, next_key = max(rows) + 1, max(orderkeys) + 1
    for key in keys_new:
        for rid in by_order[key]:
            new_rows.append({**rows[rid], "id": next_id, "l_orderkey": next_key})
            next_id += 1
        next_key += 1

    def sql(statement):
        return lambda name, _table: spark.sql(statement.format(t=name))

    def insert(_name, table):
        frame = pd.DataFrame(new_rows, columns=list(COLUMNS))
        spark.createDataFrame(frame).coalesce(1).write.format("delta").mode("append").save(str(table))

    inlist = lambda keys: ", ".join(map(str, keys))  # noqa: E731
    return {
        "discount_fix_1994q1": sql("UPDATE {t} SET l_discount = round(l_discount + 0.01, 2) "
                                   "WHERE l_shipdate BETWEEN 19940101 AND 19940331"),
        "reship_sep1995": sql("UPDATE {t} SET l_shipdate = l_shipdate + 100 "
                              "WHERE l_shipdate >= 19950901 AND l_shipdate < 19951001"),
        "return_40_orders": sql("UPDATE {t} SET l_returnflag = 'R' "
                                f"WHERE l_orderkey IN ({inlist(keys_ret)}) AND l_returnflag = 'N'"),
        "requantity_40_orders": sql(f"UPDATE {{t}} SET l_quantity = l_quantity + 1 WHERE l_orderkey IN ({inlist(keys_qty)})"),
        "rf1_insert_15_orders": insert,
    }


def _score(cases, key):
    tp = sum(1 for c in cases if c["conflict"] and c[key])
    fp = sum(1 for c in cases if not c["conflict"] and c[key])
    fn = sum(1 for c in cases if c["conflict"] and not c[key])
    return {"tp": tp, "fp": fp, "fn": fn, "tn": len(cases) - tp - fp - fn,
            "precision": round(tp / (tp + fp), 3) if tp + fp else None,
            "recall": round(tp / (tp + fn), 3) if tp + fn else None}


def main(sf: float = 0.01) -> dict:
    rows = lineitem_rows(sf)
    spark = _spark()
    work = Path(tempfile.mkdtemp(prefix="semlake-delta-"))
    runner = DeltaRunner(spark, work)
    base = work / "base"
    runner.write_base(rows, base)
    nfiles = len(list(base.glob("*.parquet")))
    labels = {}
    for name in ("tpch_pairs.json", "tpch_update_pairs.json"):
        for case in json.loads((RESULTS / name).read_text())["cases"]:
            labels[(case["refresh"], case["query"])] = (case["conflict"], case["b1r"])
    out = {"scale_factor": sf, "rows": len(rows), "files": nfiles, "engine": f"Spark {spark.version}, delta-spark 4.0.1",
           "deletion_vectors": True, "suites": {}}
    deletes = {name: (lambda clause: (lambda t, _table: spark.sql(f"DELETE FROM {t} WHERE {clause}")))(clause)
               for name, clause in _delete_refreshes(rows).items()}
    for suite, refreshes in (("delete_pairs", deletes), ("update_pairs", _update_refreshes(rows, spark))):
        cases = []
        for rf_name, refresh in refreshes.items():
            for q_name, q_sql in QUERIES.items():
                started = time.perf_counter()
                flagged, reason = runner.blocked(base, q_sql, refresh)
                truth, b1r = labels[(rf_name, q_name)]
                cases.append({"refresh": rf_name, "query": q_name, "conflict": truth, "delta": int(flagged),
                              "b1r_rule": b1r, "reason": reason, "seconds": round(time.perf_counter() - started, 2)})
                print(f"  {rf_name:<22} {q_name:<4} truth={truth} delta={int(flagged)} b1r_rule={b1r} {reason}",
                      flush=True)
        out["suites"][suite] = {"cases": cases, "delta": _score(cases, "delta"), "b1r_rule": _score(cases, "b1r_rule"),
                                "agree_with_rule": [sum(c["delta"] == c["b1r_rule"] for c in cases), len(cases)]}
        print(suite, out["suites"][suite]["delta"], "rule", out["suites"][suite]["b1r_rule"],
              "agree", out["suites"][suite]["agree_with_rule"], flush=True)
    (RESULTS / "delta_spark_pairs.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    shutil.rmtree(work, ignore_errors=True)
    return out


if __name__ == "__main__":
    main()
