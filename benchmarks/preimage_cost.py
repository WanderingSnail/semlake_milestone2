"""What recording pre-images in commit summaries costs as a purge grows.

A purge deletes the oldest fraction of TPC-H lineitem (rows sorted by ship
date), and a concurrent report reads with a clause the static test cannot rule
out (a NOT form), so the merge runs the deleted-row check. For each fraction we
report the purge summary's size, the share of it taken by pre-images, the size
if pre-images kept only the columns some read clause uses, and the merge time.

Run: python -m benchmarks.preimage_cost [--sf 0.1]
"""

from __future__ import annotations

import argparse
import json
import statistics
import time

from benchmarks.tpch_experiments import _repo, _write, lineitem_rows

FRACTIONS = (0.001, 0.01, 0.05, 0.10)
# Columns that the seven report filters of the TPC-H pairs read.
FILTER_COLUMNS = ("l_shipdate", "l_discount", "l_quantity", "l_returnflag", "l_shipmode",
                  "l_commitdate", "l_receiptdate")
REPORT = ("SELECT sum(l_discount) FROM lineitem WHERE NOT (l_shipdate < 19950101) "
          "AND l_discount BETWEEN 0.05 AND 0.07")


def run(sf: float = 0.1, repeats: int = 3) -> dict:
    from semlake.merge import three_way_merge
    from semlake.sql_predicates import predicates_from_sql

    rows = lineitem_rows(sf)
    repo, base = _repo(rows)
    ordered = sorted(rows)
    report = repo.apply(base, "report", predicates=predicates_from_sql(REPORT, "report"))
    out_rows = []
    for fraction in FRACTIONS:
        count = int(len(ordered) * fraction)
        cutoff = rows[ordered[count]]["l_shipdate"]
        doomed = [rid for rid in ordered if rows[rid]["l_shipdate"] < cutoff]
        sql = f"DELETE FROM lineitem WHERE l_shipdate < {cutoff}"
        purge = repo.apply(base, f"purge_{fraction}", deletes=doomed,
                           predicates=predicates_from_sql(sql, "purge"))
        doc = json.loads(repo.store.get(purge.summary_key))
        total = len(json.dumps(doc, separators=(",", ":")))
        before = doc.get("before", {})
        before_bytes = len(json.dumps(before, separators=(",", ":")))
        projected = {rid: {c: v for c, v in vals.items() if c in FILTER_COLUMNS} for rid, vals in before.items()}
        projected_bytes = len(json.dumps(projected, separators=(",", ":")))
        times, status, reads = [], "", 0
        for _ in range(repeats + 1):
            started = time.perf_counter()
            result = three_way_merge(repo, purge.snapshot_id, report.snapshot_id, repo.store.clone())
            times.append(time.perf_counter() - started)
            status, reads = result.status, result.base_row_reads
        row = {
            "fraction": fraction, "deleted_rows": len(doomed), "summary_bytes": total,
            "preimage_bytes": before_bytes, "projected_preimage_bytes": projected_bytes,
            "bytes_per_deleted_row": round(before_bytes / max(1, len(doomed)), 1),
            "merge_s": round(statistics.median(times[1:]), 3),
            "merge_low_s": round(min(times[1:]), 3), "merge_high_s": round(max(times[1:]), 3),
            "status": status, "base_row_reads": reads,
        }
        out_rows.append(row)
        print(f"  {fraction:>6.1%} deleted={len(doomed):<7} summary={total / 1e6:.1f} MB "
              f"pre-images={before_bytes / 1e6:.1f} MB projected={projected_bytes / 1e6:.1f} MB "
              f"merge={row['merge_s']:.2f}s {status}", flush=True)
    out = {"scale_factor": sf, "rows": len(rows), "report": REPORT, "filter_columns": FILTER_COLUMNS,
           "repeats": repeats, "rows_out": out_rows}
    _write("preimage_cost.json", out)
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sf", type=float, default=0.1)
    args = parser.parse_args()
    run(args.sf)
