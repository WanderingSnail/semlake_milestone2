"""Join the SF 0.1 TPC-H pair runs, which ran one refresh per process to fit in memory.

The four purge refreshes come from the first, single-process run, which printed every
verdict (results/tpch_pairs_sf0.1_purges.log) before it ran out of memory at rf2_a; the
rf2 refreshes and all update refreshes were rerun one per process.

    python benchmarks/merge_sf01_parts.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmarks.tpch_experiments import RESULTS, merge_parts  # noqa: E402

UPDATES = ["discount_fix_1994q1", "reship_sep1995", "return_40_orders", "requantity_40_orders",
           "rf1_insert_15_orders"]


def purges_from_log() -> str:
    template = json.loads((RESULTS / "tpch_pairs_sf0.1_rf2_a.json").read_text())
    cases = []
    for line in (RESULTS / "tpch_pairs_sf0.1_purges.log").read_text().splitlines():
        parts = line.split()
        if len(parts) < 3 or not parts[0].startswith("purge_"):
            continue
        case = {"refresh": parts[0], "query": parts[1], "source": "log of the single-process run"}
        for item in parts[2:]:
            key, value = item.split("=")
            case["conflict" if key == "truth" else key] = int(value)
        cases.append(case)
    assert len(cases) == 28, len(cases)
    doc = {**template, "cases": cases}
    name = "tpch_pairs_sf0.1_purges.json"
    (RESULTS / name).write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    return name


if __name__ == "__main__":
    merge_parts([purges_from_log(), "tpch_pairs_sf0.1_rf2_a.json", "tpch_pairs_sf0.1_rf2_b.json"],
                "tpch_pairs_sf0.1.json")
    merge_parts([f"tpch_update_pairs_sf0.1_{name}.json" for name in UPDATES], "tpch_update_pairs_sf0.1.json")
