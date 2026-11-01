"""Score the real lakeFS merge on the labelled suite and the TPC-H pairs.

B1's rule model runs next to it on the same pairs, so the comparison also
checks that the rule model decides as the real system does.

Run (with a lakeFS server up): python -m benchmarks.lakefs_runs
"""

from __future__ import annotations

import json

from benchmarks.lakefs_baseline import lakefs_available, lakefs_merge
from benchmarks.tpch_experiments import RESULTS, run_pairs, run_update_pairs
from benchmarks.workloads import semantic_cases
from semlake.baselines import WriteConflictException, file_level_occ


def _decide(fn, repo, left, right) -> int:
    """1: semantic conflict, 2: write-conflict abort, 0: merged."""
    try:
        result = fn(repo, left, right, repo.store.clone())
    except WriteConflictException:
        return 2
    return 1 if result.status == "semantic_conflict" else 0


def run_suite() -> dict:
    rows = []
    for case in semantic_cases():
        repo = case["repo"]
        row = {"name": case["name"], "expect": case["expect"]}
        for name, fn in (("lakefs", lakefs_merge), ("b1", file_level_occ)):
            row[name] = _decide(fn, repo, case["left_id"], case["right_id"])
        rows.append(row)
        print(f"  {case['name']:<40} expect={case['expect']} lakefs={row['lakefs']} b1={row['b1']}", flush=True)
    summary = {}
    for name in ("lakefs", "b1"):
        tp = sum(1 for r in rows if r[name] == 1 and r["expect"])
        fp = sum(1 for r in rows if r[name] == 1 and not r["expect"])
        summary[name] = {"tp": tp, "fp": fp, "aborts": sum(1 for r in rows if r[name] == 2),
                         "recall": tp / max(1, sum(r["expect"] for r in rows))}
    agree = sum(1 for r in rows if r["lakefs"] == r["b1"])
    return {"cases": rows, "summary": summary, "agree_with_b1": agree, "total": len(rows)}


def main() -> None:
    if not lakefs_available():
        raise SystemExit("no lakeFS server: docker run --network host treeverse/lakefs run --quickstart")
    systems = {"lakefs": lakefs_merge, "b1": file_level_occ}
    suite = run_suite()
    deletes = run_pairs(systems=systems, out_name="lakefs_tpch_pairs.json")
    updates = run_update_pairs(systems=systems, out_name="lakefs_tpch_update_pairs.json")

    def agreement(doc):
        return sum(1 for c in doc["cases"] if c["lakefs"] == c["b1"]), len(doc["cases"])

    out = {
        "suite": suite,
        "tpch_pairs": {"summary": deletes["summary"], "agree_with_b1": agreement(deletes)},
        "tpch_update_pairs": {"summary": updates["summary"], "agree_with_b1": agreement(updates)},
    }
    (RESULTS / "lakefs.json").write_text(json.dumps(out, indent=1))
    print(json.dumps({k: v if k != "suite" else v["summary"] for k, v in out.items()}, indent=1))


if __name__ == "__main__":
    main()
