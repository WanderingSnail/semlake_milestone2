"""Rerun the correctness experiments whose committed results include external systems
(Dolt, lakeFS, Delta Lake on Spark) and check that the current code decides every case
exactly as the committed files say.

The committed files keep the external systems' columns, which need binaries or servers
this script does not start. Everything else (SemLake and its ablations, the rule models
and the reimplemented baselines) is recomputed and compared case by case:

    python3 benchmarks/verify_committed.py            # suite, TPC-H delete and update pairs
    python3 benchmarks/verify_committed.py --quick    # suite only

Exit status 1 if any decision differs. Writes ``results/verify_committed.json``.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

RESULTS = Path(__file__).resolve().parent / "results"
EXTERNAL = {"dolt", "dolt_detail"}


def _compare(label: str, committed: list[dict], fresh: list[dict], key, systems: list[str]) -> dict:
    old = {key(case): case for case in committed}
    new = {key(case): case for case in fresh}
    differences = []
    for case_key, case in new.items():
        if case_key not in old:
            differences.append({"case": list(case_key), "problem": "not in the committed file"})
            continue
        for name in systems:
            if name in case and name in old[case_key] and int(case[name]) != int(old[case_key][name]):
                differences.append({"case": list(case_key), "system": name,
                                    "committed": int(old[case_key][name]), "now": int(case[name])})
        if "conflict" in case and int(case["conflict"]) != int(old[case_key].get("conflict", case["conflict"])):
            differences.append({"case": list(case_key), "problem": "label changed"})
    missing = [list(k) for k in old if k not in new]
    print(f"{label}: {len(new)} cases, {len(differences)} differences, {len(missing)} not rerun")
    return {"cases": len(new), "systems": systems, "differences": differences, "not_rerun": missing}


def verify_suite() -> dict:
    from benchmarks.run_all import _run_semantic_suite

    fresh: list[dict] = []
    _run_semantic_suite(fresh)
    with (RESULTS / "m1s_cases.csv").open(newline="", encoding="utf-8") as handle:
        committed = list(csv.DictReader(handle))
    systems = [name for name in committed[0] if name not in EXTERNAL | {"name", "expect"}]
    report = _compare("labelled suite", committed, fresh, lambda c: (c["name"],), systems)
    labels = {c["name"]: int(c["expect"]) for c in committed}
    report["differences"] += [{"case": [c["name"]], "problem": "label changed"}
                              for c in fresh if labels.get(c["name"]) != int(c["expect"])]
    return report


def verify_pairs() -> dict:
    from benchmarks import tpch_experiments

    out = {}
    for label, run, name in (("TPC-H delete pairs", tpch_experiments.run_pairs, "tpch_pairs.json"),
                             ("TPC-H update pairs", tpch_experiments.run_update_pairs, "tpch_update_pairs.json")):
        committed = json.loads((RESULTS / name).read_text(encoding="utf-8"))
        scratch = f"_verify_{name}"
        fresh = run(include_dolt=False, out_name=scratch)
        (RESULTS / scratch).unlink(missing_ok=True)
        systems = [s for s in fresh["systems"] if s in committed["systems"]]
        out[name] = _compare(label, committed["cases"], fresh["cases"], lambda c: (c["refresh"], c["query"]), systems)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true", help="the labelled suite only")
    args = parser.parse_args()
    report = {"m1s_cases.csv": verify_suite()}
    if not args.quick:
        report.update(verify_pairs())
    total = sum(len(part["differences"]) for part in report.values())
    report["differences_total"] = total
    (RESULTS / "verify_committed.json").write_text(json.dumps(report, indent=1) + "\n", encoding="utf-8")
    print(f"{total} decisions differ from the committed results")
    sys.exit(1 if total else 0)


if __name__ == "__main__":
    main()
