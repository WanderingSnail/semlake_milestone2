"""Rerun only the M1 labelled suite and replace its rows in the saved results.

    python3 benchmarks/rerun_suite.py
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.run_all import FIELDS, _run_semantic_suite, _write_case_csv  # noqa: E402

RESULTS = Path(__file__).resolve().parent / "results"


def main() -> None:
    case_rows: list[dict] = []
    fresh = _run_semantic_suite(case_rows)
    _write_case_csv(RESULTS / "m1s_cases.csv", case_rows)

    csv_path = RESULTS / "benchmark_run_summary.csv"
    with csv_path.open(newline="", encoding="utf-8") as handle:
        kept = [row for row in csv.DictReader(handle) if row["workload"] != "M1S"]
    first = next(i for i, row in enumerate(kept) if row["workload"] not in ("W1", "W3", "W3C", "W2", "W2L", "W4", "W1S"))
    merged = kept[:first] + fresh + kept[first:]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(merged)

    trace_path = RESULTS / "traces.json"
    doc = json.loads(trace_path.read_text(encoding="utf-8"))
    records = [r for r in doc["records"] if r["workload"] != "M1S"]
    at = next(i for i, r in enumerate(records) if r["workload"] not in ("W1", "W3", "W3C", "W2", "W2L", "W4", "W1S"))
    doc["records"] = records[:at] + [{key: r[key] for key in FIELDS} for r in fresh] + records[at:]
    trace_path.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    print(f"replaced {len(fresh)} M1S rows")


if __name__ == "__main__":
    main()
