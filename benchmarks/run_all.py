"""Run every workload against SemLake and the four baselines, then write traces and plots.

Smoke test:  python benchmarks/run_all.py --quick
Full matrix: python benchmarks/run_all.py
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from benchmarks.plot import render
from benchmarks.profiler import cost_usd, estimated_latency_s, pricing_doc
from benchmarks.workloads import (
    measure_scan,
    workload_w1,
    workload_w1s,
    workload_w2,
    workload_w3,
    workload_w4,
)
from semlake.baselines import (
    WriteConflictException,
    copy_on_write,
    crdt_merge,
    file_level_occ,
    tuple_version_merge,
)
from semlake.merge import three_way_merge
from semlake.storage import Counters

FIELDS = [
    "workload",
    "system",
    "manifest_depth",
    "contention",
    "status",
    "merge_cpu_s",
    "estimated_latency_s",
    "s3_put",
    "s3_get",
    "s3_list",
    "s3_requests",
    "put_bytes",
    "cost_usd",
    "false_abort",
    "semantic_recall",
    "false_positive",
    "cell_conflicts",
    "file_conflicts",
    "scan_base_s",
    "scan_nodv_s",
    "scan_dv_s",
    "scan_overhead",
    "tps",
    "trials",
    "notes",
]

SYSTEMS = {
    "semlake": three_way_merge,
    "b1": file_level_occ,
    "b2": copy_on_write,
    "b3": tuple_version_merge,
    "b4": crdt_merge,
}

QUICK = dict(
    w1_rows=800,
    w2_files=8,
    w2_rows_per_file=8,
    w2_rhos=[0.0, 0.5, 1.0],
    w2_trials=8,
    manifest_depths=[64, 256],
    file_size=256,
    scan_rows=50_000,
    scan_repeats=5,
)

FULL = dict(
    w1_rows=2000,
    w2_files=16,
    w2_rows_per_file=16,
    w2_rhos=[0.0, 0.01, 0.05, 0.2, 0.5, 1.0],
    w2_trials=80,
    manifest_depths=[1000, 5000, 10000],
    file_size=4096,
    scan_rows=1_000_000,
    scan_repeats=7,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SemLake milestone-2 benchmark suite.")
    parser.add_argument("--quick", action="store_true", help="Small depths and fewer trials.")
    parser.add_argument("--results", type=Path, default=ROOT / "benchmarks" / "results")
    parser.add_argument("--plots", type=Path, default=ROOT / "benchmarks" / "plots")
    args = parser.parse_args()

    cfg = QUICK if args.quick else FULL
    records = run_suite(cfg)
    args.results.mkdir(parents=True, exist_ok=True)
    csv_path = args.results / "benchmark_run_summary.csv"
    json_path = args.results / "traces.json"
    _write_csv(csv_path, records)
    _write_json(json_path, records, quick=args.quick, cfg=cfg)
    figures = render(csv_path, args.plots)
    _print_table(records)
    assert_hypotheses(records)
    print(f"wrote {csv_path}")
    print(f"wrote {json_path}")
    for figure in figures:
        print(f"wrote {figure}")


def run_suite(cfg: dict) -> list[dict]:
    records = []
    print("W1 disjoint updates")
    records.extend(_run_scenario(workload_w1(cfg["w1_rows"])))
    print("W3 semantic delete")
    records.extend(_run_scenario(workload_w3()))
    print("W3C disjoint delete (control)")
    records.extend(_run_scenario(workload_w3(control=True)))
    print("W2 contention sweep")
    records.extend(
        _run_w2(cfg["w2_files"], cfg["w2_rows_per_file"], cfg["w2_rhos"], cfg["w2_trials"])
    )
    for depth in cfg["manifest_depths"]:
        print(f"W4 manifest depth {depth}")
        records.extend(_run_scenario(workload_w4(depth, cfg["file_size"])))
        print(f"W1S manifest depth {depth}")
        records.extend(_run_scenario(workload_w1s(depth, cfg["file_size"])))
    print("M4 scan")
    records.append(_scan_record(measure_scan(cfg["scan_rows"], cfg["scan_repeats"])))
    return records


def _run_scenario(scenario) -> list[dict]:
    rows = []
    for system, merge_fn in SYSTEMS.items():
        outcome = _run_once(scenario.repo, merge_fn, scenario.left_id, scenario.right_id)
        rows.append(_scenario_record(scenario, system, outcome))
        print(
            f"  {system:8} {outcome['status']:20} "
            f"cpu={outcome['cpu_s']:.4f}s requests={outcome['counters'].requests} "
            f"put_bytes={outcome['counters'].put_bytes}"
        )
    return rows


def _run_w2(n_files: int, rows_per_file: int, rhos: list[float], trials: int) -> list[dict]:
    repo, _base_id, pairs = workload_w2(n_files, rows_per_file, rhos, trials)
    rows = []
    for rho, fork_pairs in pairs.items():
        for system, merge_fn in SYSTEMS.items():
            outcome = _run_trials(repo, merge_fn, fork_pairs)
            rows.append(
                _record(
                    workload="W2",
                    system=system,
                    manifest_depth=n_files,
                    contention=rho,
                    outcome=outcome,
                    trials=trials,
                    notes=f"success {outcome['ok']}/{trials}; hotspot row updates",
                    tps=outcome["tps"],
                )
            )
            print(f"  rho={rho:<4} {system:8} tps={outcome['tps']:.2f} status={outcome['status']}")
    return rows


def _run_once(repo, merge_fn, left_id: str, right_id: str) -> dict:
    """Merge one fork pair on a copy of the store, so the counters cover the merge alone."""
    scratch = repo.store.clone()
    started = time.perf_counter()
    try:
        result = merge_fn(repo, left_id, right_id, scratch)
    except WriteConflictException as exc:
        return {
            "status": "aborted",
            "cpu_s": time.perf_counter() - started,
            "counters": scratch.counters,
            "cell_conflicts": 0,
            "file_conflicts": len(exc.files),
            "message": str(exc),
            "violations": [],
            "vsa_count": 0,
        }
    return {
        "status": result.status,
        "cpu_s": time.perf_counter() - started,
        "counters": scratch.counters,
        "cell_conflicts": result.cell_conflicts,
        "file_conflicts": 0,
        "message": result.message,
        "violations": result.violations,
        "vsa_count": len(result.vsa),
    }


def _run_trials(repo, merge_fn, pairs: list[tuple[str, str]]) -> dict:
    """Run every fork pair once. Return per-trial means and successful merges per CPU second."""
    # One untimed run first, so the first trial does not pay for imports and caches.
    _run_once(repo, merge_fn, *pairs[0])
    outcomes = [_run_once(repo, merge_fn, left_id, right_id) for left_id, right_id in pairs]
    n = len(outcomes)
    cpu_total = sum(item["cpu_s"] for item in outcomes)
    ok = sum(1 for item in outcomes if item["status"] in ("clean", "vsa"))
    statuses = {item["status"] for item in outcomes}
    mean = Counters(
        put=sum(item["counters"].put for item in outcomes) / n,
        get=sum(item["counters"].get for item in outcomes) / n,
        list=sum(item["counters"].list for item in outcomes) / n,
        put_bytes=sum(item["counters"].put_bytes for item in outcomes) / n,
    )
    return {
        "status": statuses.pop() if len(statuses) == 1 else "mixed",
        "cpu_s": cpu_total / n,
        "counters": mean,
        "cell_conflicts": sum(item["cell_conflicts"] for item in outcomes) / n,
        "file_conflicts": sum(item["file_conflicts"] for item in outcomes) / n,
        "tps": ok / cpu_total if cpu_total > 0 else 0.0,
        "ok": ok,
    }


def _scenario_record(scenario, system: str, outcome: dict) -> dict:
    """Build the CSV row for one scenario run and attach its violations for the JSON trace."""
    status = outcome["status"]
    metrics = {}
    if scenario.workload == "W1":
        metrics["false_abort"] = 1 if status == "aborted" else 0
    if scenario.workload == "W3":
        metrics["semantic_recall"] = 1 if status == "semantic_conflict" else 0
    if scenario.workload == "W3C":
        metrics["false_positive"] = 1 if status == "semantic_conflict" else 0
    notes = scenario.notes
    if outcome["message"]:
        notes = f"{notes}; {outcome['message']}"
    row = _record(
        workload=scenario.workload,
        system=system,
        manifest_depth=scenario.manifest_depth,
        contention=scenario.contention,
        outcome=outcome,
        trials=1,
        notes=notes,
        **metrics,
    )
    row["_violations"] = outcome["violations"]
    row["_vsa_count"] = outcome["vsa_count"]
    return row


def _record(workload, system, manifest_depth, contention, outcome, trials, notes, **metrics) -> dict:
    """Build one CSV row. Columns that do not apply to the workload stay blank."""
    counters = outcome["counters"]
    row = {key: "" for key in FIELDS}
    row.update(
        workload=workload,
        system=system,
        manifest_depth=manifest_depth,
        contention=contention,
        status=outcome["status"],
        merge_cpu_s=outcome["cpu_s"],
        estimated_latency_s=estimated_latency_s(outcome["cpu_s"], counters),
        s3_put=counters.put,
        s3_get=counters.get,
        s3_list=counters.list,
        s3_requests=counters.requests,
        put_bytes=counters.put_bytes,
        cost_usd=cost_usd(counters),
        cell_conflicts=outcome["cell_conflicts"],
        file_conflicts=outcome["file_conflicts"],
        trials=trials,
        notes=notes,
    )
    row.update(metrics)
    return row


def _scan_record(scan: dict) -> dict:
    deleted_rows_cost = _percent_slower(scan["scan_nodv_s"], scan["scan_base_s"])
    mask_cost = _percent_slower(scan["scan_dv_s"], scan["scan_nodv_s"])
    notes = (
        f"Q6-style scan on {scan['scan_rows']} synthetic rows, not an official TPC-H dbgen load; "
        f"roaring deletion vector hides {scan['deleted_rows']} rows; "
        f"baseline is the compacted table; "
        f"reading the deleted rows costs {deleted_rows_cost}, "
        f"reading and testing the mask column costs {mask_cost}"
    )
    if scan["scan_overhead"] > 0.08:
        notes += "; overhead is above the proposal's 8% target on this microbenchmark"
    row = {key: "" for key in FIELDS}
    row.update(
        workload="M4",
        system="semlake",
        status="measured",
        scan_base_s=scan["scan_base_s"],
        scan_nodv_s=scan["scan_nodv_s"],
        scan_dv_s=scan["scan_dv_s"],
        scan_overhead=scan["scan_overhead"],
        notes=notes,
    )
    return row


def _percent_slower(slow: float, fast: float) -> str:
    return f"{(slow - fast) / fast * 100:+.1f}%" if fast > 0 else "n/a"


def _write_csv(path: Path, records: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def _write_json(path: Path, records: list[dict], quick: bool, cfg: dict) -> None:
    public = []
    for record in records:
        item = {key: record[key] for key in FIELDS}
        if "_violations" in record:
            item["violations"] = record["_violations"]
            item["vsa_count"] = record["_vsa_count"]
        public.append(item)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "quick" if quick else "full",
        "config": cfg,
        "pricing": pricing_doc(),
        "records": public,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _print_table(records: list[dict]) -> None:
    print("")
    print(f"{'workload':<8} {'system':<8} {'status':<20} {'requests':>10} {'cost_usd':>12} {'tps':>10}")
    for record in records:
        tps = f"{record['tps']:.2f}" if record["tps"] != "" else ""
        cost = f"{record['cost_usd']:.6g}" if record["cost_usd"] != "" else ""
        requests = f"{record['s3_requests']:.0f}" if record["s3_requests"] != "" else ""
        print(
            f"{record['workload']:<8} {record['system']:<8} {record['status']:<20} "
            f"{requests:>10} {cost:>12} {tps:>10}"
        )


def assert_hypotheses(records: list[dict]) -> None:
    w1 = _by_system(records, "W1")
    _expect(w1["semlake"]["status"] == "clean" and w1["semlake"]["false_abort"] == 0,
            "W1 SemLake should merge disjoint rows", w1["semlake"])
    _expect(w1["b1"]["status"] == "aborted" and w1["b1"]["false_abort"] == 1,
            "W1 B1 should false-abort", w1["b1"])
    _expect(w1["b1"]["file_conflicts"] >= 1 and w1["b1"]["cell_conflicts"] == 0,
            "W1 B1 abort should report overlapping files and zero cell conflicts", w1["b1"])
    _expect(w1["b2"]["status"] == "clean", "W1 B2 should merge", w1["b2"])
    _expect(w1["b2"]["put_bytes"] > w1["semlake"]["put_bytes"],
            "W1 B2 should write more bytes than SemLake", w1["b2"])
    for system in ("b3", "b4"):
        _expect(w1[system]["status"] == "clean" and w1[system]["false_abort"] == 0,
                f"W1 {system} should merge disjoint rows", w1[system])
    _expect(w1["b3"]["put_bytes"] > w1["semlake"]["put_bytes"],
            "W1 B3 should write more version-map bytes than SemLake", w1["b3"])

    w3 = _by_system(records, "W3")
    _expect(w3["semlake"]["status"] == "semantic_conflict" and w3["semlake"]["semantic_recall"] == 1,
            "W3 SemLake should block the semantic conflict", w3["semlake"])
    for system in ("b1", "b2", "b3", "b4"):
        _expect(w3[system]["status"] == "clean" and w3[system]["semantic_recall"] == 0,
                f"W3 {system} should miss the semantic conflict", w3[system])
    for system, record in _by_system(records, "W3C").items():
        _expect(record["status"] == "clean" and record["false_positive"] == 0,
                f"W3C {system} should merge disjoint delete and read clauses", record)

    hot = _by_system(records, "W2", contention=1.0)
    cold = _by_system(records, "W2", contention=0.0)
    _expect(hot["b1"]["tps"] == 0.0, "W2 B1 throughput should collapse at rho=1", hot["b1"])
    _expect(hot["semlake"]["tps"] > 0.0 and hot["semlake"]["status"] == "vsa",
            "W2 SemLake should keep committing with a VSA node at rho=1", hot["semlake"])
    _expect(hot["b4"]["tps"] > 0.0 and hot["b4"]["status"] == "clean",
            "W2 B4 should commit at rho=1 by overwriting conflicts", hot["b4"])
    _expect(hot["b3"]["tps"] > 0.0, "W2 B3 should still commit at rho=1", hot["b3"])
    _expect(cold["b1"]["tps"] > 0.0, "W2 B1 should commit when branches touch different files", cold["b1"])

    w4 = _at_max_depth(records, "W4")
    _expect(w4["semlake"]["status"] == "clean" and w4["b2"]["status"] == "clean",
            "W4 schema-and-append merge should commit", w4)
    _expect(w4["b2"]["put_bytes"] < 1_000_000,
            "W4 B2 should not rewrite the table for a column add", w4["b2"])
    _expect(w4["semlake"]["merge_cpu_s"] * 10 < w4["b1"]["merge_cpu_s"],
            "W4 SemLake CPU should be at least 10x below B1's full file-list scan", w4)
    _expect(w4["b3"]["s3_requests"] > w4["semlake"]["s3_requests"],
            "W4 B3 should walk more records than SemLake", w4["b3"])

    w1s = _at_max_depth(records, "W1S")
    _expect(0 < w1s["semlake"]["cost_usd"] <= 0.05 * w1s["b2"]["cost_usd"],
            "W1S SemLake request cost should be at most 5% of B2", w1s)
    _expect(w1s["semlake"]["s3_requests"] < w1s["b2"]["s3_requests"],
            "W1S SemLake should issue fewer S3 requests than B2", w1s)
    _expect(w1s["b1"]["status"] == "clean", "W1S disjoint files should not abort B1", w1s["b1"])
    print("hypothesis checks passed")


def _expect(condition: bool, message: str, context) -> None:
    if not condition:
        raise SystemExit(f"{message}, got {context}")


def _by_system(records: list[dict], workload: str, contention: float | None = None) -> dict:
    return {
        record["system"]: record
        for record in records
        if record["workload"] == workload
        and (contention is None or record["contention"] == contention)
    }


def _at_max_depth(records: list[dict], workload: str) -> dict:
    depth = max(record["manifest_depth"] for record in records if record["workload"] == workload)
    return {
        record["system"]: record
        for record in records
        if record["workload"] == workload and record["manifest_depth"] == depth
    }


if __name__ == "__main__":
    main()
