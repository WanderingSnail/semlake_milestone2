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
    measure_history,
    measure_scan,
    run_swarm,
    semantic_cases,
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
    "merge_cpu_low_s",
    "merge_cpu_high_s",
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
    scan_repeats=3,
    scan_processes=1,
    history_marks=[40],
    swarm_writers=[4],
    swarm_rounds=3,
    swarm_rhos=[0.0, 1.0],
    w2l_files=64,
    w2l_rows_per_file=4,
    w2l_rhos=[0.0, 0.05, 0.2],
    w2l_trials=2,
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
    scan_repeats=5,
    scan_processes=5,
    history_marks=[40, 120, 240, 360],
    swarm_writers=[4, 16],
    swarm_rounds=6,
    swarm_rhos=[0.0, 1.0],
    w2l_files=10_000,
    w2l_rows_per_file=1,
    w2l_rhos=[0.0, 0.01, 0.05, 0.2],
    w2l_trials=8,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SemLake milestone-2 benchmark suite.")
    parser.add_argument("--quick", action="store_true", help="Small depths and fewer trials.")
    parser.add_argument("--results", type=Path, default=ROOT / "benchmarks" / "results")
    parser.add_argument("--plots", type=Path, default=ROOT / "benchmarks" / "plots")
    args = parser.parse_args()

    cfg = QUICK if args.quick else FULL
    case_rows: list[dict] = []
    records = run_suite(cfg, case_rows)
    args.results.mkdir(parents=True, exist_ok=True)
    csv_path = args.results / "benchmark_run_summary.csv"
    json_path = args.results / "traces.json"
    _write_csv(csv_path, records)
    _write_json(json_path, records, quick=args.quick, cfg=cfg)
    if case_rows:
        case_path = args.results / "m1s_cases.csv"
        _write_case_csv(case_path, case_rows)
        print(f"wrote {case_path}")
    figures = render(csv_path, args.plots)
    _print_table(records)
    assert_hypotheses(records)
    print(f"wrote {csv_path}")
    print(f"wrote {json_path}")
    for figure in figures:
        print(f"wrote {figure}")


def run_suite(cfg: dict, case_rows: list[dict] | None = None) -> list[dict]:
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
    print(f"W2L contention at {cfg['w2l_files']} files")
    records.extend(
        _run_w2(
            cfg["w2l_files"],
            cfg["w2l_rows_per_file"],
            cfg["w2l_rhos"],
            cfg["w2l_trials"],
            workload="W2L",
        )
    )
    for depth in cfg["manifest_depths"]:
        print(f"W4 manifest depth {depth}")
        records.extend(_run_scenario(workload_w4(depth, cfg["file_size"])))
        print(f"W1S manifest depth {depth}")
        records.extend(_run_scenario(workload_w1s(depth, cfg["file_size"])))
    print("M1 labelled suite")
    records.extend(_run_semantic_suite(case_rows if case_rows is not None else []))
    print("W5 compare-and-swap")
    for writers in cfg["swarm_writers"]:
        for rho in cfg["swarm_rhos"]:
            print(f"  writers {writers} rho {rho}")
            records.extend(_swarm_records(run_swarm(writers, cfg["swarm_rounds"], rho)))
    print("history length")
    records.extend(_history_records(measure_history(cfg["history_marks"])))
    print("M4 scan")
    for scan in measure_scan(cfg["scan_rows"], cfg["scan_repeats"], cfg["scan_processes"]):
        records.append(_scan_record(scan))
        print(
            f"  {scan['layout']:8} density={scan['density']:.1%} "
            f"overhead={scan['scan_overhead'] * 100:.1f}%"
        )
    return records


SCENARIO_REPEATS = 5


def _run_scenario(scenario, repeats: int | None = None) -> list[dict]:
    """Merge the scenario ``repeats`` times per system; report the median CPU and its range."""
    repeats = repeats or SCENARIO_REPEATS
    rows = []
    for system, merge_fn in SYSTEMS.items():
        _run_once(scenario.repo, merge_fn, scenario.left_id, scenario.right_id)  # warm-up, untimed
        runs = [
            _run_once(scenario.repo, merge_fn, scenario.left_id, scenario.right_id)
            for _ in range(repeats)
        ]
        times = sorted(item["cpu_s"] for item in runs)
        outcome = dict(runs[0])
        outcome["cpu_s"] = times[len(times) // 2]
        outcome["cpu_low_s"] = times[0]
        outcome["cpu_high_s"] = times[-1]
        outcome["repeats"] = repeats
        rows.append(_scenario_record(scenario, system, outcome))
        print(
            f"  {system:8} {outcome['status']:20} "
            f"cpu={outcome['cpu_s']:.4f}s requests={outcome['counters'].requests} "
            f"put_bytes={outcome['counters'].put_bytes}"
        )
    return rows


def _run_w2(n_files: int, rows_per_file: int, rhos: list[float], trials: int, workload: str = "W2") -> list[dict]:
    repo, _base_id, pairs = workload_w2(n_files, rows_per_file, rhos, trials)
    rows = []
    for rho, fork_pairs in pairs.items():
        for system, merge_fn in SYSTEMS.items():
            outcome = _run_trials(repo, merge_fn, fork_pairs)
            rows.append(
                _record(
                    workload=workload,
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
        trials=outcome.get("repeats", 1),
        notes=notes,
        **metrics,
    )
    row["merge_cpu_low_s"] = outcome.get("cpu_low_s", "")
    row["merge_cpu_high_s"] = outcome.get("cpu_high_s", "")
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
    layout = "W1 layout, deletes in the first row group" if scan["layout"] == "w1" else "uniform deletes"
    notes = (
        f"Q6-style Parquet scan on {scan['scan_rows']} synthetic rows, ordered by row_id; "
        f"{layout}; density {scan['density']:.1%}; "
        f"deletion vector hides {scan['deleted_rows']} rows; "
        f"{scan['clean_groups']} of {scan['groups']} row groups have an empty deletion vector and are scanned unchanged; "
        f"dirty groups keep their data file and join a separate mask by position; "
        f"overhead {scan['scan_overhead'] * 100:.1f}% "
        f"(95% interval {scan['overhead_low'] * 100:.1f}% to {scan['overhead_high'] * 100:.1f}%, "
        f"{scan['processes']} process(es))"
    )
    if scan["layout"] == "w1" and scan["scan_overhead"] > 0.08:
        notes += "; overhead is above the proposal's 8% target on this microbenchmark"
    row = {key: "" for key in FIELDS}
    row.update(
        workload="M4",
        system="semlake",
        status="measured",
        scan_base_s=scan["scan_base_s"],
        scan_nodv_s=scan["scan_nodv_s"],
        scan_dv_s=scan["scan_dv_s"],
        contention=scan["density"],
        scan_overhead=scan["scan_overhead"],
        notes=notes,
    )
    return row


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

    for rho in (0.0,):
        matched = [
            record for record in records
            if record["workload"] == "W2L" and record["contention"] == rho
        ]
        if not matched or matched[0]["manifest_depth"] < 1000:
            continue
        by_name = {record["system"]: record for record in matched}
        _expect(
            by_name["semlake"]["merge_cpu_s"] * 10 < by_name["b1"]["merge_cpu_s"],
            f"W2L SemLake merge CPU at rho={rho} should be at least 10x below B1",
            by_name,
        )

    labelled = _by_system(records, "M1S")
    if labelled:
        _expect(labelled["semlake"]["semantic_recall"] == 1,
                "M1S SemLake should catch every real predicate conflict", labelled["semlake"])
        _expect(labelled["semlake"]["false_positive"] == 0,
                "M1S SemLake should clear an unparsed clause when the deleted rows miss the read",
                labelled["semlake"])
        for system in ("b1", "b2", "b3", "b4"):
            _expect(labelled[system]["semantic_recall"] == 0,
                    f"M1S {system} should miss predicate conflicts", labelled[system])
        if "dolt" in labelled:
            notes = labelled["dolt"]["notes"]
            _expect(labelled["dolt"]["false_positive"] == 0,
                    "M1S Dolt should not flag a case whose predicates do not conflict",
                    labelled["dolt"])
            _expect(labelled["dolt"]["semantic_recall"] < 1,
                    "M1S Dolt should miss some predicate conflicts", labelled["dolt"])
            _expect("foreign_key=caught" in notes, "M1S Dolt should catch the foreign key", labelled["dolt"])
            _expect("reads_where=0/" in notes, "M1S Dolt should miss WHERE overlaps", labelled["dolt"])
            _expect("rows_live=missed" in notes, "M1S Dolt should miss rows_live", labelled["dolt"])
            _expect("fresh_check=caught" in notes,
                    "M1S Dolt should catch a check the declaring branch can install",
                    labelled["dolt"])
            _expect("domain_check_rejected" in notes,
                    "M1S Dolt should refuse a check the existing rows already break",
                    labelled["dolt"])

    w1s = _at_max_depth(records, "W1S")
    _expect(0 < w1s["semlake"]["cost_usd"] <= 0.05 * w1s["b2"]["cost_usd"],
            "W1S SemLake request cost should be at most 5% of B2", w1s)
    _expect(w1s["semlake"]["s3_requests"] < w1s["b2"]["s3_requests"],
            "W1S SemLake should issue fewer S3 requests than B2", w1s)
    _expect(w1s["b1"]["status"] == "clean", "W1S disjoint files should not abort B1", w1s["b1"])
    print("hypothesis checks passed")


def _run_semantic_suite(case_rows: list[dict]) -> list[dict]:
    from benchmarks.dolt_baseline import dolt_binary, dolt_merge

    from benchmarks.rule_baselines import b1r_merge, ssi_merge

    cases = semantic_cases()
    systems = dict(SYSTEMS)
    systems["b1r"] = b1r_merge
    systems["b6"] = ssi_merge
    # Ablation: SemLake with the delete-time row-value post-check turned off.
    systems["semlake_static"] = lambda repo, left, right, store: three_way_merge(
        repo, left, right, store, post_check=False
    )
    if dolt_binary():
        systems["dolt"] = dolt_merge
    totals = {name: {"tp": 0, "fp": 0, "fn": 0, "tn": 0} for name in systems}
    for case in cases:
        row = {"name": case["name"], "expect": case["expect"], "dolt_detail": ""}
        for name, merge_fn in systems.items():
            try:
                result = merge_fn(case["repo"], case["left_id"], case["right_id"], case["repo"].store.clone())
                predicted = result.status == "semantic_conflict"
                if name == "dolt":
                    row["dolt_detail"] = result.message
            except WriteConflictException:
                predicted = False
                aborted = True
            else:
                aborted = False
            # 1: semantic conflict reported, 2: write-conflict abort, 0: merged.
            row[name] = 1 if predicted else (2 if aborted else 0)
            expect = bool(case["expect"])
            if predicted and expect:
                totals[name]["tp"] += 1
            elif predicted and not expect:
                totals[name]["fp"] += 1
            elif expect:
                totals[name]["fn"] += 1
            else:
                totals[name]["tn"] += 1
        case_rows.append(row)
    rows = []
    for name, bucket in totals.items():
        tp, fp, fn, tn = bucket["tp"], bucket["fp"], bucket["fn"], bucket["tn"]
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        print(f"  {name:8} precision={precision:.2f} recall={recall:.2f} tp={tp} fp={fp} fn={fn}")
        notes = (
            f"{len(cases)} labelled cases; precision {precision:.3f}; recall {recall:.3f}; "
            f"tp {tp}; fp {fp}; fn {fn}; tn {tn}"
        )
        if name == "dolt":
            notes = f"{notes}; {_dolt_story(case_rows)}"
        rows.append(_record(
            workload="M1S",
            system=name,
            manifest_depth=len(cases),
            contention=0.0,
            outcome={
                "status": "measured",
                "cpu_s": 0.0,
                "counters": Counters(),
                "cell_conflicts": 0,
                "file_conflicts": 0,
            },
            trials=len(cases),
            notes=notes,
            semantic_recall=round(recall, 3),
            false_positive=fp,
        ))
    return rows


def _dolt_story(case_rows: list[dict]) -> str:
    """Summarise the Dolt column of the labelled suite for the trace notes."""
    by_name = {row["name"]: row for row in case_rows}
    where_cases = [row for row in case_rows if row["expect"] == 1 and "overlap" in row["name"]]
    where_caught = sum(row.get("dolt") == 1 for row in where_cases)

    def caught(name: str) -> str:
        return "caught" if by_name[name].get("dolt") == 1 else "missed"

    parts = [
        f"foreign_key={caught('foreign key')}",
        f"reads_where={where_caught}/{len(where_cases)}",
        f"rows_live={caught('rows live')}",
        f"fresh_check={caught('check on a satisfied column')}",
    ]
    domain = by_name["domain violation"]
    if "check_rejected" in domain.get("dolt_detail", ""):
        parts.append("domain_check_rejected")
    return "; ".join(parts)


def _write_case_csv(path: Path, case_rows: list[dict]) -> None:
    fields = ["name", "expect", "semlake", "semlake_static", "b1", "b1r", "b2", "b3", "b4", "b6", "dolt", "dolt_detail"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in case_rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _swarm_records(samples: list[dict]) -> list[dict]:
    rows = []
    for item in samples:
        print(
            f"    {item['system']:8} tps={item['tps']:.2f} "
            f"retries/commit={item['retries_per_commit']:.2f} p99={item['p99_s']:.4f}s"
        )
        rows.append(_record(
            workload="W5",
            system=item["system"],
            manifest_depth=item["writers"],
            contention=item["contention"],
            outcome={
                "status": "measured",
                "cpu_s": item["p99_s"],
                "counters": Counters(),
                "cell_conflicts": 0,
                "file_conflicts": 0,
            },
            trials=item["rounds"],
            notes=(
                f"retries/commit {item['retries_per_commit']:.2f}; "
                f"p99 {item['p99_s']:.6f}s; successes {item['successes']}; "
                f"applied {item['applied']}; parked {item['parked']}"
            ),
            tps=item["tps"],
        ))
    return rows


def _history_records(samples: list[dict]) -> list[dict]:
    rows = []
    for item in samples:
        print(
            f"  snapshots {item['snapshots']} "
            f"lca={item['lca_s'] * 1e3:.3f}ms merge={item['merge_s'] * 1e3:.2f}ms"
        )
        for system, seconds in (("lca", item["lca_s"]), ("semlake", item["merge_s"])):
            rows.append(_record(
                workload="H1",
                system=system,
                manifest_depth=item["snapshots"],
                contention=0.0,
                outcome={
                    "status": "measured",
                    "cpu_s": seconds,
                    "counters": Counters(),
                    "cell_conflicts": 0,
                    "file_conflicts": 0,
                },
                trials=30 if system == "lca" else 5,
                notes="merge-heavy history; lca is the mean of 30 calls, semlake is the median of 5 merges",
            ))
            if system == "semlake":
                rows[-1]["merge_cpu_low_s"] = item["merge_low_s"]
                rows[-1]["merge_cpu_high_s"] = item["merge_high_s"]
    return rows


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
