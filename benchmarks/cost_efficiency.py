"""Dollar cost of one merged change.

Request prices come from ``benchmarks.profiler``. Storage and CPU rates are
in ``ASSUMPTIONS``. Scan overhead is read from the M4 JSON when that file
exists. B2 is priced as one rewrite of a 128 MiB file. SemLake is priced
from the bytes a merge PUT.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from semlake.baselines import WriteConflictException, copy_on_write, file_level_occ
from semlake.merge import three_way_merge
from semlake.metadata import Repository, Schema, SchemaField, Snapshot
from semlake.storage import MemoryObjectStore

from benchmarks.profiler import BANDWIDTH_BPS, PRICE_GET_USD, PRICE_PUT_USD, cost_usd

ASSUMPTIONS = {
    "storage_usd_per_gb_month": 0.023,
    "cpu_usd_per_hour": 0.10,
    "agent_usd_per_hour": 1.00,
    "rewritten_file_bytes": 128 * 1024 * 1024,
    "day_fraction_of_month": 1 / 30,
}


def _schema() -> Schema:
    return Schema(
        (
            SchemaField("id", "int64"),
            SchemaField("kind", "string"),
            SchemaField("value", "int64"),
        )
    )


def _table(n_rows: int, rows_per_file: int) -> tuple[Repository, Snapshot]:
    repo = Repository(MemoryObjectStore())
    rows = {i: {"id": i, "kind": "user", "value": i} for i in range(n_rows)}
    base = repo.create_table("main", _schema(), rows=rows, rows_per_file=rows_per_file)
    return repo, base


def long_lived_outcomes(n_agents: int = 8, rounds: int = 13, n_rows: int = 32) -> dict:
    """Count merge statuses for agents that keep their own branch.

    Each agent updates one private row, merges into main, and continues from
    its own commit. ``rounds`` passes of ``n_agents`` is the merge count.
    """
    repo, base = _table(n_rows, 1)
    head = base.snapshot_id
    branches = [head] * n_agents
    counts = {"clean": 0, "vsa": 0, "other": 0}
    for _round in range(rounds):
        for agent in range(n_agents):
            parent = repo.snapshots[branches[agent]]
            local = repo.apply(
                parent,
                f"a{agent}",
                updates={agent: {"value": parent.rows[agent]["value"] + 1}},
            )
            merged = three_way_merge(repo, local.snapshot_id, head, repo.store.clone())
            if merged.status in counts:
                counts[merged.status] += 1
            else:
                counts["other"] += 1
            if merged.snapshot_id and merged.status == "clean":
                head = merged.snapshot_id
            branches[agent] = local.snapshot_id
    total = sum(counts.values())
    return {**counts, "merges": total, "vsa_fraction": counts["vsa"] / total if total else 0.0}


def _day_storage(put_bytes: float) -> float:
    gb = put_bytes / (1024 ** 3)
    return gb * ASSUMPTIONS["storage_usd_per_gb_month"] * ASSUMPTIONS["day_fraction_of_month"]


def _cpu_usd(seconds: float) -> float:
    return seconds * ASSUMPTIONS["cpu_usd_per_hour"] / 3600


def sample_merge_io(n_merges: int = 16) -> dict:
    """Mean requests, bytes and CPU for one clean SemLake merge and one B2 rewrite."""
    repo, base = _table(32, 1)
    head = base.snapshot_id
    branches = [head] * 4
    semlake_rows = []
    for index in range(n_merges):
        agent = index % 4
        parent = repo.snapshots[branches[agent]]
        local = repo.apply(
            parent,
            f"a{agent}",
            updates={agent: {"value": parent.rows[agent]["value"] + 1}},
        )
        scratch = repo.store.clone()
        started = time.perf_counter()
        merged = three_way_merge(repo, local.snapshot_id, head, scratch)
        elapsed = time.perf_counter() - started
        if merged.status != "clean" or not merged.snapshot_id:
            raise RuntimeError(f"expected a clean merge, got {merged.status}")
        semlake_rows.append((scratch.counters, elapsed))
        head = merged.snapshot_id
        branches[agent] = local.snapshot_id

    cow_repo, cow_base = _table(32, 1)
    left = cow_repo.apply(cow_base, "A", updates={0: {"value": 10}})
    right = cow_repo.apply(cow_base, "B", updates={1: {"value": 11}})
    cow_store = cow_repo.store.clone()
    started = time.perf_counter()
    cow = copy_on_write(cow_repo, left.snapshot_id, right.snapshot_id, cow_store)
    cow_s = time.perf_counter() - started
    if cow.status != "clean":
        raise RuntimeError(cow.status)

    def mean(rows, attr):
        return sum(getattr(counters, attr) for counters, _elapsed in rows) / len(rows)

    return {
        "merges": n_merges,
        "semlake_requests": mean(semlake_rows, "requests"),
        "semlake_put_bytes": mean(semlake_rows, "put_bytes"),
        "semlake_cpu_s": sum(elapsed for _counters, elapsed in semlake_rows) / len(semlake_rows),
        "semlake_request_usd": sum(cost_usd(counters) for counters, _elapsed in semlake_rows) / len(semlake_rows),
        "b2_requests": cow_store.counters.requests,
        "b2_put_bytes_small": cow_store.counters.put_bytes,
        "b2_cpu_s": cow_s,
        "b2_request_usd": cost_usd(cow_store.counters),
    }


def same_file_abort() -> dict:
    """B1 aborts two edits of one file. SemLake commits them."""
    repo, base = _table(16, 16)
    left = repo.apply(base, "A", updates={0: {"value": 10}})
    right = repo.apply(base, "B", updates={1: {"value": 11}})
    scratch = repo.store.clone()
    started = time.perf_counter()
    try:
        file_level_occ(repo, left.snapshot_id, right.snapshot_id, scratch)
        aborted = False
    except WriteConflictException:
        aborted = True
    abort_s = time.perf_counter() - started
    sem = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
    return {
        "b1_aborts": aborted,
        "b1_attempt_s": abort_s,
        "b1_attempt_requests": scratch.counters.requests,
        "semlake_status": sem.status,
    }


def history_requests(depths: tuple[int, ...] = (1, 8, 32)) -> list[dict]:
    """Requests to merge one side commit after a main chain of ``depth`` commits.

    ``from_root`` diverged at the first snapshot, so every later commit is
    read. ``from_tip`` diverged at the current head, so the replay is the
    two new commits.
    """
    rows = []
    for depth in depths:
        for kind in ("from_root", "from_tip"):
            repo, base = _table(max(8, depth + 2), 1)
            head = base
            for step in range(depth):
                head = repo.apply(head, "main", updates={step: {"value": step + 1}})
            parent = base if kind == "from_root" else head
            side = repo.apply(parent, "side", updates={depth: {"value": -1}})
            other = repo.apply(head, "main2", updates={depth + 1: {"value": -2}})
            scratch = repo.store.clone()
            merged = three_way_merge(repo, side.snapshot_id, other.snapshot_id, scratch)
            rows.append({
                "history": depth,
                "fork": kind,
                "status": merged.status,
                "requests": scratch.counters.requests,
                "gets": scratch.counters.get,
                "put_bytes": scratch.counters.put_bytes,
            })
    return rows


def _overhead_anchors() -> dict:
    """Read the scan overhead fractions and the base-scan time.

    The preferred source is ``tpch_scan.json``: TPC-H Q6 over real SF 1
    lineitem with 1% localized and 1% uniform deletes. Without it the older
    synthetic runs (``m4_sequential.json``, ``m4_sf1.json``) are used.
    """
    results = Path(__file__).resolve().parent / "results"
    tpch = results / "tpch_scan.json"
    if tpch.exists():
        doc = json.loads(tpch.read_text(encoding="utf-8"))
        picked = {}
        for row in doc["results"]:
            if abs(row["density"] - 0.01) < 1e-9:
                picked[row["layout"]] = row
        if "localized" in picked and "uniform" in picked:
            return {"localized": picked["localized"]["overhead_median"],
                    "uniform": picked["uniform"]["overhead_median"],
                    "source": tpch.name, "scan_s": picked["localized"]["base_ms"] / 1e3, "sf1": None}
    path = results / "m4_sequential.json"
    localized = 0.102
    uniform = 0.75
    source = "fallback"
    if path.exists():
        rows = json.loads(path.read_text(encoding="utf-8"))
        source = str(path.name)
        for row in rows:
            if row.get("layout") == "w1" and abs(row.get("density", 0) - 0.01) < 1e-9:
                localized = row["scan_overhead"]
            if row.get("layout") == "uniform" and abs(row.get("density", 0) - 0.01) < 1e-9:
                uniform = row["scan_overhead"]
    scaled = results / "m4_sf1.json"
    sf1 = None
    if scaled.exists():
        sf1 = json.loads(scaled.read_text(encoding="utf-8"))
    return {"localized": localized, "uniform": uniform, "source": source, "scan_s": None, "sf1": sf1}


def _rewrite_usd(file_bytes: float) -> float:
    """One PUT of ``file_bytes``, a day of storage, and the transfer time."""
    transfer_s = file_bytes / BANDWIDTH_BPS
    return PRICE_PUT_USD + _day_storage(file_bytes) + _cpu_usd(transfer_s)


def dollars_per_1k(
    io: dict,
    reads_per_merge: float,
    overhead: float,
    scan_s: float,
    redo_s: float,
) -> dict:
    """USD for 1,000 merges plus ``reads_per_merge`` scans after each one.

    B2 pays for one 128 MiB rewrite per merge. SemLake pays for the PUT bytes
    measured on the small merge. A plain scan is one GET plus the measured
    CPU. An open deletion vector stretches that CPU by ``overhead``.
    Compacting on every merge clears the overhead and rewrites one file.
    ``redo_s`` is the measured time of one aborted file-level attempt.
    """
    file_bytes = ASSUMPTIONS["rewritten_file_bytes"]
    sem_write = (
        io["semlake_request_usd"]
        + _day_storage(io["semlake_put_bytes"])
        + _cpu_usd(io["semlake_cpu_s"])
    )
    b2_write = io["b2_request_usd"] + _rewrite_usd(file_bytes) + _cpu_usd(io["b2_cpu_s"])
    read_plain = reads_per_merge * (PRICE_GET_USD + _cpu_usd(scan_s))
    read_open = reads_per_merge * (PRICE_GET_USD + _cpu_usd(scan_s * (1 + overhead)))
    sem_open = sem_write + read_open
    sem_compacted = sem_write + _rewrite_usd(file_bytes) + read_plain
    b1 = b2_write + read_plain + redo_s * ASSUMPTIONS["agent_usd_per_hour"] / 3600
    return {
        "reads_per_merge": reads_per_merge,
        "overhead": overhead,
        "semlake_open_usd": sem_open * 1000,
        "semlake_compact_each_merge_usd": sem_compacted * 1000,
        "b2_usd": (b2_write + read_plain) * 1000,
        "b1_usd": b1 * 1000,
    }


def crossover(io: dict, scan_s: float, overhead: float) -> float | None:
    """Reads per merge where an open deletion vector costs as much as B2's rewrite.

    Returns None when the open scan is already more expensive at zero reads.
    """
    file_bytes = ASSUMPTIONS["rewritten_file_bytes"]
    sem_fixed = io["semlake_request_usd"] + _day_storage(io["semlake_put_bytes"]) + _cpu_usd(io["semlake_cpu_s"])
    b2_fixed = io["b2_request_usd"] + _rewrite_usd(file_bytes) + _cpu_usd(io["b2_cpu_s"])
    extra_per_read = _cpu_usd(scan_s * overhead)
    if extra_per_read <= 0:
        return None
    gap = b2_fixed - sem_fixed
    if gap <= 0:
        return 0.0
    return gap / extra_per_read


def compaction_policies(io: dict, scan_s: float, overhead: float, reads_per_merge: float) -> list[dict]:
    """Steady cost when a file is rewritten once its deletion vector passes ``d``.

    Each merge deletes 0.1% of one 128 MiB file. Scans after the earlier
    merges in a cycle pay ``overhead``. The rewrite runs on the last merge,
    and the scans after that merge are plain.
    """
    file_bytes = ASSUMPTIONS["rewritten_file_bytes"]
    deletes_per_merge = 0.001
    rows = []
    for threshold in (0.001, 0.01, 0.05, 0.20):
        merges_per_cycle = max(1, round(threshold / deletes_per_merge))
        # Compact on the last merge of the cycle. Reads after that merge are
        # plain. Reads after the earlier merges still see the deletion vector.
        dirty_reads = reads_per_merge * (merges_per_cycle - 1)
        plain_reads = reads_per_merge * 1.0
        rewrite = _rewrite_usd(file_bytes)
        cycle_usd = (
            merges_per_cycle * (io["semlake_request_usd"] + _day_storage(io["semlake_put_bytes"]) + _cpu_usd(io["semlake_cpu_s"]))
            + dirty_reads * (_cpu_usd(scan_s * (1 + overhead)) + PRICE_GET_USD)
            + plain_reads * (_cpu_usd(scan_s) + PRICE_GET_USD)
            + rewrite
        )
        rows.append({
            "threshold": threshold,
            "merges_per_compaction": merges_per_cycle,
            "usd_per_merge": cycle_usd / merges_per_cycle,
            "compaction_bytes_per_merge": file_bytes / merges_per_cycle,
            "read_overhead_while_dirty": overhead,
        })
    return rows


def build_report(scan_s: float | None = None) -> dict:
    """Measure the swarm, the I/O samples, and the dollar curves."""
    anchors = _overhead_anchors()
    if scan_s is None:
        scan_s = anchors["scan_s"] or 0.015
        if anchors["scan_s"] is None and anchors["sf1"]:
            for row in anchors["sf1"]:
                if row.get("layout") == "w1":
                    scan_s = row["scan_base_s"]
    outcomes = long_lived_outcomes()
    io = sample_merge_io()
    abort = same_file_abort()
    localized = anchors["localized"]
    uniform = anchors["uniform"]
    curves = []
    for reads in (0, 1, 10, 100, 1000):
        curves.append({
            "localized": dollars_per_1k(io, reads, localized, scan_s, abort["b1_attempt_s"]),
            "uniform": dollars_per_1k(io, reads, uniform, scan_s, abort["b1_attempt_s"]),
        })
    return {
        "assumptions": ASSUMPTIONS,
        "scan_base_s": scan_s,
        "overhead": {
            "localized": localized,
            "uniform": uniform,
            "fraction_source": anchors["source"],
            "scan_source": anchors["source"] if anchors["scan_s"] else ("m4_sf1.json" if anchors["sf1"] else "fallback"),
        },
        "long_lived_swarm": outcomes,
        "merge_io": io,
        "same_file": abort,
        "history": history_requests(),
        "crossover_reads_per_merge": {
            "localized": crossover(io, scan_s, localized),
            "uniform": crossover(io, scan_s, uniform),
        },
        "usd_per_1k": curves,
        "compaction": {
            "localized": compaction_policies(io, scan_s, localized, reads_per_merge=10),
            "uniform": compaction_policies(io, scan_s, uniform, reads_per_merge=10),
        },
        "gb_per_commit": {
            "semlake": io["semlake_put_bytes"] / (1024 ** 3),
            "b2_scaled_file": ASSUMPTIONS["rewritten_file_bytes"] / (1024 ** 3),
        },
    }


def write_report(path: Path | None = None) -> dict:
    report = build_report()
    target = path or Path(__file__).resolve().parent / "results" / "cost_efficiency.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def run_scaled_m4(n_rows: int, repeats: int, processes: int, path: Path | None = None) -> list[dict]:
    """Time localized and uniform deletes at ``n_rows``. Processes run one after another."""
    from benchmarks.workloads import _scan_in_process, _scan_once, _summarize_scan

    layouts = (("w1", 0.01), ("uniform", 0.01))
    width = max(1, min(100_000, n_rows // 60 if n_rows >= 60 else n_rows))
    grouped: dict[tuple, list[dict]] = {}
    for layout, density in layouts:
        task = {
            "n_rows": n_rows,
            "repeats": repeats,
            "group_rows": width,
            "layout": layout,
            "density": density,
        }
        samples = []
        for _ in range(max(1, processes)):
            samples.append(_scan_in_process(task) if processes > 1 else _scan_once(task))
        grouped[(layout, density)] = samples
    rows = [_summarize_scan(grouped[key]) for key in layouts]
    target = path or Path(__file__).resolve().parent / "results" / "m4_sf1.json"
    target.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return rows


def main() -> None:
    report = write_report()
    swarm = report["long_lived_swarm"]
    cross = report["crossover_reads_per_merge"]
    print(
        f"long-lived merges {swarm['merges']} vsa {swarm['vsa']} "
        f"fraction {swarm['vsa_fraction']:.3f}"
    )
    print(
        "crossover reads/merge "
        f"localized {cross['localized']} uniform {cross['uniform']}"
    )
    print(f"history requests {[row['requests'] for row in report['history']]}")


if __name__ == "__main__":
    main()
