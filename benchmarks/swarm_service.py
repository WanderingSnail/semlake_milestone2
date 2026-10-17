"""A merge service for agents that share one table head.

The catalog compare-and-swap is charged ``CATALOG_RTT_S`` in the timing
model. The charge is added after the run. The process does not sleep. One
CAS per applied commit stays near five commits per second. Group commit
folds a batch of merges into one CAS.
"""

from __future__ import annotations

import statistics
import time

from semlake.merge import three_way_merge
from semlake.metadata import Repository, Schema, SchemaField
from semlake.storage import MemoryObjectStore
from semlake.verification import Predicate

CATALOG_RTT_S = 0.2


def _schema() -> Schema:
    return Schema(
        (
            SchemaField("id", "int64"),
            SchemaField("kind", "string"),
            SchemaField("value", "int64"),
        )
    )


def run_service(
    n_agents: int,
    rounds: int,
    seed: int,
    group_size: int = 1,
    n_rows: int = 32,
    rtt_s: float = CATALOG_RTT_S,
    heads: str = "single",
) -> dict:
    """Publish ``n_agents * rounds`` disjoint updates through one head.

    ``heads="partition"`` gives each row its own compare-and-swap. The catalog
    charge is one round trip times the longest partition, because those heads
    move together. ``heads="single"`` charges every CAS on the one main head.
    """
    if heads == "partition":
        return _run_partitions(n_agents, rounds, seed, n_rows, rtt_s)
    repo = Repository(MemoryObjectStore())
    rows = {i: {"id": i, "kind": "user", "value": i} for i in range(n_rows)}
    repo.create_table("main", _schema(), rows=rows, rows_per_file=1)
    applied = 0
    parked = 0
    cas_count = 0
    started = time.perf_counter()
    batch: list[str] = []
    for _round in range(rounds):
        head = repo.heads["main"]
        for agent in range(n_agents):
            rid = (agent + seed) % n_rows
            parent = repo.snapshots[head]
            local = repo.apply(parent, f"a{agent}", updates={rid: {"value": parent.rows[rid]["value"] + 1}})
            batch.append(local.snapshot_id)
            if len(batch) >= group_size:
                applied, parked, cas_count = _publish_batch(repo, batch, applied, parked, cas_count)
                batch = []
                head = repo.heads["main"]
    if batch:
        applied, parked, cas_count = _publish_batch(repo, batch, applied, parked, cas_count)
    cpu_s = time.perf_counter() - started
    elapsed = cpu_s + cas_count * rtt_s
    return {
        "seed": seed,
        "group_size": group_size,
        "heads": "single",
        "applied": applied,
        "parked": parked,
        "cas_count": cas_count,
        "cpu_s": cpu_s,
        "elapsed_s": elapsed,
        "tps": applied / elapsed if elapsed else 0.0,
        "hazards": hazard_probe(seed),
    }


def _publish_batch(repo, batch: list[str], applied: int, parked: int, cas_count: int):
    """Fold every snapshot in ``batch`` onto the head, then move the head once."""
    current = repo.heads["main"]
    for snap_id in batch:
        if snap_id == current:
            applied += 1
            continue
        merged = three_way_merge(repo, snap_id, current, repo.store)
        if merged.status not in ("clean", "vsa") or not merged.snapshot_id:
            continue
        current = merged.snapshot_id
        if merged.status == "vsa":
            parked += 1
        else:
            applied += 1
    if repo.cas_head("main", repo.heads["main"], current):
        cas_count += 1
    return applied, parked, cas_count


def _run_partitions(n_agents: int, rounds: int, seed: int, n_rows: int, rtt_s: float) -> dict:
    """Publish the same updates onto one head per row."""
    repo = Repository(MemoryObjectStore())
    rows = {i: {"id": i, "kind": "user", "value": i} for i in range(n_rows)}
    repo.create_table("main", _schema(), rows=rows, rows_per_file=1)
    root = repo.heads["main"]
    for rid in range(n_rows):
        repo.heads[f"p{rid}"] = root
    applied = 0
    parked = 0
    depth = {rid: 0 for rid in range(n_rows)}
    started = time.perf_counter()
    for _round in range(rounds):
        for agent in range(n_agents):
            rid = (agent + seed) % n_rows
            branch = f"p{rid}"
            parent = repo.snapshots[repo.heads[branch]]
            local = repo.apply(parent, branch, updates={rid: {"value": parent.rows[rid]["value"] + 1}})
            kind = _cas_partition(repo, branch, local.snapshot_id)
            if kind == "parked":
                parked += 1
            elif kind == "applied":
                applied += 1
                depth[rid] += 1
    cpu_s = time.perf_counter() - started
    longest = max(depth.values()) if depth else 0
    elapsed = cpu_s + longest * rtt_s
    return {
        "seed": seed,
        "group_size": 1,
        "heads": "partition",
        "applied": applied,
        "parked": parked,
        "cas_count": longest,
        "cpu_s": cpu_s,
        "elapsed_s": elapsed,
        "tps": applied / elapsed if elapsed else 0.0,
        "hazards": hazard_probe(seed),
    }


def _cas_partition(repo, branch: str, snap_id: str) -> str:
    """Move one partition head. A lost swap becomes one 3-way merge."""
    expected = repo.heads[branch]
    if snap_id == expected:
        return "applied"
    if repo.cas_head(branch, expected, snap_id):
        return "applied"
    current = repo.heads[branch]
    merged = three_way_merge(repo, snap_id, current, repo.store)
    if merged.status not in ("clean", "vsa") or not merged.snapshot_id:
        return "lost"
    if not repo.cas_head(branch, current, merged.snapshot_id):
        return "lost"
    return "parked" if merged.status == "vsa" else "applied"


def hazard_probe(seed: int) -> int:
    """Return 1 when a delete/read conflict is lost after a chain of merges."""
    repo = Repository(MemoryObjectStore())
    rows = {i: {"id": i, "kind": "K", "value": i} for i in range(40)}
    base = repo.create_table("main", _schema(), rows=rows, rows_per_file=10)
    head = base
    for step in range(4):
        left = repo.apply(head, "L", updates={(step + seed) % 20: {"value": 1000 + step}})
        right = repo.apply(head, "R", updates={20 + step: {"value": -step}})
        merged = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store)
        if merged.status != "clean" or not merged.snapshot_id:
            return 1
        head = repo.snapshots[merged.snapshot_id]
    report = repo.apply(
        head,
        "report",
        predicates=(Predicate("report", "reads_where", where="kind = 'K' AND value >= 20"),),
    )
    purge = repo.apply(
        head,
        "purge",
        deletes=[25, 26],
        predicates=(Predicate("purge", "deletes_where", where="kind = 'K' AND value >= 25"),),
    )
    blocked = three_way_merge(repo, report.snapshot_id, purge.snapshot_id, repo.store)
    return 0 if blocked.status == "semantic_conflict" else 1


def run_seeds(seeds: tuple[int, ...] = (1, 2, 3, 4, 5), group_sizes: tuple[int, ...] = (1, 8)) -> list[dict]:
    """Run the service once per seed and group size."""
    rows = [
        run_service(n_agents=8, rounds=2, seed=seed, group_size=group)
        for seed in seeds
        for group in group_sizes
    ]
    rows.extend(
        run_service(n_agents=8, rounds=2, seed=seed, heads="partition")
        for seed in seeds
    )
    return rows


def summarize(rows: list[dict], group_size: int, heads: str = "single") -> dict:
    """Median throughput and the hazard total for one group size."""
    chosen = [
        row for row in rows
        if row["group_size"] == group_size and row.get("heads", "single") == heads
    ]
    rates = sorted(row["tps"] for row in chosen)
    mid = rates[len(rates) // 2]
    return {
        "group_size": group_size,
        "seeds": len(chosen),
        "tps_median": mid,
        "tps_low": rates[0],
        "tps_high": rates[-1],
        "hazards": sum(row["hazards"] for row in chosen),
        "tps_mean": statistics.fmean(rates),
    }
