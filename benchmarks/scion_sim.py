"""Scion-style agent swarm: many agents branch, work, and commit to main by compare-and-swap.

Discrete-event simulation in simulated seconds. SemLake runs on the real engine
(``Repository.apply``, ``three_way_merge``, ``cas_head``); its merge time is the
measured CPU plus the object-store requests it actually issued, priced with the
round trips in ``profiler.py``. The baselines are rule models of how each system
decides a lost race, charged with the same latency model:

- b1: file-level OCC (Iceberg default). Overlapping files abort and the agent redoes its work.
- b1r: Delta-style row-level concurrency with deletion vectors. Same-row writes abort, and a
  report aborts when a concurrent delete touched a file it read (ConcurrentDeleteReadException).
- b2: copy-on-write with row-level merge. Same conflicts as b1r without the read check, but
  every commit rewrites each touched file.

``semlake_group`` adds a merge service: agents send their local commit to the service, which
takes up to ``GROUP_SIZE`` waiting commits, merges them one by one onto the head with the real
engine, and publishes the batch with one catalog compare-and-swap. A commit that conflicts with
the head is sent back to its agent, which redoes the work. The service runs one batch at a time.

Every system redoes the agent's work on a true same-row conflict, so the systems differ only
in false conflicts, semantic checks and write cost. A report is a semantic hazard when a delete
committed after the report's base snapshot removed rows the report read; a system that commits
such a report has published an aggregate over deleted rows.

    python benchmarks/scion_sim.py                  # paper sweep: 8-64 agents, 5 seeds
    python benchmarks/scion_sim.py --quick          # smoke test
"""

from __future__ import annotations

import argparse
import json
import math
import multiprocessing
import os
import csv
import heapq
import random
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from benchmarks.profiler import (  # noqa: E402
    BANDWIDTH_BPS,
    PRICE_GET_USD,
    PRICE_LIST_USD,
    PRICE_PUT_USD,
    RTT_GET_S,
    RTT_LIST_S,
    RTT_PUT_S,
)
from semlake.merge import three_way_merge  # noqa: E402
from semlake.metadata import Repository, Schema, SchemaField  # noqa: E402
from semlake.storage import MemoryObjectStore  # noqa: E402
from semlake.verification import Predicate, where_overlap  # noqa: E402

SYSTEMS = ("semlake", "semlake_group", "b1", "b1r", "b2")
GROUP_SIZE = 8
N_FILES = 32  # files in the shared table
ROWS_PER_FILE = 32
WORK_MEAN_S = 2.0  # mean agent think time per task
PARALLEL = 16  # concurrent object-store requests per step
CATALOG_RTT_S = 0.010  # one conditional row update in the catalog database
FILE_BYTES = 128 * 1024 * 1024  # modelled data-file size for copy-on-write rewrites
KINDS = ("user", "user", "user", "user", "user", "user", "user", "user",
         "user", "user", "user", "user", "spam", "trial", "vip", "churn")
DELETE_KINDS = ("spam", "trial", "churn")
REPORT_KINDS = ("spam", "trial", "vip", "churn")


@dataclass
class Task:
    kind: str  # update | delete | report
    updates: dict = field(default_factory=dict)
    deletes: list = field(default_factory=list)
    insert_id: int = -1
    insert_row: dict = field(default_factory=dict)
    where: str = ""
    read_rows: frozenset = frozenset()
    read_files: frozenset = frozenset()
    rows: frozenset = frozenset()  # rows written (updated or deleted)
    files: frozenset = frozenset()  # existing files written


@dataclass
class Commit:
    version: int
    task: Task


@dataclass
class Agent:
    aid: int
    started: float = 0.0
    base_version: int = 0
    base_snap: str = ""
    expected_version: int = 0
    expected_snap: str = ""
    candidate: str = ""
    task: Task | None = None
    attempts: int = 0


class Stats:
    def __init__(self) -> None:
        self.commits = {"update": 0, "delete": 0, "report": 0}
        self.latency: list[float] = []
        self.redo_true = 0  # same-row conflicts, redone by every system
        self.redo_false = 0  # aborts with no row overlap (file-level false conflicts)
        self.redo_semantic = 0
        self.rebases = 0
        self.wasted_work_s = 0.0
        self.put = self.get = self.list = 0
        self.put_bytes = 0
        self.hazards_committed = 0
        self.hazards_caught = 0
        self.false_blocks = 0
        self.vsa_parked = 0

    def charge(self, put=0, get=0, lst=0, put_bytes=0) -> float:
        self.put += put
        self.get += get
        self.list += lst
        self.put_bytes += put_bytes
        # Requests in one step are issued in parallel, up to PARALLEL at a time, for every system.
        rounds = lambda n: -(-n // PARALLEL)  # noqa: E731
        return (rounds(put) * RTT_PUT_S + rounds(get) * RTT_GET_S + lst * RTT_LIST_S
                + put_bytes / BANDWIDTH_BPS)


class World:
    """Shared table state for one system."""

    def __init__(self, system: str, n_files: int, rows_per_file: int, seed: int) -> None:
        self.system = system
        self.rows: dict[int, dict] = {}
        self.row_file: dict[int, int] = {}
        n_rows = n_files * rows_per_file
        rng = random.Random(seed)
        for rid in range(n_rows):
            self.rows[rid] = {"id": rid, "kind": KINDS[rid % len(KINDS)], "value": rng.randrange(100)}
            self.row_file[rid] = rid // rows_per_file
        self.version = 0
        self.log: list[Commit] = []
        self.next_id = 10**9
        self.repo = None
        self.head_snap = ""
        if system.startswith("semlake"):
            self.repo = Repository(MemoryObjectStore())
            schema = Schema((SchemaField("id", "int64"), SchemaField("kind", "string"),
                             SchemaField("value", "int64")))
            base = self.repo.create_table("main", schema, rows=self.rows, rows_per_file=rows_per_file)
            self.head_snap = base.snapshot_id

    def since(self, version: int) -> list[Commit]:
        return self.log[version:]

    def apply(self, task: Task) -> None:
        for rid, cols in task.updates.items():
            if rid in self.rows:
                self.rows[rid] = {**self.rows[rid], **cols}
        for rid in task.deletes:
            self.rows.pop(rid, None)
        if task.insert_id >= 0:
            self.rows[task.insert_id] = dict(task.insert_row)
            self.row_file[task.insert_id] = -1
        self.log.append(Commit(self.version, task))
        self.version += 1


def make_task(world: World, rng: random.Random, mix: dict, hot_rho: float, hot_rows: list[int]) -> Task:
    roll = rng.random()
    if roll < mix["delete"]:
        k = rng.choice(DELETE_KINDS)
        cut = rng.randrange(5, 40)
        where = f"kind = '{k}' AND value < {cut}"
        rows = [rid for rid, row in world.rows.items() if row["kind"] == k and row["value"] < cut]
        return Task("delete", deletes=rows, where=where, rows=frozenset(rows),
                    files=frozenset(world.row_file[r] for r in rows))
    if roll < mix["delete"] + mix["report"]:
        k = rng.choice(REPORT_KINDS)
        low = rng.randrange(0, 90)
        where = f"kind = '{k}' AND value >= {low}"
        read = frozenset(rid for rid, row in world.rows.items()
                         if row["kind"] == k and row["value"] >= low and world.row_file.get(rid, -1) >= 0)
        total = sum(world.rows[r]["value"] for r in read)
        world.next_id += 1
        rid = world.next_id
        return Task("report", insert_id=rid, insert_row={"id": rid, "kind": "report", "value": total},
                    where=where, read_rows=read, read_files=frozenset(world.row_file[r] for r in read))
    users = [rid for rid, row in world.rows.items() if row["kind"] == "user"]
    picked = set()
    while len(picked) < 4:
        if hot_rho > 0 and rng.random() < hot_rho:
            picked.add(rng.choice(hot_rows))
        else:
            picked.add(rng.choice(users))
    updates = {rid: {"value": rng.randrange(100)} for rid in picked if rid in world.rows}
    return Task("update", updates=updates, rows=frozenset(updates),
                files=frozenset(world.row_file[r] for r in updates))


def is_hazard(task: Task, intervening: list[Commit]) -> bool:
    """A report and a delete that ran concurrently (neither saw the other) over shared rows.

    Committing them together leaves an aggregate in main computed over rows that main no longer
    has, whichever of the two commits first. This is the W3 invariant. SSI would accept it,
    because the pair has a read-write dependency in one direction only.
    """
    if task.kind == "report":
        return any(c.task.kind == "delete" and (task.read_rows & set(c.task.deletes)) for c in intervening)
    if task.kind == "delete":
        gone = set(task.deletes)
        return any(c.task.kind == "report" and (c.task.read_rows & gone) for c in intervening)
    return False


def write_cost(stats: Stats, system: str, task: Task) -> float:
    """Local commit: SemLake-style MOR writes a delta/DV and two manifests; CoW rewrites files."""
    if system == "b2" and task.files:
        return stats.charge(put=len(task.files) + 2, put_bytes=len(task.files) * FILE_BYTES)
    return stats.charge(put=3)


def run(system: str, agents: int, horizon: float, mix: dict, hot_rho: float, work_mean: float,
        n_files: int, rows_per_file: int, seed: int) -> dict:
    world = World(system, n_files, rows_per_file, seed)
    rng = random.Random(seed * 7 + agents)
    stats = Stats()
    users = [rid for rid, row in world.rows.items() if row["kind"] == "user"]
    hot_rows = users[:16]
    heap: list[tuple[float, int, str]] = []
    pool = [Agent(i) for i in range(agents)]
    for agent in pool:
        heapq.heappush(heap, (rng.random() * work_mean, agent.aid, "start"))

    def begin(agent: Agent, now: float, redo: bool) -> None:
        if not redo:
            agent.started = now
            agent.attempts = 0
        now += stats.charge(get=1)
        agent.base_version = world.version
        agent.base_snap = world.head_snap
        agent.task = make_task(world, rng, mix, hot_rho, hot_rows)
        work = rng.expovariate(1 / work_mean)
        if redo:
            stats.wasted_work_s += work
        now += work + write_cost(stats, system, agent.task)
        if system.startswith("semlake"):
            snap = world.repo.snapshots[agent.base_snap]
            task = agent.task
            preds = ()
            if task.kind == "delete":
                preds = (Predicate("purge", "deletes_where", where=task.where),)
            elif task.kind == "report":
                preds = (Predicate("report", "reads_where", where=task.where),)
            local = world.repo.apply(
                snap, f"agent{agent.aid}",
                updates=task.updates or None,
                deletes=[r for r in task.deletes if r in snap.rows] or None,
                inserts={task.insert_id: task.insert_row} if task.insert_id >= 0 else None,
                predicates=preds,
            )
            agent.candidate = local.snapshot_id
        agent.expected_version = agent.base_version
        agent.expected_snap = agent.base_snap
        heapq.heappush(heap, (now, agent.aid, "cas"))

    queue: list[Agent] = []
    service_busy = False

    def run_batch(now: float) -> bool:
        """Merge up to GROUP_SIZE queued commits onto the head and publish them with one CAS."""
        batch = queue[:GROUP_SIZE]
        del queue[:GROUP_SIZE]
        store = world.repo.store
        old_head = world.head_snap
        head = old_head
        done = []
        for member in batch:
            task = member.task
            if member.base_snap == head:
                merged_id = member.candidate
            else:
                before = (store.counters.put, store.counters.get, store.counters.list, store.counters.put_bytes)
                t0 = time.perf_counter()
                result = three_way_merge(world.repo, member.candidate, head, store)
                cpu = time.perf_counter() - t0
                c = store.counters
                now += cpu + stats.charge(put=int(c.put - before[0]), get=int(c.get - before[1]),
                                          lst=int(c.list - before[2]), put_bytes=int(c.put_bytes - before[3]))
                if result.status != "clean":
                    if result.status == "semantic_conflict":
                        if is_hazard(task, world.since(member.base_version)):
                            stats.hazards_caught += 1
                        else:
                            stats.false_blocks += 1
                        stats.redo_semantic += 1
                    else:
                        stats.vsa_parked += 1
                        stats.redo_true += 1
                    done.append((member, False))
                    continue
                merged_id = result.snapshot_id
                stats.rebases += 1
            if is_hazard(task, world.since(member.base_version)):
                stats.hazards_committed += 1
            head = merged_id
            world.head_snap = head
            world.apply(task)
            stats.commits[task.kind] += 1
            done.append((member, True))
        now += CATALOG_RTT_S  # one compare-and-swap publishes the whole batch
        if head != old_head:
            assert world.repo.cas_head("main", old_head, head)
        for member, ok in done:
            if ok:
                stats.latency.append(now - member.started)
                heapq.heappush(heap, (now, member.aid, "start"))
            else:
                begin(member, now, redo=True)
        if queue:
            heapq.heappush(heap, (now, -1, "service"))
            return True
        return False

    while heap:
        now, aid, phase = heapq.heappop(heap)
        if now > horizon:
            break
        if phase == "service":
            service_busy = run_batch(now)
            continue
        agent = pool[aid]
        if phase == "start":
            begin(agent, now, redo=False)
            continue
        if system == "semlake_group":
            # Send the local commit to the merge service.
            queue.append(agent)
            if not service_busy:
                service_busy = True
                heapq.heappush(heap, (now + CATALOG_RTT_S, -1, "service"))
            continue
        agent.attempts += 1
        now += CATALOG_RTT_S
        if world.version == agent.expected_version:
            intervening = world.since(agent.base_version)
            if is_hazard(agent.task, intervening):
                stats.hazards_committed += 1
            if system.startswith("semlake"):
                assert world.repo.cas_head("main", world.head_snap, agent.candidate)
                world.head_snap = agent.candidate
            world.apply(agent.task)
            stats.commits[agent.task.kind] += 1
            stats.latency.append(now - agent.started)
            heapq.heappush(heap, (now, aid, "start"))
            continue

        new = world.since(agent.expected_version)
        task = agent.task
        if system.startswith("semlake"):
            store = world.repo.store
            before = (store.counters.put, store.counters.get, store.counters.list, store.counters.put_bytes)
            t0 = time.perf_counter()
            result = three_way_merge(world.repo, agent.candidate, world.head_snap, store)
            cpu = time.perf_counter() - t0
            c = store.counters
            if system == "semlake":
                # As built: every request the engine issued during the merge.
                now += cpu + stats.charge(put=int(c.put - before[0]), get=int(c.get - before[1]),
                                          lst=int(c.list - before[2]), put_bytes=int(c.put_bytes - before[3]))
            else:
                # Scion-style commit path (modelled): immutable commit summaries come from a cache
                # after their first read, and a retry publishes one manifest list instead of a new
                # delta file plus three manifests. Same charge as a baseline rebase.
                now += cpu + stats.charge(get=len(new), lst=1, put=1)
            if result.status == "clean":
                stats.rebases += 1
                agent.candidate = result.snapshot_id
                agent.expected_version = world.version
                agent.expected_snap = world.head_snap
                heapq.heappush(heap, (now, aid, "cas"))
                continue
            if result.status == "semantic_conflict":
                if is_hazard(task, world.since(agent.base_version)):
                    stats.hazards_caught += 1
                else:
                    stats.false_blocks += 1
                stats.redo_semantic += 1
            else:  # vsa: a real same-row conflict; the agent redoes its write on the new head
                stats.vsa_parked += 1
                stats.redo_true += 1
            begin(agent, now, redo=True)
            continue

        touched_rows = set().union(*(c.task.rows for c in new)) if new else set()
        touched_files = set().union(*(c.task.files for c in new)) if new else set()
        deleted_files = set().union(*(c.task.files for c in new if c.task.kind == "delete")) if new else set()
        row_clash = bool(task.rows & touched_rows) and task.kind != "delete"
        if task.kind == "delete":
            # Deleting a row another commit just updated is a real conflict; two deletes are not.
            row_clash = bool(task.rows & set().union(*(c.task.rows for c in new if c.task.kind == "update")))
        abort = False
        if row_clash:
            stats.redo_true += 1
            abort = True
        elif system == "b1" and task.files & touched_files:
            stats.redo_false += 1
            abort = True
        elif system == "b1r" and task.kind == "report" and task.read_files & deleted_files:
            if is_hazard(task, world.since(agent.base_version)):
                stats.hazards_caught += 1
            else:
                stats.false_blocks += 1
            stats.redo_semantic += 1
            abort = True
        if abort:
            begin(agent, now, redo=True)
            continue
        # Rebase: re-read the new manifests and rewrite the commit's manifest on the new head.
        stats.rebases += 1
        now += stats.charge(get=len(new), lst=1, put=1)
        if system == "b2" and task.files & touched_files:
            # Copy-on-write must rewrite a file again when another commit replaced it meanwhile.
            shared = task.files & touched_files
            now += stats.charge(put=len(shared), put_bytes=len(shared) * FILE_BYTES)
        agent.expected_version = world.version
        heapq.heappush(heap, (now, aid, "cas"))

    total = sum(stats.commits.values())
    lat = sorted(stats.latency) or [0.0]
    cost = stats.put * PRICE_PUT_USD + stats.get * PRICE_GET_USD + stats.list * PRICE_LIST_USD
    return {
        "system": system,
        "agents": agents,
        "hot_rho": hot_rho,
        "commits": total,
        "commits_per_s": total / horizon,
        "updates": stats.commits["update"],
        "deletes": stats.commits["delete"],
        "reports": stats.commits["report"],
        "p50_s": statistics.median(lat),
        "p99_s": lat[min(len(lat) - 1, int(0.99 * (len(lat) - 1)))],
        "redo_true": stats.redo_true,
        "redo_false": stats.redo_false,
        "redo_semantic": stats.redo_semantic,
        "rebases": stats.rebases,
        "wasted_work_s_per_commit": stats.wasted_work_s / total if total else 0.0,
        "requests_per_commit": (stats.put + stats.get + stats.list) / total if total else 0.0,
        "gb_written_per_commit": stats.put_bytes / total / 1e9 if total else 0.0,
        "usd_per_1k_commits": 1000 * cost / total if total else 0.0,
        "hazards_committed": stats.hazards_committed,
        "hazards_caught": stats.hazards_caught,
        "false_blocks": stats.false_blocks,
        "redo_false_per_commit": stats.redo_false / total if total else 0.0,
        "false_blocks_per_commit": stats.false_blocks / total if total else 0.0,
        "seed": seed,
    }


MIXES = {"agentic": {"delete": 0.05, "report": 0.15}, "purge-heavy": {"delete": 0.15, "report": 0.30}}


def _one(job: tuple) -> dict:
    mix_name, rho, agents, system, seed, horizon = job
    started = time.perf_counter()
    row = run(system, agents, horizon, MIXES[mix_name], rho, work_mean=WORK_MEAN_S,
              n_files=N_FILES, rows_per_file=ROWS_PER_FILE, seed=seed)
    row = {"mix": mix_name, **row, "wall_s": time.perf_counter() - started}
    print(f"{mix_name:<11} rho={rho:<4} agents={agents:<3} seed={seed:<3} {system:<14} "
          f"commits/s={row['commits_per_s']:6.2f} false_redo={row['redo_false']:5d} "
          f"hazards={row['hazards_committed']:3d} caught={row['hazards_caught']:3d} "
          f"false_blocks={row['false_blocks']:3d} GB/commit={row['gb_written_per_commit']:.3f} "
          f"({row['wall_s']:.0f}s wall)", flush=True)
    return row


# Two-sided 95% Student t quantiles by degrees of freedom.
_T95 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262}


def summarize(rows: list[dict]) -> list[dict]:
    """Mean and 95% confidence half-width over seeds, per mix, contention, agents and system."""
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        groups.setdefault((row["mix"], row["hot_rho"], row["agents"], row["system"]), []).append(row)
    out = []
    metrics = ("commits_per_s", "hazards_committed", "hazards_caught", "false_blocks", "redo_false",
               "redo_false_per_commit", "false_blocks_per_commit", "gb_written_per_commit",
               "wasted_work_s_per_commit", "requests_per_commit", "p99_s")
    for (mix, rho, agents, system), items in sorted(groups.items()):
        entry = {"mix": mix, "hot_rho": rho, "agents": agents, "system": system, "seeds": len(items)}
        for key in metrics:
            values = [float(item[key]) for item in items]
            mean = statistics.fmean(values)
            half = 0.0
            if len(values) > 1:
                half = _T95.get(len(values) - 1, 1.96) * statistics.stdev(values) / math.sqrt(len(values))
            entry[key] = mean
            entry[f"{key}_ci"] = half
        entry["hazards_committed_total"] = sum(int(item["hazards_committed"]) for item in items)
        entry["hazards_caught_total"] = sum(int(item["hazards_caught"]) for item in items)
        entry["false_blocks_total"] = sum(int(item["false_blocks"]) for item in items)
        out.append(entry)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--seeds", type=int, default=5)
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    parser.add_argument("--out", default=str(ROOT / "benchmarks" / "results" / "scion_sim.csv"))
    args = parser.parse_args()
    if args.quick:
        plan = [("agentic", 0.0, a) for a in (8, 16)]
        horizon, seeds = 20.0, [11]
    else:
        plan = ([("agentic", 0.0, a) for a in (8, 16, 32, 64)]
                + [("purge-heavy", 0.0, a) for a in (8, 32)])
        horizon, seeds = 60.0, [11 + 2 * i for i in range(args.seeds)]
    jobs = [(mix, rho, agents, system, seed, horizon)
            for mix, rho, agents in plan for system in SYSTEMS for seed in seeds]
    # Longest jobs first: SemLake at many agents dominates the wall time.
    jobs.sort(key=lambda job: (not job[3].startswith("semlake"), -job[2]))
    with multiprocessing.get_context("spawn").Pool(args.workers) as pool:
        rows = pool.map(_one, jobs, chunksize=1)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = summarize(rows)
    out.with_name(out.stem + "_summary.json").write_text(
        json.dumps({"horizon_s": horizon, "seeds": seeds, "group_size": GROUP_SIZE,
                    "catalog_rtt_s": CATALOG_RTT_S, "rows": summary}, indent=2), encoding="utf-8")
    print(f"wrote {out} and its summary", flush=True)
    return 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    os._exit(code)  # skip interpreter teardown, which can hang on library threads
