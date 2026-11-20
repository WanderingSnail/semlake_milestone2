"""Experiments added for the revision (the reviewers' MC2, MC3, MC4 and the tail-latency rubric).

    python benchmarks/revision_experiments.py ledger    # per-object ledger, every system (Table: ledger)
    python benchmarks/revision_experiments.py tails     # P50/P95/P99 merge latency, W2/W4/history sweeps
    python benchmarks/revision_experiments.py sweep     # delta-size and predicate-count sweep
    python benchmarks/revision_experiments.py threads   # multi-threaded commit service, 1 to 64 writers
    python benchmarks/revision_experiments.py recovery  # fault injection at every PUT, invariants I1 to I5
    python benchmarks/revision_experiments.py prune     # pre-image pruning: bytes saved and merges blocked
    python benchmarks/revision_experiments.py all [--quick]

Each subcommand writes one JSON file under ``benchmarks/results/``. ``--quick``
shrinks every sweep so the whole file runs in about a minute (used by the tests
and by the artifact's smoke run); the paper uses the full runs.
"""

from __future__ import annotations

import argparse
import json
import platform
import random
import resource
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from benchmarks.rule_baselines import b1r_storage_merge  # noqa: E402
from semlake.sql_predicates import predicates_from_sql  # noqa: E402
from benchmarks.workloads import workload_w1s, workload_w2, workload_w4  # noqa: E402
from semlake.baselines import (  # noqa: E402
    WriteConflictException,
    copy_on_write,
    crdt_merge,
    file_level_occ,
    tuple_version_merge,
)
from semlake.merge import three_way_merge  # noqa: E402
from semlake.metadata import Repository, Schema, SchemaField  # noqa: E402
from semlake.storage import MemoryObjectStore  # noqa: E402
from semlake.verification import Predicate  # noqa: E402

RESULTS = ROOT / "benchmarks" / "results"

SYSTEMS = {
    "semlake": three_way_merge,
    "b1r": b1r_storage_merge,
    "b1": file_level_occ,
    "b2": copy_on_write,
    "b3": tuple_version_merge,
    "b4": crdt_merge,
}

# Median request times measured against the S3 bucket (results/s3_run_v2.json, eu-north-1).
S3_PUT_S, S3_GET_S, S3_LIST_S = 0.2973, 0.1541, 0.1473
# Requests a copy-on-write engine issues at once (the same setting as the live S3 run).
PARALLEL = 32
# AWS S3 Standard, USD per request.
PRICE_PUT, PRICE_GET, PRICE_LIST = 0.005 / 1000, 0.0004 / 1000, 0.005 / 1000


def environment() -> dict:
    """Hardware and software of this run, stored with every result file."""
    import duckdb
    import pyarrow

    cpu = platform.processor() or ""
    mem_kb = 0
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal"):
                mem_kb = int(line.split()[1])
                break
    except OSError:
        pass
    import os

    return {
        "cpu": cpu,
        "cores": os.cpu_count(),
        "memory_gb": round(mem_kb / 1024 / 1024, 1),
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "pyarrow": pyarrow.__version__,
    }


def percentiles(samples: list[float]) -> dict:
    """P50, P95, P99, mean and extremes of ``samples`` (nearest-rank percentiles)."""
    ordered = sorted(samples)
    n = len(ordered)

    def rank(q: float) -> float:
        return ordered[min(n - 1, max(0, int(round(q * n + 0.5)) - 1))]

    return {
        "n": n,
        "p50": rank(0.50),
        "p95": rank(0.95),
        "p99": rank(0.99),
        "mean": statistics.fmean(ordered),
        "min": ordered[0],
        "max": ordered[-1],
    }


def _write(name: str, doc: dict) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    doc = {"environment": environment(), **doc}
    (RESULTS / name).write_text(json.dumps(doc, indent=1))
    print(f"wrote {RESULTS / name}")


# ---------------------------------------------------------------------------
# Per-object ledger (MC3) with B1R in every storage number (MC2)


def object_class(key: str) -> str:
    if key.startswith("data/"):
        return "data"
    if key.endswith(".summary.json"):
        return "summary"
    if key.startswith("manifests/"):
        return "manifest"
    return "other"


class LedgerStore(MemoryObjectStore):
    """An object store that also counts every request by object class."""

    def __init__(self) -> None:
        super().__init__()
        self.ledger: dict[str, dict[str, float]] = {}

    def _note(self, op: str, key: str, size: int) -> None:
        cell = self.ledger.setdefault(object_class(key), {"put": 0, "put_bytes": 0, "get": 0, "get_bytes": 0})
        cell[op] += 1
        cell[f"{op}_bytes"] += size

    def put(self, key: str, data: bytes) -> None:
        super().put(key, data)
        self._note("put", key, len(data))

    def get(self, key: str) -> bytes:
        data = super().get(key)
        self._note("get", key, len(data))
        return data

    def clone(self) -> LedgerStore:
        copied = LedgerStore()
        copied.objects.update(self.objects)
        return copied


def _counters_doc(store: MemoryObjectStore) -> dict:
    c = store.counters
    return {"put": c.put, "get": c.get, "list": c.list, "requests": c.requests,
            "put_bytes": c.put_bytes, "get_bytes": c.get_bytes}


def end_to_end_s(cpu_s: float, counters: dict, parallel: int = 1) -> float:
    """Merge latency over S3: CPU plus requests at the measured medians.

    With ``parallel`` > 1 the data-file requests go out that many at a time, as
    in the live run; the handful of metadata requests stay sequential.
    """
    puts, gets = counters["put"], counters["get"]
    if parallel > 1 and puts + gets > 16:
        return cpu_s + (puts * S3_PUT_S + gets * S3_GET_S) / parallel + counters["list"] * S3_LIST_S
    return cpu_s + puts * S3_PUT_S + gets * S3_GET_S + counters["list"] * S3_LIST_S


def request_usd(counters: dict) -> float:
    return counters["put"] * PRICE_PUT + counters["get"] * PRICE_GET + counters["list"] * PRICE_LIST


def _merge_once(repo, fn, left_id: str, right_id: str) -> dict:
    scratch = repo.store.clone()
    started = time.perf_counter()
    try:
        result = fn(repo, left_id, right_id, scratch)
        status = result.status
        choices = len(result.vsa)
    except WriteConflictException:
        status, choices = "aborted", 0
    cpu = time.perf_counter() - started
    return {"status": status, "cpu_s": cpu, "choices": choices, "counters": _counters_doc(scratch),
            "ledger": getattr(scratch, "ledger", {})}


def _ledger_scenario(workload: str, n_files: int, repeats: int) -> dict:
    """Build one scenario on a ledger store and merge it with every system."""
    import benchmarks.workloads as workloads

    original = workloads.MemoryObjectStore
    workloads.MemoryObjectStore = LedgerStore  # the scenario's own store counts by class
    try:
        build = workload_w1s if workload == "W1S" else workload_w4
        # Branch commits are measured by diffing the ledger around the builder.
        scenario = build(n_files, 4096)
    finally:
        workloads.MemoryObjectStore = original
    repo = scenario.repo
    commit_ledger = _commit_phase(repo, scenario)
    out = {"workload": workload, "files": n_files, "commit": commit_ledger, "systems": {}}
    for name, fn in SYSTEMS.items():
        _merge_once(repo, fn, scenario.left_id, scenario.right_id)  # warm-up
        runs = [_merge_once(repo, fn, scenario.left_id, scenario.right_id) for _ in range(repeats)]
        first = runs[0]
        cpu = percentiles([run["cpu_s"] for run in runs])
        parallel = PARALLEL if name in ("b2", "b3") else 1
        out["systems"][name] = {
            "status": first["status"],
            "merge": first["ledger"],
            "counters": first["counters"],
            "cpu_s": cpu,
            "end_to_end_s": {q: end_to_end_s(cpu[q], first["counters"], parallel) for q in ("p50", "p95", "p99")},
            "sequential_s": end_to_end_s(cpu["p50"], first["counters"], 1),
            "request_usd": request_usd(first["counters"]),
        }
        c = first["counters"]
        print(f"  {workload} {n_files:>6} {name:8} {first['status']:9} req={c['requests']:>7.0f} "
              f"put_bytes={c['put_bytes']:>12,.0f} cpu_p50={cpu['p50'] * 1000:8.2f}ms p99={cpu['p99'] * 1000:8.2f}ms")
    return out


def _commit_phase(repo, scenario) -> dict:
    """Objects the two branch commits wrote, read back from the stored objects by key."""
    out: dict[str, dict[str, float]] = {}
    for snap_id in (scenario.left_id, scenario.right_id):
        for key, blob in repo.store.objects.items():
            if f"/{snap_id}." in key or f"/{snap_id}/" in key:
                cell = out.setdefault(object_class(key), {"put": 0, "put_bytes": 0})
                cell["put"] += 1
                cell["put_bytes"] += len(blob)
    return out


def run_ledger(quick: bool) -> None:
    depths = [200] if quick else [1000, 5000, 10000]
    repeats = 3 if quick else 30
    rows = []
    for workload in ("W1S", "W4"):
        for depth in depths:
            print(f"ledger {workload} at {depth} files")
            rows.append(_ledger_scenario(workload, depth, repeats))
    _write("ledger.json", {
        "note": ("per-object counts and bytes, split into the two branch commits and the merge; "
                 "end_to_end_s adds requests at the S3 medians of s3_run_v2.json"),
        "s3_medians_s": {"put": S3_PUT_S, "get": S3_GET_S, "list": S3_LIST_S},
        "parallel_requests": PARALLEL,
        "rows": rows,
    })


# ---------------------------------------------------------------------------
# Tail latency (rubric) and open choices under contention (MC5)


def run_tails(quick: bool) -> None:
    n_files = 256 if quick else 10_000
    trials = 6 if quick else 60
    rhos = [0.0, 0.2] if quick else [0.0, 0.01, 0.05, 0.2, 0.5]
    print(f"W2 contention sweep at {n_files} files, {trials} fork pairs per point")
    repo, _base_id, pairs = workload_w2(n_files, 1, rhos, trials)
    contention = []
    for rho, fork_pairs in pairs.items():
        for name, fn in SYSTEMS.items():
            for left_id, right_id in fork_pairs[:2]:
                _merge_once(repo, fn, left_id, right_id)
            # Tuple versioning rebuilds a dictionary per file (about 10 s a merge at 10K files),
            # so it gets 12 fork pairs per point; every other system gets all of them.
            chosen = fork_pairs[:12] if name == "b3" and not quick else fork_pairs
            runs = [_merge_once(repo, fn, left_id, right_id) for left_id, right_id in chosen]
            cpu = percentiles([run["cpu_s"] for run in runs])
            statuses: dict[str, int] = {}
            for run in runs:
                statuses[run["status"]] = statuses.get(run["status"], 0) + 1
            row = {"rho": rho, "system": name, "cpu_s": cpu, "statuses": statuses,
                   "choices_opened": sum(run["choices"] for run in runs), "pairs": len(runs)}
            contention.append(row)
            print(f"  rho={rho:<5} {name:8} p50={cpu['p50'] * 1000:8.2f}ms p99={cpu['p99'] * 1000:8.2f}ms {statuses}")

    print("choice resolution after the W2 sweep")
    resolution = _choice_lifecycle(repo, pairs)

    depths = [128, 512] if quick else [1000, 2000, 5000, 10000]
    repeats = 5 if quick else 40
    manifest = []
    for depth in depths:
        scenario = workload_w4(depth, 4096)
        for name, fn in SYSTEMS.items():
            _merge_once(scenario.repo, fn, scenario.left_id, scenario.right_id)
            runs = [_merge_once(scenario.repo, fn, scenario.left_id, scenario.right_id) for _ in range(repeats)]
            cpu = percentiles([run["cpu_s"] for run in runs])
            manifest.append({"files": depth, "system": name, "cpu_s": cpu, "status": runs[0]["status"],
                             "requests": runs[0]["counters"]["requests"]})
            print(f"  W4 files={depth:<6} {name:8} p50={cpu['p50'] * 1000:8.3f}ms p99={cpu['p99'] * 1000:8.3f}ms")

    history = _history([40, 80] if quick else [40, 120, 240, 360], 10 if quick else 40)
    _write("tails.json", {"contention_files": n_files, "contention": contention,
                          "choice_lifecycle": resolution, "manifest": manifest, "history": history})


def _choice_lifecycle(repo, pairs) -> list[dict]:
    """Per contention level: choices opened, resolved by one commit, and left open."""
    from semlake.maintenance import resolve_choices, unresolved

    out = []
    for rho, fork_pairs in pairs.items():
        opened = resolved = still_open = 0
        resolve_cpu = []
        for left_id, right_id in fork_pairs:
            result = three_way_merge(repo, left_id, right_id, repo.store.clone())
            if result.status != "vsa":
                continue
            merged = repo.snapshots[result.snapshot_id]
            opened += len(merged.open_choices)
            picks = {(c["row_id"], c.get("column", "*")): "A" for c in merged.open_choices}
            started = time.perf_counter()
            fixed = resolve_choices(repo, merged, picks)
            resolve_cpu.append(time.perf_counter() - started)
            resolved += len(picks)
            still_open += len(unresolved(fixed))
        out.append({"rho": rho, "pairs": len(fork_pairs), "opened": opened, "resolved": resolved,
                    "still_open": still_open,
                    "resolve_cpu_s": percentiles(resolve_cpu) if resolve_cpu else None})
        print(f"  rho={rho:<5} opened={opened} resolved={resolved} still_open={still_open}")
    return out


def _history(marks: list[int], repeats: int) -> list[dict]:
    """LCA and merge time as the commit chain grows, with percentiles."""
    schema = Schema((SchemaField("id", "int64"), SchemaField("kind", "string"), SchemaField("value", "int64")))
    repo = Repository(MemoryObjectStore())
    rows = {i: {"id": i, "kind": "user", "value": i} for i in range(4)}
    head = repo.create_table("main", schema, rows=rows, rows_per_file=4)
    pending = sorted(set(marks))
    found = []
    while pending:
        left = repo.apply(head, "A", updates={0: {"value": head.rows[0]["value"] + 1}})
        right = repo.apply(head, "B", updates={1: {"value": head.rows[1]["value"] + 1}})
        before = len(repo.snapshots)
        if before < pending[0]:
            merged = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store)
            head = repo.snapshots[merged.snapshot_id]
            continue
        lca = []
        for _ in range(repeats):
            started = time.perf_counter()
            repo.graph.lca(left.snapshot_id, right.snapshot_id)
            lca.append(time.perf_counter() - started)
        times = []
        for _ in range(repeats):
            scratch = repo.store.clone()
            started = time.perf_counter()
            three_way_merge(repo, left.snapshot_id, right.snapshot_id, scratch)
            times.append(time.perf_counter() - started)
        merged = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store)
        head = repo.snapshots[merged.snapshot_id]
        found.append({"snapshots": before, "lca_s": percentiles(lca), "merge_s": percentiles(times)})
        print(f"  history={before:<4} lca_p50={found[-1]['lca_s']['p50'] * 1e6:6.1f}us "
              f"merge_p50={found[-1]['merge_s']['p50'] * 1000:6.3f}ms p99={found[-1]['merge_s']['p99'] * 1000:6.3f}ms")
        pending.pop(0)
    return found


# ---------------------------------------------------------------------------
# Delta-size and predicate-count sweep (MC4, R1.1)


def sweep_point(kind: str, n: int, preds: int, vectorized: bool, repeats: int) -> dict:
    """One point of the sweep, run in its own process so peak memory is its own.

    Branch A deletes (or updates) ``n`` rows under ``preds`` clauses. Branch B is
    a report with ``preds`` read clauses that overlap A's clauses statically but
    match none of the changed rows, so the row-value check has to look at every
    changed row for every read clause and the merge still commits: the worst
    case of a merge that goes through.
    """
    table_rows = max(2 * n, 20_000)
    schema = Schema((SchemaField("id", "int64"), SchemaField("region", "string"),
                     SchemaField("value", "int64"), SchemaField("score", "double")))
    rows = {i: {"id": i, "region": f"r{i % 7}", "value": i % 1000, "score": float(i % 13)}
            for i in range(table_rows)}
    repo = Repository(MemoryObjectStore())
    base = repo.create_table("main", schema, rows=rows, rows_per_file=100_000)
    del rows
    step = max(1, n // preds)
    changed = list(range(n))
    if kind == "delete":
        clauses = [Predicate(f"purge{i}", "deletes_where", where=f"id >= {i * step} AND id < {(i + 1) * step}")
                   for i in range(preds)]
        branch_a = repo.apply(base, "A", deletes=changed, predicates=clauses, purge=True)
    else:
        branch_a = repo.apply(base, "A", updates={rid: {"score": -1.0} for rid in changed})
    reads = [Predicate(f"report{i}", "reads_where", where=f"region = 'none{i}' AND value >= {i}",
                       columns=frozenset({"score"})) for i in range(preds)]
    branch_b = repo.apply(base, "B", inserts={table_rows + 1: {"id": table_rows + 1, "region": "out",
                                                              "value": 0, "score": 0.0}}, predicates=reads)
    summary_bytes = len(repo.store.objects[branch_a.summary_key])
    times = []
    status = ""
    for _ in range(repeats):
        scratch = repo.store.clone()
        started = time.perf_counter()
        result = three_way_merge(repo, branch_a.snapshot_id, branch_b.snapshot_id, scratch, vectorized=vectorized)
        times.append(time.perf_counter() - started)
        status = result.status
        put_bytes = scratch.counters.put_bytes
    return {
        "kind": kind, "rows_changed": n, "predicates": preds, "vectorized": vectorized, "status": status,
        "table_rows": table_rows, "summary_bytes": summary_bytes, "merge_put_bytes": put_bytes,
        "merge_s": percentiles(times),
        "peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
    }


def _spawn_point(kind: str, n: int, preds: int, vectorized: bool, repeats: int) -> dict:
    out = subprocess.run(
        [sys.executable, __file__, "sweep-point", "--kind", kind, "--n", str(n), "--preds", str(preds),
         "--vectorized", "1" if vectorized else "0", "--repeats", str(repeats)],
        capture_output=True, text=True, check=True,
    )
    row = json.loads(out.stdout.strip().splitlines()[-1])
    print(f"  {kind:6} rows={n:>9,} preds={preds:<3} vec={int(vectorized)} "
          f"merge_p50={row['merge_s']['p50']:8.3f}s summary={row['summary_bytes'] / 1e6:8.2f}MB "
          f"rss={row['peak_rss_mb']:8.0f}MB {row['status']}", flush=True)
    return row


def run_sweep(quick: bool) -> None:
    sizes = [1_000, 10_000] if quick else [1_000, 10_000, 100_000, 1_000_000]
    counts = [1, 4] if quick else [1, 4, 16, 64]
    rows = []
    print("rows changed")
    for kind in ("delete", "update"):
        for n in sizes:
            repeats = 3 if n >= 1_000_000 else (5 if n >= 100_000 else 15)
            rows.append(_spawn_point(kind, n, 1, True, repeats))
            if n <= 100_000:
                rows.append(_spawn_point(kind, n, 1, False, repeats))
    print("predicates per branch")
    fixed = 10_000 if quick else 100_000
    for preds in counts:
        if preds == 1:
            continue
        rows.append(_spawn_point("delete", fixed, preds, True, 5))
        if preds <= 16:
            rows.append(_spawn_point("delete", fixed, preds, False, 3))
    _write("delta_sweep.json", {"rows": rows})


# ---------------------------------------------------------------------------
# Multi-threaded commit service (rubric: scale-out and contention with real threads)

THREAD_SCALE = 0.1      # request and think times are the S3 medians times this
THREAD_WORK_S = 2.0     # mean think time of a writer per task, as in the swarm simulator
THREAD_FILES = 64
THREAD_ROWS = 4096
CATALOG_S = 0.010       # one conditional update in the catalog


class _LatencyStore(MemoryObjectStore):
    """A store whose requests take wall-clock time, so threads really overlap."""

    def __init__(self, scale: float) -> None:
        super().__init__()
        self.scale = scale
        self.requests = {"put": 0, "get": 0, "list": 0}
        self._count = threading.Lock()

    def _tick(self, kind: str) -> None:
        with self._count:
            self.requests[kind] += 1

    def put(self, key: str, data: bytes) -> None:
        self._tick("put")
        time.sleep(S3_PUT_S * self.scale)
        self.objects[key] = data

    def get(self, key: str) -> bytes:
        self._tick("get")
        time.sleep(S3_GET_S * self.scale)
        return self.objects[key]

    def list(self, prefix: str = "") -> list[str]:
        self._tick("list")
        time.sleep(S3_LIST_S * self.scale)
        return []


class _Buffer:
    """What one merge reads and writes, recorded under the catalog lock and paid for outside it.

    The merge runs against the in-memory objects; its PUTs are held back and its
    GETs and LISTs are only noted. ``settle`` then issues the same requests
    against the latency store from a thread pool, the way a client would issue
    independent requests at once. ``cache`` holds immutable objects this client
    has already read or written, which need no second GET.
    """

    def __init__(self, shared: dict, cache: set[str]) -> None:
        self.shared = shared
        self.cache = cache
        self.pending: dict[str, bytes] = {}
        self.read: list[str] = []
        self.lists = 0
        from semlake.storage import Counters

        self.counters = Counters()
        self.objects = self  # three_way_merge only calls put/get/list

    def put(self, key: str, data: bytes) -> None:
        self.pending[key] = data
        self.shared[key] = data  # visible to this process; unreachable until the compare-and-swap

    def get(self, key: str) -> bytes:
        if key not in self.cache:
            self.read.append(key)
            self.cache.add(key)
        return self.shared[key]

    def list(self, prefix: str = "") -> list[str]:
        self.lists += 1
        return []

    def settle(self, store: _LatencyStore, pool) -> None:
        jobs = [pool.submit(store.get, key) for key in self.read]
        jobs += [pool.submit(store.list, "manifests/") for _ in range(self.lists)]
        for job in jobs:
            job.result()
        jobs = [pool.submit(store.put, key, data) for key, data in self.pending.items()]
        for job in jobs:
            job.result()
        self.cache.update(self.pending)
        self.pending, self.read, self.lists = {}, [], 0


def run_service(n_writers: int, group: int, duration_s: float, scale: float, hot: float, seed: int,
                system: str = "semlake") -> dict:
    """``n_writers`` threads commit row updates to one branch head.

    Every writer forks the head, thinks, writes its commit (a delta file, a
    summary and a manifest, issued at once) and then publishes it.

    ``group == 0``: optimistic clients. The writer tries the compare-and-swap
    itself; when it loses the race it merges its commit with the new head (or,
    for ``b1``, aborts if a commit it raced with touched one of its files) and
    tries again.

    ``group >= 1``: a commit service. Writers queue their commit; one committer
    thread takes up to ``group`` of them, merges them onto the head one after
    another in memory, writes the batch's objects at once and moves the head
    with one compare-and-swap, so the batch is visible atomically (I4).

    Each commit is an increment and declares the read of its row, so two
    increments of one row from the same base are a semantic conflict; the
    losing writer redoes its transaction on the new head (``redo_true``). Storage requests sleep for the
    measured S3 medians times ``scale``.
    """
    from concurrent.futures import ThreadPoolExecutor

    schema = Schema((SchemaField("id", "int64"), SchemaField("kind", "string"), SchemaField("value", "int64")))
    setup = MemoryObjectStore()
    repo = Repository(setup)
    per_file = THREAD_ROWS // THREAD_FILES
    repo.create_table("main", schema, rows={i: {"id": i, "kind": "user", "value": 0} for i in range(THREAD_ROWS)},
                      rows_per_file=per_file)
    store = _LatencyStore(scale)
    store.objects.update(setup.objects)
    shared = store.objects
    pool = ThreadPoolExecutor(max_workers=max(16, 4 * n_writers))

    lock = threading.Lock()          # guards the in-memory catalog and the engine
    log: list[tuple[str, int]] = []  # (snapshot the head moved to, file the commit touched)
    position: dict[str, int] = {repo.heads["main"]: 0}
    queue: list[tuple] = []
    queue_ready = threading.Condition()
    stop = threading.Event()
    latencies: list[float] = []
    count = {"applied": 0, "published": 0, "redo_true": 0, "redo_false": 0, "cas": 0, "cas_lost": 0, "merges": 0}

    def local_commit(index: int, head_id: str, rid: int):
        """Fork ``head_id`` and commit one row update; returns the snapshot id and its objects."""
        with lock:
            head = repo.snapshots[head_id]
            scratch = MemoryObjectStore()
            scratch.objects = dict(shared)
            plain = repo.store
            repo.store = scratch
            try:
                # An increment reads the row it rewrites. Declaring that read is what makes two
                # increments of one row from the same base a conflict instead of one lost update:
                # both write the same value, which a merge without the read takes as agreement.
                read = predicates_from_sql(f"SELECT value FROM t WHERE id = {rid}", f"w{index}-read")
                local = repo.apply(head, f"w{index}", updates={rid: {"value": head.rows[rid]["value"] + 1}},
                                   predicates=read)
            finally:
                repo.store = plain
            fresh = {k: v for k, v in scratch.objects.items() if k not in shared}
            shared.update(fresh)
        jobs = [pool.submit(store.put, key, data) for key, data in fresh.items()]
        for job in jobs:
            job.result()
        return local.snapshot_id, set(fresh)

    def rebase(local_id: str, base_id: str, head_id: str | None, file_id: int, cache: set[str],
               batch_files: tuple[int, ...] = ()):
        """Bring ``local_id`` (forked at ``base_id``) onto ``head_id`` (the branch head when None).

        Returns ``(verdict, merged snapshot, head it was merged onto, buffer)``.
        """
        buffer = _Buffer(shared, cache)
        with lock:
            if head_id is None:
                head_id = repo.heads["main"]
            if system == "b1":
                raced = {fid for _, fid in log[position[base_id]:]} | set(batch_files)
                if file_id in raced:
                    return "redo_false", None, head_id, buffer
            count["merges"] += 1
            result = three_way_merge(repo, head_id, local_id, buffer, semantic_check=system != "b1",
                                     on_cell_conflict="abort")
        if result.status != "clean":
            return "redo_true", None, head_id, buffer
        return "applied", result.snapshot_id, head_id, buffer

    def advance(expected: str, new: str, files: list[int]) -> bool:
        time.sleep(CATALOG_S * scale)
        with lock:
            count["cas"] += 1
            moved = repo.heads["main"] == expected and repo.cas_head("main", expected, new)
            if moved:
                count["published"] += len(files)
                for fid in files:
                    log.append((new, fid))
                position[new] = len(log)
            else:
                count["cas_lost"] += 1
        return bool(moved)

    def writer(index: int) -> None:
        rng = random.Random(seed * 1000 + index)
        cache: set[str] = set()
        while not stop.is_set():
            rid = 0 if rng.random() < hot else 1 + rng.randrange(THREAD_ROWS - 1)
            file_id = rid // per_file
            started = time.perf_counter()
            done = False
            while not done and not stop.is_set():
                with lock:
                    base_id = repo.heads["main"]
                time.sleep(rng.expovariate(1 / THREAD_WORK_S) * scale)
                local_id, written = local_commit(index, base_id, rid)
                cache.update(written)
                if group:
                    finished = threading.Event()
                    outcome: list[str] = []
                    with queue_ready:
                        queue.append((local_id, base_id, file_id, finished, outcome))
                        queue_ready.notify()
                    while not finished.wait(0.05):
                        if stop.is_set():
                            return
                    verdict = outcome[0]
                else:
                    candidate, expected = local_id, base_id
                    while True:
                        if advance(expected, candidate, [file_id]):
                            verdict = "applied"
                            break
                        if stop.is_set():
                            return
                        verdict, merged, head_id, buffer = rebase(local_id, base_id, None, file_id, cache)
                        buffer.settle(store, pool)
                        if verdict != "applied":
                            break
                        candidate, expected = merged, head_id
                if verdict == "applied":
                    with lock:
                        count["applied"] += 1
                        latencies.append(time.perf_counter() - started)
                    done = True
                else:
                    with lock:
                        count[verdict] += 1

    def committer() -> None:
        cache: set[str] = set()
        while not stop.is_set():
            with queue_ready:
                while not queue and not stop.is_set():
                    queue_ready.wait(0.05)
                batch = queue[:group]
                del queue[:group]
            if not batch:
                continue
            with lock:
                expected = repo.heads["main"]
            current = expected
            verdicts: list[str] = []
            buffers: list[_Buffer] = []
            files: list[int] = []
            for local_id, base_id, file_id, finished, outcome in batch:
                if base_id == current:
                    verdict, merged = "applied", local_id  # fast-forward, nothing to merge
                else:
                    verdict, merged, _, buffer = rebase(local_id, base_id, current, file_id, cache, tuple(files))
                    buffers.append(buffer)
                verdicts.append(verdict)
                if verdict == "applied":
                    current = merged
                    files.append(file_id)
            # One round for the batch: its reads at once, then its writes at once.
            jobs = [pool.submit(store.get, key) for buffer in buffers for key in buffer.read]
            if any(buffer.lists for buffer in buffers):
                jobs.append(pool.submit(store.list, "manifests/"))
            for job in jobs:
                job.result()
            jobs = [pool.submit(store.put, key, data) for buffer in buffers for key, data in buffer.pending.items()]
            for job in jobs:
                job.result()
            moved = current == expected or advance(expected, current, files)
            for (local_id, base_id, file_id, finished, outcome), verdict in zip(batch, verdicts):
                outcome.append(verdict if moved else "redo_true")
                finished.set()

    threads = [threading.Thread(target=writer, args=(i,), daemon=True) for i in range(n_writers)]
    if group:
        threads.append(threading.Thread(target=committer, daemon=True))
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    time.sleep(duration_s)
    stop.set()
    for thread in threads:
        thread.join(timeout=5)
    elapsed = time.perf_counter() - started
    pool.shutdown(wait=False, cancel_futures=True)
    with lock:
        final = repo.snapshots[repo.heads["main"]]
        total = sum(row["value"] for row in final.rows.values())
        applied = count["applied"]
        lat = percentiles([value / scale for value in latencies]) if latencies else None
    return {
        "system": system, "writers": n_writers, "group": group, "hot": hot, "seed": seed, "scale": scale,
        **count, "requests": dict(store.requests),
        # Every applied commit adds one to one row, so a lost or doubled update shows up here.
        "increments_on_head": total, "lost_updates": count["published"] - total,
        # Sleeps are the S3 medians times ``scale``; these are scaled back to S3 time.
        "commits_per_s": applied / elapsed * scale,
        "latency_s": lat,
        "requests_per_commit": sum(store.requests.values()) / applied if applied else None,
    }


def run_threads(quick: bool) -> None:
    writers = [1, 4] if quick else [1, 2, 4, 8, 16, 32, 64]
    duration = 2.0 if quick else 10.0
    seeds = [1] if quick else [1, 2, 3]
    rows = []
    for system in ("semlake", "b1"):
        for hot in ((0.0,) if quick else (0.0, 0.1)):
            for group in (0, 8):
                for n in writers:
                    runs = [run_service(n, group, duration, THREAD_SCALE, hot, seed, system) for seed in seeds]
                    tps = [run["commits_per_s"] for run in runs]
                    p99 = [run["latency_s"]["p99"] for run in runs if run["latency_s"]]
                    p50 = [run["latency_s"]["p50"] for run in runs if run["latency_s"]]
                    applied = sum(run["applied"] for run in runs)
                    row = {"system": system, "writers": n, "group": group, "hot": hot, "runs": runs,
                           "commits_per_s_mean": statistics.fmean(tps),
                           "commits_per_s_min": min(tps), "commits_per_s_max": max(tps),
                           "p50_s": statistics.fmean(p50) if p50 else None,
                           "p99_s": statistics.fmean(p99) if p99 else None,
                           "redo_true": sum(run["redo_true"] for run in runs),
                           "redo_false": sum(run["redo_false"] for run in runs),
                           "cas_lost": sum(run["cas_lost"] for run in runs),
                           "lost_updates": sum(run["lost_updates"] for run in runs),
                           "requests_per_commit": (sum(sum(run["requests"].values()) for run in runs) / applied
                                                   if applied else None)}
                    rows.append(row)
                    print(f"  {system:<8} hot={hot} G={group} writers={n:<3} {row['commits_per_s_mean']:6.2f} commits/s "
                          f"p50={row['p50_s'] or 0:6.2f}s p99={row['p99_s'] or 0:6.2f}s "
                          f"redo true={row['redo_true']} false={row['redo_false']} cas_lost={row['cas_lost']} "
                          f"req/commit={row['requests_per_commit'] or 0:5.1f} lost={row['lost_updates']}", flush=True)
    _write("threads.json", {
        "note": "real threads; request and think times are the S3 medians and a 2 s mean think time, both "
                "times scale; throughput and latency are scaled back to S3 time (CPU time is therefore "
                "counted at 1/scale its real weight)",
        "scale": THREAD_SCALE, "work_mean_s": THREAD_WORK_S, "files": THREAD_FILES, "rows": THREAD_ROWS,
        "rows_out": rows})


# ---------------------------------------------------------------------------
# Fault injection (rubric: crash/recovery invariants I1 to I5)


def _random_branches(rng: random.Random, n_rows: int):
    """Two branches with disjoint random updates, deletes and inserts."""
    ids = list(range(n_rows))
    rng.shuffle(ids)
    cut = n_rows // 2
    left_ids, right_ids = ids[:cut], ids[cut:]
    left = {"updates": {rid: {"value": 1000 + rid} for rid in left_ids[: rng.randrange(1, 6)]},
            "deletes": left_ids[6: 6 + rng.randrange(0, 4)], "inserts": {}}
    right = {"updates": {rid: {"value": 2000 + rid} for rid in right_ids[: rng.randrange(1, 6)]},
             "deletes": [], "inserts": {}}
    if rng.random() < 0.5:
        right["inserts"] = {10_000 + rng.randrange(1000): {"id": 10_000, "kind": "user", "value": 7}}
    return left, right


def run_recovery(quick: bool) -> None:
    """Crash a commit, a merge and a group commit before each of their PUTs and check I1 to I5."""
    from semlake import reader
    from semlake.maintenance import FaultyStore, check_repository, collect_garbage, reachable_objects
    from semlake.metadata import CrashError

    schema = Schema((SchemaField("id", "int64"), SchemaField("kind", "string"), SchemaField("value", "int64")))
    scenarios = 5 if quick else 100
    points = {"commit": 0, "merge": 0, "group": 0}
    failures: list[str] = []
    orphans_total = 0

    def build(seed: int):
        rng = random.Random(seed)
        n_rows = rng.choice([24, 40, 64])
        store = FaultyStore()
        repo = Repository(store)
        rows = {i: {"id": i, "kind": "vip" if i % 4 == 0 else "user", "value": i} for i in range(n_rows)}
        base = repo.create_table("main", schema, rows=rows, rows_per_file=rng.choice([4, 8, 16]))
        left_spec, right_spec = _random_branches(rng, n_rows)
        return repo, store, base, left_spec, right_spec

    def commit(repo, base, name, spec):
        return repo.apply(base, name, updates=spec["updates"] or None, deletes=spec["deletes"] or None,
                          inserts=spec["inserts"] or None)

    def check(label: str, condition: bool) -> None:
        if not condition:
            failures.append(label)

    for seed in range(scenarios):
        # Reference run without a crash.
        repo, store, base, left_spec, right_spec = build(seed)
        before = store.puts_seen
        left = commit(repo, base, "A", left_spec)
        commit_puts = store.puts_seen - before
        expected_left = reader.materialize(repo, left.snapshot_id)
        right = commit(repo, base, "B", right_spec)
        before = store.puts_seen
        merged = three_way_merge(repo, left.snapshot_id, right.snapshot_id, store)
        merge_puts = store.puts_seen - before
        expected = reader.materialize(repo, merged.snapshot_id)

        for k in range(1, commit_puts + 1):          # crash inside a branch commit
            repo, store, base, left_spec, right_spec = build(seed)
            heads, snaps = dict(repo.heads), set(repo.snapshots)
            store.crash_at = store.puts_seen + k
            try:
                commit(repo, base, "A", left_spec)
                check(f"seed {seed} commit {k}: no crash", False)
            except CrashError:
                pass
            store.crash_at = None
            points["commit"] += 1
            check(f"seed {seed} commit {k}: I2 heads", repo.heads == heads and set(repo.snapshots) == snaps)
            check(f"seed {seed} commit {k}: I1", check_repository(repo) == [])
            orphans = collect_garbage(repo)
            orphans_total += len(orphans)
            check(f"seed {seed} commit {k}: I5", len(orphans) == k - 1 and set(store.objects) == reachable_objects(repo))
            retry = commit(repo, base, "A", left_spec)
            check(f"seed {seed} commit {k}: I3", reader.materialize(repo, retry.snapshot_id) == expected_left)

        for k in range(1, merge_puts + 1):           # crash inside a merge
            repo, store, base, left_spec, right_spec = build(seed)
            left = commit(repo, base, "A", left_spec)
            right = commit(repo, base, "B", right_spec)
            repo.heads["A"], repo.heads["B"] = left.snapshot_id, right.snapshot_id
            heads, snaps = dict(repo.heads), set(repo.snapshots)
            store.crash_at = store.puts_seen + k
            try:
                three_way_merge(repo, left.snapshot_id, right.snapshot_id, store)
                check(f"seed {seed} merge {k}: no crash", False)
            except CrashError:
                pass
            store.crash_at = None
            points["merge"] += 1
            check(f"seed {seed} merge {k}: I2", repo.heads == heads and set(repo.snapshots) == snaps)
            check(f"seed {seed} merge {k}: I1", check_repository(repo) == [])
            orphans = collect_garbage(repo)
            orphans_total += len(orphans)
            check(f"seed {seed} merge {k}: I5", len(orphans) == k - 1 and set(store.objects) == reachable_objects(repo))
            result = three_way_merge(repo, left.snapshot_id, right.snapshot_id, store)
            check(f"seed {seed} merge {k}: cas", repo.cas_head("main", base.snapshot_id, result.snapshot_id))
            check(f"seed {seed} merge {k}: I1 after retry", check_repository(repo) == [])
            check(f"seed {seed} merge {k}: I3", reader.materialize(repo, repo.heads["main"]) == expected)

        # Group commit: four branches merged onto the head, one compare-and-swap for the batch.
        def batch_setup():
            repo, store, base, _l, _r = build(seed)
            members = [repo.apply(base, f"g{i}", updates={i: {"value": 5000 + i}}) for i in range(4)]
            return repo, store, base, members

        def publish(repo, store, members) -> str:
            current = repo.heads["main"]
            for snap in members:
                current = three_way_merge(repo, current, snap.snapshot_id, store).snapshot_id
            assert repo.cas_head("main", repo.heads["main"], current)
            return current

        repo, store, base, members = batch_setup()
        before = store.puts_seen
        publish(repo, store, members)
        batch_puts = store.puts_seen - before
        final = reader.materialize(repo, repo.heads["main"])
        for k in range(1, batch_puts + 1):
            repo, store, base, members = batch_setup()
            store.crash_at = store.puts_seen + k
            try:
                publish(repo, store, members)
                check(f"seed {seed} group {k}: no crash", False)
            except CrashError:
                pass
            store.crash_at = None
            points["group"] += 1
            check(f"seed {seed} group {k}: I4", repo.heads["main"] == base.snapshot_id)
            orphans_total += len(collect_garbage(repo, roots=tuple(snap.snapshot_id for snap in members)))
            check(f"seed {seed} group {k}: I1", check_repository(repo) == [])
            publish(repo, store, members)
            check(f"seed {seed} group {k}: I3", reader.materialize(repo, repo.heads["main"]) == final)

    doc = {"scenarios": scenarios, "crash_points": points, "crash_points_total": sum(points.values()),
           "violations": len(failures), "failures": failures[:20], "orphans_collected": orphans_total,
           "note": "each crash point is one run that raises before the k-th PUT of the operation; the "
                   "catalog (heads and snapshot graph) is the in-process store the prototype uses"}
    print(f"  {scenarios} scenarios, {doc['crash_points_total']} crash points {points}, "
          f"{len(failures)} violations, {orphans_total} orphans collected")
    for line in failures[:10]:
        print("   FAIL", line)
    _write("recovery.json", doc)


# ---------------------------------------------------------------------------
# Pre-image pruning (reviewer MC5): bytes saved, and what it costs in precision


def run_prune(quick: bool) -> None:
    """Summary size of a purge before and after pruning, and the merges it then blocks.

    A purge deletes ``n`` rows (``region = 'r0'``-style clause outside the fragment so the
    row-value tier decides). Two reports race with it: one whose clause no deleted row
    satisfies (safe) and one that read deleted rows (a true conflict). The pair is merged
    with full pre-images, with pre-images projected onto the filter columns, and with
    pre-images removed.
    """
    from semlake.maintenance import prune_preimages

    sizes = [1_000, 10_000] if quick else [1_000, 10_000, 100_000, 1_000_000]
    rows_out = []
    for n in sizes:
        table_rows = max(2 * n, 20_000)
        schema = Schema((SchemaField("id", "int64"), SchemaField("region", "string"),
                         SchemaField("value", "int64"), SchemaField("score", "double"),
                         SchemaField("note", "string")))
        rows = {i: {"id": i, "region": f"r{i % 7}", "value": i % 1000, "score": float(i % 13),
                    "note": f"customer note {i:08d} with some free text"} for i in range(table_rows)}
        variants = {}
        for label, keep in (("full", None), ("filter_columns", ("region", "value")), ("none", ())):
            repo = Repository(MemoryObjectStore())
            base = repo.create_table("main", schema, rows=rows, rows_per_file=100_000)
            # NOT (...) keeps the clause outside the static fragment.
            purge = repo.apply(base, "purge", deletes=list(range(n)), purge=True,
                               predicates=[Predicate("purge", "deletes_where", where=f"NOT id >= {n}")])
            safe = repo.apply(base, "safe", predicates=[
                Predicate("safe", "reads_where", where="region = 'nowhere' AND value >= 0", columns=frozenset({"score"}))])
            hit = repo.apply(base, "hit", predicates=[
                Predicate("hit", "reads_where", where="region = 'r3' AND value < 500", columns=frozenset({"score"}))])
            before = len(repo.store.objects[purge.summary_key])
            if keep is not None:
                prune_preimages(repo, purge.snapshot_id, keep)
            after = len(repo.store.objects[purge.summary_key])
            outcome = {}
            for name, reader_snap in (("safe", safe), ("hit", hit)):
                started = time.perf_counter()
                result = three_way_merge(repo, purge.snapshot_id, reader_snap.snapshot_id, repo.store.clone())
                outcome[name] = {"status": result.status, "merge_s": time.perf_counter() - started}
            variants[label] = {"summary_bytes": after, "bytes_per_deleted_row": after / n,
                               "safe_report": outcome["safe"]["status"], "conflicting_report": outcome["hit"]["status"],
                               "merge_s": outcome["safe"]["merge_s"]}
            variants.setdefault("unpruned_bytes", before)
        rows_out.append({"rows_deleted": n, "table_rows": table_rows, **variants})
        print(f"  deleted={n:>9,} full={variants['full']['summary_bytes'] / 1e6:8.2f}MB "
              f"filter cols={variants['filter_columns']['summary_bytes'] / 1e6:8.2f}MB "
              f"none={variants['none']['summary_bytes'] / 1e6:8.2f}MB | safe report: "
              f"{variants['full']['safe_report']}/{variants['filter_columns']['safe_report']}/{variants['none']['safe_report']}"
              f" | conflicting report: {variants['full']['conflicting_report']}/"
              f"{variants['filter_columns']['conflicting_report']}/{variants['none']['conflicting_report']}", flush=True)
    _write("prune.json", {"rows": rows_out})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("which", choices=["ledger", "tails", "sweep", "threads", "recovery", "prune", "all", "sweep-point"])
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--kind", default="delete")
    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--preds", type=int, default=1)
    parser.add_argument("--vectorized", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if args.which == "sweep-point":
        print(json.dumps(sweep_point(args.kind, args.n, args.preds, bool(args.vectorized), args.repeats)))
        return
    if args.which in ("ledger", "all"):
        run_ledger(args.quick)
    if args.which in ("tails", "all"):
        run_tails(args.quick)
    if args.which in ("sweep", "all"):
        run_sweep(args.quick)
    if args.which in ("threads", "all"):
        run_threads(args.quick)
    if args.which in ("recovery", "all"):
        run_recovery(args.quick)
    if args.which in ("prune", "all"):
        run_prune(args.quick)


if __name__ == "__main__":
    main()
