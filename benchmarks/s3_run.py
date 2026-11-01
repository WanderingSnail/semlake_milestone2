"""Merge W4 and W1S against a real S3 bucket and compare with the latency model.

Reads the bucket from ``SEMLAKE_S3_BUCKET`` and credentials from the usual AWS
variables. Each repeat copies the scenario's objects to a fresh prefix (setup,
not counted), times one merge with wall-clock time, then removes the prefix.
Writes ``results/s3_run.json``.

    python3 benchmarks/s3_run.py [--depths 1000] [--repeats 5] [--endpoint URL]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.profiler import estimated_latency_s, pricing_doc  # noqa: E402
from benchmarks.workloads import workload_w1s, workload_w4  # noqa: E402
from semlake.baselines import copy_on_write  # noqa: E402
from semlake.merge import three_way_merge  # noqa: E402
from semlake.s3store import S3ObjectStore  # noqa: E402
from semlake.storage import Counters  # noqa: E402

RESULTS = Path(__file__).resolve().parent / "results"
# name -> (merge function, concurrent requests). b2_parallel issues its file
# GETs and PUTs on 32 threads, as a production copy-on-write engine would.
SYSTEMS = {"semlake": (three_way_merge, 1), "b2": (copy_on_write, 1), "b2_parallel": (copy_on_write, 32)}
FILE_SIZE = 4096


def _probe(bucket: str, endpoint: str | None, n: int = 40) -> dict:
    """Median seconds of one small PUT, GET and LIST from this client: the calibrated model."""
    store = S3ObjectStore(bucket, f"semlake-bench/probe-{uuid.uuid4().hex}", endpoint_url=endpoint)
    times: dict[str, list[float]] = {"put": [], "get": [], "list": []}
    try:
        store.put("warm", b"x")
        for i in range(n):
            for op, call in (("put", lambda: store.put(f"k{i}", bytes(FILE_SIZE))),
                             ("get", lambda: store.get(f"k{i}")),
                             ("list", lambda: store.list("warm"))):
                started = time.perf_counter()
                call()
                times[op].append(time.perf_counter() - started)
    finally:
        store.delete_all()
    return {op: sorted(vals)[len(vals) // 2] for op, vals in times.items()}


def _calibrated(cpu: float, row: dict, probe: dict) -> float:
    return cpu + row["put"] * probe["put"] + row["get"] * probe["get"] + row["list"] * probe["list"]


def _one(scenario, merge_fn, bucket: str, endpoint: str | None, workers: int = 1) -> dict:
    store = S3ObjectStore(bucket, f"semlake-bench/{uuid.uuid4().hex}", endpoint_url=endpoint, workers=workers)
    store.upload_all(scenario.repo.store.objects)
    try:
        cpu0 = time.process_time()
        wall0 = time.perf_counter()
        result = merge_fn(scenario.repo, scenario.left_id, scenario.right_id, store)
        wall = time.perf_counter() - wall0
        cpu = time.process_time() - cpu0
    finally:
        store.delete_all()
    c = store.counters
    return {
        "status": result.status,
        "wall_s": wall,
        "cpu_s": cpu,
        "put": c.put,
        "get": c.get,
        "list": c.list,
        "put_bytes": c.put_bytes,
        "model_s": estimated_latency_s(cpu, c),
    }


def _memory_cpu(scenario, merge_fn) -> float:
    """Median in-memory merge time, the CPU term the model adds round trips to."""
    times = []
    for _ in range(5):
        scratch = scenario.repo.store.clone()
        started = time.perf_counter()
        merge_fn(scenario.repo, scenario.left_id, scenario.right_id, scratch)
        times.append(time.perf_counter() - started)
    return sorted(times)[len(times) // 2]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--depths", type=int, nargs="+", default=[1000])
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--sequential-max", type=int, default=1000,
                        help="run the one-request-at-a-time B2 only up to this many files")
    parser.add_argument("--out", default="s3_run.json")
    parser.add_argument("--endpoint", default=os.environ.get("SEMLAKE_S3_ENDPOINT"))
    args = parser.parse_args()
    bucket = os.environ.get("SEMLAKE_S3_BUCKET")
    if not bucket:
        sys.exit("SEMLAKE_S3_BUCKET is not set")

    probe = _probe(bucket, args.endpoint)
    print("probe (median s):", {op: round(v, 4) for op, v in probe.items()}, flush=True)
    rows = []
    for depth in args.depths:
        for build in (workload_w4, workload_w1s):
            scenario = build(depth, FILE_SIZE)
            for system, (merge_fn, workers) in SYSTEMS.items():
                if system == "b2" and depth > args.sequential_max:
                    continue
                mem_cpu = _memory_cpu(scenario, merge_fn)
                _one(scenario, merge_fn, bucket, args.endpoint, workers)  # warm-up: connections, imports
                runs = [_one(scenario, merge_fn, bucket, args.endpoint, workers) for _ in range(args.repeats)]
                walls = sorted(r["wall_s"] for r in runs)
                mid = sorted(runs, key=lambda r: r["wall_s"])[len(runs) // 2]
                row = {
                    "workload": scenario.workload,
                    "depth": depth,
                    "system": system,
                    "status": mid["status"],
                    "repeats": args.repeats,
                    "wall_s": mid["wall_s"],
                    "wall_low_s": walls[0],
                    "wall_high_s": walls[-1],
                    "memory_cpu_s": mem_cpu,
                    "put": mid["put"],
                    "get": mid["get"],
                    "list": mid["list"],
                    "put_bytes": mid["put_bytes"],
                    "model_s": estimated_latency_s(mem_cpu, Counters(
                        put=mid["put"], get=mid["get"], list=mid["list"], put_bytes=mid["put_bytes"])),
                    "workers": workers,
                    "runs": runs,
                }
                if workers == 1:
                    row["calibrated_model_s"] = _calibrated(mem_cpu, row, probe)
                rows.append(row)
                print(f"{row['workload']:4} {depth:6} {system:8} {row['status']:8} "
                      f"wall={row['wall_s']:.3f}s model={row['model_s']:.3f}s "
                      f"calibrated={row.get('calibrated_model_s', float('nan')):.3f}s "
                      f"req={row['put'] + row['get'] + row['list']:.0f}", flush=True)
    RESULTS.mkdir(exist_ok=True)
    doc = {"bucket_region": os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION")),
           "endpoint": args.endpoint, "pricing": pricing_doc(), "probe_s": probe, "rows": rows}
    (RESULTS / args.out).write_text(json.dumps(doc, indent=1, default=float))


if __name__ == "__main__":
    main()
