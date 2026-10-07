"""Read benchmark_run_summary.csv and write the three milestone figures."""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

SYSTEMS = ("semlake", "b1", "b2", "b3", "b4")
LABELS = {
    "semlake": "SemLake",
    "b1": "B1 File-OCC",
    "b2": "B2 CoW",
    "b3": "B3 TupleVer",
    "b4": "B4 CRDT",
}
COLORS = {
    "semlake": "#0072B2",
    "b1": "#D55E00",
    "b2": "#009E73",
    "b3": "#CC79A7",
    "b4": "#E69F00",
}
MARKERS = {"semlake": "o", "b1": "s", "b2": "^", "b3": "D", "b4": "v"}


def render(csv_path: Path, out_dir: Path) -> list[Path]:
    """Write every figure as PDF and PNG and return the written paths."""
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = _read_csv(csv_path)
    _apply_style()
    written = []
    written += _plot_merge_time(rows, out_dir)
    written += _plot_cost(rows, out_dir)
    written += _plot_tps(rows, out_dir)
    return written


def _read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _apply_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 11,
            "axes.labelsize": 12,
            "axes.titlesize": 13,
            "legend.fontsize": 10,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "axes.grid": True,
            "grid.alpha": 0.35,
            "grid.linestyle": "--",
        }
    )


def _save(fig, out_dir: Path, name: str) -> list[Path]:
    fig.tight_layout()
    paths = []
    for suffix in (".pdf", ".png"):
        target = out_dir / f"{name}{suffix}"
        fig.savefig(target, dpi=200, bbox_inches="tight")
        paths.append(target)
    plt.close(fig)
    return paths


def _group_by_system(rows: list[dict], workload: str) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {system: [] for system in SYSTEMS}
    for row in rows:
        if row["workload"] == workload and row["system"] in grouped:
            grouped[row["system"]].append(row)
    return grouped


def _plot_lines(ax, grouped: dict[str, list[dict]], x_key: str, y_key: str) -> None:
    for system in SYSTEMS:
        points = sorted(grouped[system], key=lambda row: float(row[x_key]))
        if not points:
            continue
        ax.plot(
            [float(row[x_key]) for row in points],
            [float(row[y_key]) for row in points],
            color=COLORS[system],
            marker=MARKERS[system],
            linewidth=1.8,
            label=LABELS[system],
        )


def _add_footnote(fig, text: str, y: float = -0.02) -> None:
    fig.text(0.01, y, text, fontsize=8, color="#444444")


def _plot_merge_time(rows: list[dict], out_dir: Path) -> list[Path]:
    grouped = _group_by_system(rows, "W4")
    fig, ax = plt.subplots(figsize=(6.4, 4.0))
    _plot_lines(ax, grouped, "manifest_depth", "merge_cpu_s")
    ax.set_xlabel("Manifest files")
    ax.set_ylabel("Merge CPU time (s)")
    ax.set_title("Merge CPU time vs. manifest depth")
    if _needs_log_scale(grouped, "merge_cpu_s"):
        ax.set_yscale("log")
    ax.legend(frameon=False)
    _add_footnote(
        fig,
        "Measured in-process with no network. The CSV also has an estimated cloud latency "
        "(CPU + S3 RTT + bytes/bandwidth).",
    )
    return _save(fig, out_dir, "latency_vs_manifest_depth")


def _plot_cost(rows: list[dict], out_dir: Path) -> list[Path]:
    grouped = _group_by_system(rows, "W1S")
    depths = [int(row["manifest_depth"]) for system in SYSTEMS for row in grouped[system]]
    if not depths:
        return []
    depth = max(depths)
    picked = [
        row
        for system in SYSTEMS
        for row in grouped[system]
        if int(row["manifest_depth"]) == depth
    ]
    labels = [LABELS[row["system"]] for row in picked]
    requests = [float(row["s3_requests"]) for row in picked]
    costs = [float(row["cost_usd"]) for row in picked]
    colors = [COLORS[row["system"]] for row in picked]

    fig, axes = plt.subplots(1, 2, figsize=(8.4, 3.9))
    axes[0].bar(labels, requests, color=colors)
    axes[0].set_ylabel("S3 requests")
    axes[0].set_title(f"Requests at {depth} files")
    axes[1].bar(labels, costs, color=colors)
    axes[1].set_ylabel("Estimated API cost (USD)")
    axes[1].set_title(f"Request cost at {depth} files")
    for ax in axes:
        ax.tick_params(axis="x", labelrotation=15)
    if costs and max(costs) > 20 * max(min(costs), 1e-12):
        axes[1].set_yscale("log")
    if requests and max(requests) > 20 * max(min(requests), 1):
        axes[0].set_yscale("log")
    fig.suptitle("S3 requests and cost for disjoint file updates", y=1.02)
    _add_footnote(
        fig,
        "W1S: each branch updates rows in a different half of the files, and B2 rewrites every one of them.",
        y=-0.04,
    )
    return _save(fig, out_dir, "s3_cost_comparison")


def _plot_tps(rows: list[dict], out_dir: Path) -> list[Path]:
    grouped = _group_by_system(rows, "W2")
    fig, ax = plt.subplots(figsize=(6.4, 4.0))
    _plot_lines(ax, grouped, "contention", "tps")
    ax.set_xlabel("Contention rate")
    ax.set_ylabel("Successful merges per CPU second")
    ax.set_title("Throughput vs. contention")
    ax.legend(frameon=False)
    _add_footnote(fig, "No client retry loop. A merge succeeds if it commits clean or with a VSA node.")
    return _save(fig, out_dir, "tps_vs_contention")


def _needs_log_scale(grouped: dict[str, list[dict]], key: str) -> bool:
    values = [
        float(row[key])
        for system in SYSTEMS
        for row in grouped[system]
        if row.get(key) not in ("", None)
    ]
    positive = [value for value in values if value > 0]
    if len(values) < 2 or not positive:
        return False
    return max(values) > 20 * min(positive)
