"""Figures added in the revision, drawn from the raw result files only.

    python benchmarks/revision_figures.py            # every figure whose result file exists

    fig_ledger       results/ledger.json        requests and end-to-end time per merge, all systems
    fig_scalability  results/tails.json         P50/P99 merge CPU over contention, manifest size, history
    fig_sweep        results/delta_sweep.json   merge time, memory and summary bytes over delta size
    fig_threads      results/threads.json       commit throughput and latency with real threads
    fig_swarm        results/scion_sim_calibrated_summary.json   simulated swarm, equal batching
    fig_packing      results/packing.json       false aborts against rows per file

All figures are vector PDF with embedded TrueType fonts (``pdf.fonttype = 42``).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import NullFormatter  # noqa: E402

from benchmarks.paper_experiments import (  # noqa: E402
    COL_W,
    COLORS,
    FULL_W,
    SERIES,
    SWARM_SUMMARY,
    _legend_above,
    _load,
    _plot,
    _save,
    _style,
)

ORDER = ("b1", "b1r", "b2", "b3", "b4", "semlake")
GREY = "#7F7F7F"


def _whisker(pts: list[dict], key: str = "cpu_s", scale: float = 1e3):
    """P50 values and the one-sided bar that reaches P99."""
    mid = [p[key]["p50"] * scale for p in pts]
    top = [max(0.0, (p[key]["p99"] - p[key]["p50"]) * scale) for p in pts]
    return mid, [[0.0] * len(pts), top]


def fig_ledger() -> None:
    doc = _load("ledger.json")
    rows = [row for row in doc["rows"] if row["workload"] == "W1S"]
    rows.sort(key=lambda row: row["files"])
    files = [row["files"] / 1e3 for row in rows]
    fig, axes = plt.subplots(1, 2, figsize=(COL_W, 1.45), constrained_layout=True)
    for name in ORDER:
        style = dict(linestyle="--", zorder=4) if name == "b1r" else {}
        requests = [row["systems"][name]["counters"]["requests"] for row in rows]
        _plot(axes[0], name, files, requests, **style)
        mid = [row["systems"][name]["end_to_end_s"]["p50"] for row in rows]
        top = [max(0.0, row["systems"][name]["end_to_end_s"]["p99"] - m) for row, m in zip(rows, mid)]
        _plot(axes[1], name, files, mid, yerr=[[0.0] * len(mid), top], **style)
    axes[0].set(yscale="log", xlabel="Files touched (thousands)\n(a) Requests per merge", ylabel="Object-store requests")
    axes[1].set(yscale="log", xlabel="Files touched (thousands)\n(b) End-to-end merge time", ylabel="Seconds (P50, bar to P99)")
    for ax in axes:
        ax.set_xticks(files, [f"{f:g}" for f in files])
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, [label.split(" ")[0] for label in labels], loc="lower center", ncol=6,
               bbox_to_anchor=(0.5, 1.0), columnspacing=0.8, handlelength=1.6)
    _save(fig, "fig_ledger")


def fig_scalability() -> None:
    doc = _load("tails.json")
    fig, axes = plt.subplots(1, 3, figsize=(FULL_W, 1.45), constrained_layout=True)
    for name in ORDER:
        style = dict(linestyle="--", zorder=4) if name == "b1r" else {}
        pts = sorted((r for r in doc["contention"] if r["system"] == name), key=lambda r: r["rho"])
        mid, err = _whisker(pts)
        _plot(axes[0], name, list(range(len(pts))), mid, yerr=err, **style)
        rho_labels = [f"{r['rho'] * 100:g}" for r in pts]
        pts = sorted((r for r in doc["manifest"] if r["system"] == name), key=lambda r: r["files"])
        mid, err = _whisker(pts)
        _plot(axes[1], name, [r["files"] / 1e3 for r in pts], mid, yerr=err, **style)
    pts = sorted(doc["history"], key=lambda r: r["snapshots"])
    mid, err = _whisker(pts, "merge_s")
    _plot(axes[2], "semlake", [r["snapshots"] for r in pts], mid, yerr=err, label="three-way merge")
    mid, err = _whisker(pts, "lca_s")
    _plot(axes[2], "semlake", [r["snapshots"] for r in pts], mid, yerr=err, label="LCA search",
          color=GREY, marker="D", linestyle="--")
    files = doc.get("contention_files", 10_000)
    axes[0].set(xlabel=f"Hot-row contention ρ (%)\n(a) W2 on {files // 1000}K files", ylabel="Merge CPU (ms), P50 to P99",
                yscale="log")
    axes[0].set_xticks(list(range(len(rho_labels))), rho_labels)
    axes[1].set(xlabel="Files in manifest (thousands)\n(b) W4, manifest size", ylabel="Merge CPU (ms), P50 to P99",
                yscale="log")
    axes[2].set(xlabel="Snapshots in history\n(c) W4, history depth", ylabel="CPU (ms), P50 to P99", yscale="log")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower left", ncol=6, bbox_to_anchor=(0.02, 1.0), columnspacing=0.9)
    axes[2].legend(loc="lower center", ncol=2, bbox_to_anchor=(0.5, 1.0))
    _save(fig, "fig_scalability")


def fig_sweep() -> None:
    doc = _load("delta_sweep.json")
    rows = doc["rows"]
    fig, axes = plt.subplots(1, 3, figsize=(FULL_W, 1.45), constrained_layout=True)
    looks = {
        ("delete", True): dict(color=COLORS["semlake"], marker="o", linestyle="-", label="delete, one pass"),
        ("delete", False): dict(color=COLORS["semlake"], marker="o", linestyle=":", label="delete, per-row loop"),
        ("update", True): dict(color=COLORS["b1"], marker="s", linestyle="-", label="update, one pass"),
        ("update", False): dict(color=COLORS["b1"], marker="s", linestyle=":", label="update, per-row loop"),
    }
    base_preds = min(r["predicates"] for r in rows)
    for (kind, vec), look in looks.items():
        pts = sorted((r for r in rows if r["kind"] == kind and r["vectorized"] == vec and r["predicates"] == base_preds),
                     key=lambda r: r["rows_changed"])
        if not pts:
            continue
        mid, err = _whisker(pts, "merge_s", 1.0)
        axes[0].errorbar([r["rows_changed"] for r in pts], mid, yerr=err, elinewidth=0.6, markeredgewidth=0, **look)
    axes[0].set(xscale="log", yscale="log", xlabel="Rows changed on the branch\n(a) Merge time",
                ylabel="Seconds (P50, bar to P99)")
    _legend_above(axes[0], ncol=2)

    pts = sorted((r for r in rows if r["kind"] == "delete" and r["vectorized"] and r["predicates"] == base_preds),
                 key=lambda r: r["rows_changed"])
    x = [r["rows_changed"] for r in pts]
    axes[1].plot(x, [r["summary_bytes"] / 1e6 for r in pts], color=COLORS["semlake"], marker="o", markeredgewidth=0,
                 label="summary, full pre-images")
    try:
        prune = sorted(_load("prune.json")["rows"], key=lambda r: r["rows_deleted"])
        axes[1].plot([r["rows_deleted"] for r in prune], [r["none"]["summary_bytes"] / 1e6 for r in prune],
                     color=COLORS["semlake"], marker="^", linestyle=":", markeredgewidth=0,
                     label="summary, pruned")
    except FileNotFoundError:
        pass
    axes[1].plot(x, [r["peak_rss_mb"] for r in pts], color=GREY, marker="D", linestyle="--", markeredgewidth=0,
                 label="peak memory of the merge")
    axes[1].set(xscale="log", yscale="log", xlabel="Rows deleted on the branch\n(b) Bytes and memory", ylabel="MB")
    _legend_above(axes[1], ncol=2)

    fixed = max((r["rows_changed"] for r in rows if r["predicates"] > base_preds), default=None)
    if fixed is not None:
        for vec, look in ((True, dict(color=COLORS["semlake"], marker="o", linestyle="-", label="one pass per clause")),
                          (False, dict(color=COLORS["semlake"], marker="o", linestyle=":", label="per-row loop"))):
            pts = sorted((r for r in rows if r["kind"] == "delete" and r["vectorized"] == vec
                          and r["rows_changed"] == fixed), key=lambda r: r["predicates"])
            if pts:
                mid, err = _whisker(pts, "merge_s", 1.0)
                axes[2].errorbar([r["predicates"] for r in pts], mid, yerr=err, elinewidth=0.6, markeredgewidth=0, **look)
        axes[2].set(xscale="log", yscale="log",
                    xlabel=f"Read and delete clauses per branch\n(c) {fixed:,} rows deleted", ylabel="Seconds (P50, bar to P99)")
        axes[2].set_xticks([1, 4, 16, 64], ["1", "4", "16", "64"])
        axes[2].xaxis.set_minor_formatter(NullFormatter())
        axes[2].set_yticks([1, 2, 4, 8], ["1", "2", "4", "8"])
        axes[2].yaxis.set_minor_formatter(NullFormatter())
        _legend_above(axes[2], ncol=2)
    _save(fig, "fig_sweep")


def fig_threads() -> None:
    doc = _load("threads.json")
    rows = doc["rows_out"]
    looks = {
        ("semlake", 0): dict(color=COLORS["semlake"], marker="o", linestyle=":", label="SemLake, clients race"),
        ("semlake", 8): dict(color=COLORS["semlake"], marker="s", linestyle="-", label="SemLake, service G=8"),
        ("b1", 0): dict(color=COLORS["b1"], marker="o", linestyle=":", label="B1, clients race"),
        ("b1", 8): dict(color=COLORS["b1"], marker="s", linestyle="-", label="B1, service G=8"),
    }
    fig, axes = plt.subplots(1, 2, figsize=(COL_W, 1.55), constrained_layout=True)
    for (system, group), look in looks.items():
        pts = sorted((r for r in rows if r["system"] == system and r["group"] == group and r["hot"] == 0.0),
                     key=lambda r: r["writers"])
        if not pts:
            continue
        x = [r["writers"] for r in pts]
        mean = [r["commits_per_s_mean"] for r in pts]
        err = [[m - r["commits_per_s_min"] for m, r in zip(mean, pts)],
               [r["commits_per_s_max"] - m for m, r in zip(mean, pts)]]
        axes[0].errorbar(x, mean, yerr=err, elinewidth=0.6, markeredgewidth=0, **look)
        p50 = [r["p50_s"] for r in pts]
        axes[1].errorbar(x, p50, yerr=[[0.0] * len(pts), [max(0.0, r["p99_s"] - r["p50_s"]) for r in pts]],
                         elinewidth=0.6, markeredgewidth=0, **look)
    axes[0].set(xscale="log", yscale="log", xlabel="Writer threads\n(a) Throughput", ylabel="Commits / s")
    axes[1].set(xscale="log", yscale="log", xlabel="Writer threads\n(b) Commit latency",
                ylabel="Seconds (P50 to P99)")
    writers = sorted({r["writers"] for r in rows})
    for ax in axes:
        ax.set_xticks(writers, [str(w) for w in writers])
        ax.xaxis.set_minor_formatter(NullFormatter())
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=2, bbox_to_anchor=(0.5, 1.0), columnspacing=0.9)
    _save(fig, "fig_threads")


def fig_swarm() -> None:
    swarm = _load(SWARM_SUMMARY)
    rows = [r for r in swarm["rows"] if r["mix"] == "agentic" and r["hot_rho"] == 0.0]
    agents = sorted({r["agents"] for r in rows})
    names = {"b1": "B1", "b1r": "B1R", "b2": "B2", "semlake": "SemLake"}
    fig, axes = plt.subplots(1, 2, figsize=(FULL_W, 1.45), constrained_layout=True)

    def series(system: str, metric: str):
        vals = [next((r for r in rows if r["agents"] == a and r["system"] == system), None) for a in agents]
        if not all(vals):
            return None
        return [v[metric] for v in vals], [v[f"{metric}_ci"] for v in vals]

    for base in ("b1", "b1r", "b2", "semlake"):
        for suffix, look in (("", dict(linestyle=":", marker="o")), ("_group", dict(linestyle="-", marker="s"))):
            got = series(base + suffix, "commits_per_s")
            if got is None:
                continue
            label = f"{names[base]}, {'service G=' + str(swarm.get('group_size', 8)) if suffix else 'clients race'}"
            axes[0].errorbar(agents, got[0], yerr=got[1], elinewidth=0.6, markeredgewidth=0, color=COLORS[base],
                             label=label, **look)
    axes[0].set(xscale="log", xlabel="Concurrent agents\n(a) Applied commits per second", ylabel="Commits / s")
    for metric, system, label in (("redo_false_per_commit", "b1_group", "B1 false redos"),
                                  ("false_blocks_per_commit", "b1r_group", "B1R false blocks"),
                                  ("false_blocks_per_commit", "semlake_group", "SemLake false blocks")):
        got = series(system, metric)
        if got is None:
            continue
        base = system.removesuffix("_group")
        axes[1].errorbar(agents, got[0], yerr=got[1], elinewidth=0.6, markeredgewidth=0, color=COLORS[base],
                         marker=SERIES[base]["marker"], linestyle="-", label=label)
    axes[1].set(xscale="log", xlabel="Concurrent agents\n(b) Wasted attempts per applied commit, with the service",
                ylabel="Wasted attempts / commit")
    axes[1].set_yscale("symlog", linthresh=0.01)
    axes[1].set_ylim(bottom=0)
    for ax in axes:
        ax.set_xticks(agents, [str(a) for a in agents])
        ax.xaxis.set_minor_formatter(NullFormatter())
    _legend_above(axes[0], ncol=4)
    _legend_above(axes[1], ncol=3)
    _save(fig, "fig_swarm")


def fig_packing() -> None:
    pts = _load("packing.json")["points"]
    fig, ax = plt.subplots(figsize=(COL_W, 1.2), constrained_layout=True)
    for name in ("b1", "b1r", "semlake"):
        _plot(ax, name, [p["rows_per_file"] for p in pts], [p[f"{name}_false_abort_rate"] * 100 for p in pts],
              **(dict(linestyle="--", zorder=3) if name == "b1r" else {}))
    ax.set(xscale="log", xlabel="Rows per Parquet file", ylabel="False aborts (%)")
    ax.set_ylim(-4, 105)
    _legend_above(ax, ncol=3)
    _save(fig, "fig_packing")


def figures() -> None:
    _style()
    for fn in (fig_ledger, fig_scalability, fig_sweep, fig_threads, fig_swarm, fig_packing):
        try:
            fn()
            print(f"drew {fn.__name__}")
        except FileNotFoundError as exc:
            print(f"skipped {fn.__name__}: missing {exc.filename}")


if __name__ == "__main__":
    figures()
