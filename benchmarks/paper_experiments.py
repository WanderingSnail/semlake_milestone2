"""Run the remaining paper experiment and draw every paper figure from result files.

    python benchmarks/paper_experiments.py packing   # W1 false aborts vs rows per file (Fig. 8a)
    python benchmarks/paper_experiments.py figures   # draw all figures from benchmarks/results/

Every plotted number is read from a file in ``benchmarks/results/`` written by a script
in this repository:

    benchmark_run_summary.csv, m1s_cases.csv   benchmarks/run_all.py
    cost_efficiency.json                       benchmarks/cost_efficiency.py
    tpch_pairs.json, selectivity.json,
    tpch_scan.json, compaction.json            benchmarks/tpch_experiments.py
    scion_sim.csv, scion_sim_summary.json      benchmarks/scion_sim.py
    packing.json                               this file
"""

from __future__ import annotations

import csv
import json
import random
import sys
from pathlib import Path

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FIG_DIR = ROOT / "paper" / "figures"
RESULTS = ROOT / "benchmarks" / "results"

COLORS = {"semlake": "#0072B2", "semlake_group": "#0072B2", "semlake_static": "#E69F00", "b1": "#D55E00",
          "b1r": "#56B4E9", "b2": "#009E73", "b3": "#CC79A7", "b4": "#E69F00", "b6": "#999999"}
LABELS = {"semlake": "SemLake", "semlake_group": "SemLake (group commit, G=8)", "semlake_static": "SemLake (static only)",
          "b1": "B1 file OCC", "b1r": "B1R Delta-style", "b2": "B2 CoW", "b3": "B3 tuple versioning",
          "b4": "B4 CRDT", "b6": "B6 SSI"}


def _style() -> None:
    plt.rcParams.update({
        "font.family": "serif", "font.size": 8, "axes.labelsize": 8, "axes.titlesize": 8,
        "legend.fontsize": 7, "xtick.labelsize": 7, "ytick.labelsize": 7,
        "pdf.fonttype": 42, "ps.fonttype": 42, "axes.grid": True, "grid.alpha": 0.3, "grid.linestyle": "--",
    })


def _save(fig, name: str) -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    for ext in (".pdf", ".png"):
        fig.savefig(FIG_DIR / f"{name}{ext}", dpi=200, bbox_inches="tight")
    plt.close(fig)


def _load(name: str):
    return json.loads((RESULTS / name).read_text(encoding="utf-8"))


def _csv(name: str) -> list[dict]:
    with (RESULTS / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


# ---------------------------------------------------------------------------
# Experiment: W1 false aborts against rows per file


def run_packing(n_rows: int = 1024, trials: int = 50, rows_changed: int = 4) -> dict:
    """Two branches update disjoint random rows; count merges each system refuses."""
    from benchmarks.rule_baselines import b1r_merge
    from benchmarks.workloads import _user_schema
    from semlake.baselines import WriteConflictException, file_level_occ
    from semlake.merge import three_way_merge
    from semlake.metadata import Repository
    from semlake.storage import MemoryObjectStore

    systems = {"b1": file_level_occ, "b1r": b1r_merge, "semlake": three_way_merge}
    points = []
    for rows_per_file in (1, 4, 16, 64, 256, 1024):
        rng = random.Random(rows_per_file)
        repo = Repository(MemoryObjectStore())
        rows = {i: {"id": i, "kind": "user", "value": i} for i in range(n_rows)}
        base = repo.create_table("main", _user_schema(), rows=rows, rows_per_file=rows_per_file)
        refused = {name: 0 for name in systems}
        for _ in range(trials):
            picked = rng.sample(range(n_rows), 2 * rows_changed)
            left = repo.apply(base, "A", updates={r: {"value": -1} for r in picked[:rows_changed]})
            right = repo.apply(base, "B", updates={r: {"value": -2} for r in picked[rows_changed:]})
            for name, fn in systems.items():
                try:
                    result = fn(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
                    refused[name] += result.status not in ("clean",)
                except WriteConflictException:
                    refused[name] += 1
        point = {"rows_per_file": rows_per_file, "trials": trials,
                 **{f"{name}_false_abort_rate": refused[name] / trials for name in systems}}
        points.append(point)
        print("  " + json.dumps(point), flush=True)
    out = {"rows": n_rows, "rows_changed_per_branch": rows_changed, "points": points}
    (RESULTS / "packing.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    return out


# ---------------------------------------------------------------------------
# Figures


def fig_selectivity() -> None:
    data = _load("selectivity.json")
    pts = data["points"]
    x = [p["mean_fraction"] * 100 for p in pts]
    fig, ax = plt.subplots(figsize=(3.3, 1.9))
    for name, marker, style in (("b1r", "s", "-"), ("semlake_static", "^", "--"), ("semlake", "o", "-")):
        ax.plot(x, [p[f"{name}_false_block_rate"] for p in pts], marker=marker, linestyle=style,
                color=COLORS[name], linewidth=1.4, markersize=3.5, label=LABELS[name])
    ax.set_xscale("log")
    ax.set_xlabel("Rows deleted by the purge (% of lineitem, log)")
    ax.set_ylabel("False-block rate")
    ax.set_ylim(-0.05, 1.08)
    ax.legend(frameon=False, loc="center right")
    _save(fig, "fig_selectivity")


def fig_scalability() -> None:
    rows = _csv("benchmark_run_summary.csv")
    fig, axes = plt.subplots(1, 3, figsize=(7.0, 1.9))
    order = ("b1", "b2", "b3", "b4", "semlake")
    for name in order:
        pts = sorted((r for r in rows if r["workload"] == "W2L" and r["system"] == name),
                     key=lambda r: float(r["contention"]))
        axes[0].plot([float(r["contention"]) * 100 for r in pts], [float(r["merge_cpu_s"]) * 1e3 for r in pts],
                     marker="o", markersize=3, linewidth=1.2, color=COLORS[name], label=LABELS[name])
        pts = sorted((r for r in rows if r["workload"] == "W4" and r["system"] == name),
                     key=lambda r: float(r["manifest_depth"]))
        axes[1].errorbar([float(r["manifest_depth"]) / 1e3 for r in pts], [float(r["merge_cpu_s"]) * 1e3 for r in pts],
                         yerr=_err(pts), marker="o", markersize=3, linewidth=1.2, capsize=2, color=COLORS[name])
    for name, label, color in (("semlake", "3-way merge", COLORS["semlake"]), ("lca", "LCA search", "#D55E00")):
        pts = sorted((r for r in rows if r["workload"] == "H1" and r["system"] == name),
                     key=lambda r: float(r["manifest_depth"]))
        axes[2].errorbar([float(r["manifest_depth"]) for r in pts], [float(r["merge_cpu_s"]) * 1e3 for r in pts],
                         yerr=_err(pts) if name == "semlake" else None, marker="o", markersize=3, linewidth=1.2,
                         capsize=2, color=color, label=label)
    axes[0].set(xlabel="Hot-row contention ρ (%)", ylabel="Merge CPU (ms)", yscale="log", title="(a) W2, 10K files")
    axes[1].set(xlabel="Files in manifest (K)", ylabel="Merge CPU (ms)", yscale="log", title="(b) W4, manifest size")
    axes[2].set(xlabel="Snapshots in history", ylabel="CPU (ms)", yscale="log", title="(c) W4, history depth")
    handles, labels = axes[0].get_legend_handles_labels()
    extra_h, extra_l = axes[2].get_legend_handles_labels()
    fig.legend(handles + extra_h[1:], labels + extra_l[1:], frameon=False, fontsize=6.5,
               ncol=len(handles) + 1, loc="lower center", bbox_to_anchor=(0.5, 1.0))
    _save(fig, "fig_scalability")


def _err(pts: list[dict]):
    low = [float(r["merge_cpu_s"]) - float(r["merge_cpu_low_s"]) if r.get("merge_cpu_low_s") else 0 for r in pts]
    high = [float(r["merge_cpu_high_s"]) - float(r["merge_cpu_s"]) if r.get("merge_cpu_high_s") else 0 for r in pts]
    return [[v * 1e3 for v in low], [v * 1e3 for v in high]]


def fig_requests() -> None:
    rows = _csv("benchmark_run_summary.csv")
    depth = max(int(r["manifest_depth"]) for r in rows if r["workload"] == "W1S")
    by = {r["system"]: r for r in rows if r["workload"] == "W1S" and int(r["manifest_depth"]) == depth}
    order = ("b1", "b2", "b3", "b4", "semlake")
    labels = ["B1", "B2", "B3", "B4", "SemLake"]
    fig, axes = plt.subplots(1, 2, figsize=(3.4, 1.8))
    axes[0].bar(labels, [float(by[s]["s3_requests"]) for s in order], color=[COLORS[s] for s in order])
    axes[0].set(yscale="log", ylabel="Requests per merge", title=f"(a) Requests, {depth // 1000}K files")
    axes[1].bar(labels, [max(0.1, float(by[s]["put_bytes"]) / 1024) for s in order], color=[COLORS[s] for s in order])
    axes[1].set(yscale="log", ylabel="KB written per merge", title="(b) Bytes written")
    for ax in axes:
        ax.tick_params(axis="x", labelrotation=35)
    _save(fig, "fig_requests")


def fig_cost() -> None:
    doc = _load("cost_efficiency.json")
    pts = doc["usd_per_1k"]
    x = [max(0.5, p["localized"]["reads_per_merge"]) for p in pts]
    fig, ax = plt.subplots(figsize=(3.3, 1.9))
    ax.plot(x, [p["localized"]["b2_usd"] for p in pts], marker="D", markersize=3, color=COLORS["b2"],
            label="B2 copy-on-write (128 MiB files)")
    ax.plot(x, [p["uniform"]["semlake_open_usd"] for p in pts], marker="^", markersize=3, linestyle="--",
            color=COLORS["b1r"], label="SemLake, uniform deletes")
    ax.plot(x, [p["localized"]["semlake_open_usd"] for p in pts], marker="o", markersize=3,
            color=COLORS["semlake"], label="SemLake, localized deletes")
    ax.set(xscale="log", yscale="log", xlabel="Full-table scans per merge", ylabel="USD per 1,000 merges")
    ax.legend(frameon=False)
    _save(fig, "fig_cost")


def fig_compaction() -> None:
    doc = _load("compaction.json")
    fig, ax = plt.subplots(figsize=(3.3, 1.9))
    layouts = ("localized", "uniform")
    colors = ["#D55E00", "#0072B2", "#009E73"]
    width = 0.27
    ticks = []
    for li, layout in enumerate(layouts):
        runs = sorted((r for r in doc["runs"] if r["layout"] == layout),
                      key=lambda r: -1.0 if r["threshold"] is None else -r["threshold"])
        for ri, run in enumerate(runs):
            vals = np.array([s["overhead"] * 100 for s in run["trace"]])
            sem = vals.std(ddof=1) / np.sqrt(len(vals))
            x = li + (ri - 1) * width
            thr = run["threshold"]
            ax.bar(x, vals.mean(), width, yerr=1.96 * sem, capsize=2, color=colors[ri])
            ticks.append((x, "none" if thr is None else f"{thr:.1%}"))
            rows = run["rows_rewritten_per_merge"]
            ax.text(x, vals.mean() + 1.96 * sem + 0.4, "0" if rows == 0 else f"{rows / 1e6:.2g}M",
                    ha="center", va="bottom", fontsize=5.5)
    ax.axhline(0, color="black", linewidth=0.5)
    ax.set_xticks([t[0] for t in ticks], [t[1] for t in ticks], fontsize=6)
    for li, layout in enumerate(layouts):
        ax.text(li, -0.25, f"{layout} deletes", transform=ax.get_xaxis_transform(), ha="center", va="top", fontsize=7)
    ax.set_ylabel("Mean Q6 overhead (%)")
    ax.set_ylim(-3, 16)
    _save(fig, "fig_compaction")


def fig_ablation() -> None:
    packing = _load("packing.json")
    swarm = _load("scion_sim_summary.json")
    fig, axes = plt.subplots(1, 2, figsize=(3.4, 1.9))
    pts = packing["points"]
    for name, marker in (("b1", "s"), ("b1r", "d"), ("semlake", "o")):
        axes[0].plot([p["rows_per_file"] for p in pts], [p[f"{name}_false_abort_rate"] * 100 for p in pts],
                     marker=marker, markersize=3, linewidth=1.2, color=COLORS[name], label=LABELS[name])
    axes[0].set(xscale="log", xlabel="Rows per file", ylabel="False aborts (%)", title="(a) W1 packing")
    axes[0].legend(frameon=False, fontsize=6)
    rows = [r for r in swarm["rows"] if r["mix"] == "agentic" and r["hot_rho"] == 0.0]
    agents = sorted({r["agents"] for r in rows})
    width = 0.38
    for i, name in enumerate(("semlake", "semlake_group")):
        vals = [next(r for r in rows if r["agents"] == a and r["system"] == name) for a in agents]
        axes[1].bar([j + (i - 0.5) * width for j in range(len(agents))], [v["commits_per_s"] for v in vals],
                    width, yerr=[v["commits_per_s_ci"] for v in vals], capsize=2,
                    color=COLORS[name], alpha=1.0 if i == 0 else 0.5,
                    label="one CAS per commit" if i == 0 else "group commit (G=8)")
    axes[1].set_xticks(range(len(agents)), [str(a) for a in agents])
    axes[1].set(xlabel="Agents", ylabel="Commits / s", title="(b) Commit path")
    axes[1].legend(frameon=False, fontsize=6, loc="upper left")
    axes[1].set_ylim(top=max(v["commits_per_s"] for v in rows) * 1.45)
    _save(fig, "fig_ablation")


def _swarm_series(metric: str):
    swarm = _load("scion_sim_summary.json")
    rows = [r for r in swarm["rows"] if r["mix"] == "agentic" and r["hot_rho"] == 0.0]
    agents = sorted({r["agents"] for r in rows})
    series = {}
    for name in ("b1", "b1r", "b2", "semlake", "semlake_group"):
        vals = [next((r for r in rows if r["agents"] == a and r["system"] == name), None) for a in agents]
        if all(vals):
            series[name] = ([v[metric] for v in vals], [v[f"{metric}_ci"] for v in vals])
    return agents, series


def fig_swarm() -> None:
    agents, series = _swarm_series("commits_per_s")
    fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.3))
    for name, (mean, ci) in series.items():
        axes[0].errorbar(agents, mean, yerr=ci, marker="o", markersize=3, linewidth=1.2, capsize=2,
                         linestyle="--" if name == "semlake_group" else "-", color=COLORS[name], label=LABELS[name])
    axes[0].set(xscale="log", xlabel="Concurrent agents", ylabel="Applied commits / s", title="(a) Throughput")
    axes[0].set_xticks(agents, [str(a) for a in agents])
    for metric, name, label in (("redo_false_per_commit", "b1", "B1 false redos"),
                                ("false_blocks_per_commit", "b1r", "B1R false blocks"),
                                ("false_blocks_per_commit", "semlake", "SemLake false blocks"),
                                ("false_blocks_per_commit", "semlake_group", "SemLake (G=8) false blocks")):
        _a, s = _swarm_series(metric)
        mean, ci = s[name]
        axes[1].errorbar(agents, mean, yerr=ci, marker="o", markersize=3, linewidth=1.2, capsize=2,
                         linestyle="--" if name == "semlake_group" else "-", color=COLORS[name], label=label)
    axes[1].set(xscale="log", xlabel="Concurrent agents", ylabel="Wasted attempts / commit",
                title="(b) Wasted agent work")
    axes[1].set_xticks(agents, [str(a) for a in agents])
    axes[1].set_yscale("symlog", linthresh=0.01)
    axes[1].set_ylim(bottom=0)
    from matplotlib.ticker import NullFormatter

    for ax in axes:
        ax.xaxis.set_minor_formatter(NullFormatter())
        ax.set_title("")
        ax.legend(frameon=False, fontsize=6, ncol=2, loc="lower center", bbox_to_anchor=(0.5, 1.0))
    axes[0].set_xlabel("Concurrent agents\n(a) Throughput")
    axes[1].set_xlabel("Concurrent agents\n(b) Wasted agent work")
    _save(fig, "fig_swarm")


def figures() -> None:
    _style()
    for fn in (fig_selectivity, fig_scalability, fig_requests, fig_cost, fig_compaction, fig_ablation, fig_swarm):
        try:
            fn()
            print(f"drew {fn.__name__}")
        except FileNotFoundError as exc:
            print(f"skipped {fn.__name__}: missing {exc.filename}")


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "figures"
    if which in ("packing", "all"):
        run_packing()
    if which in ("figures", "all"):
        figures()
