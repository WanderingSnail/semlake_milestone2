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

# One style for every figure: Libertinus Serif (the paper's Libertine face) at 7 pt,
# drawn at print size so that \includegraphics[width=\columnwidth] does not rescale text.
COL_W, FULL_W = 3.33, 7.0
SERIES = {
    "semlake": dict(color="#0072B2", marker="o", linestyle="-", label="SemLake"),
    "semlake_group": dict(color="#0072B2", marker="s", linestyle="--", label="SemLake, group commit (G=8)"),
    "semlake_static": dict(color="#0072B2", marker="^", linestyle=":", label="SemLake, static test only"),
    "b1": dict(color="#D55E00", marker="s", linestyle="-", label="B1 file OCC"),
    "b1r": dict(color="#56B4E9", marker="D", linestyle="-", label="B1R Delta row-level"),
    "b2": dict(color="#009E73", marker="v", linestyle="-", label="B2 copy-on-write"),
    "b3": dict(color="#CC79A7", marker="P", linestyle="-", label="B3 tuple versioning"),
    "b4": dict(color="#E69F00", marker="X", linestyle="-", label="B4 CRDT"),
    "b6": dict(color="#7F7F7F", marker="h", linestyle="-", label="B6 SSI"),
}
COLORS = {k: v["color"] for k, v in SERIES.items()}
LABELS = {k: v["label"] for k, v in SERIES.items()}
SHORT = {"b1": "B1", "b2": "B2", "b3": "B3", "b4": "B4", "semlake": "SemLake"}


def _style() -> None:
    from matplotlib import font_manager

    font_dir = Path("/usr/share/texlive/texmf-dist/fonts/opentype/public/libertinus-fonts")
    family = "serif"
    for font in sorted(font_dir.glob("LibertinusSerif-*.otf")):
        font_manager.fontManager.addfont(str(font))
        family = "Libertinus Serif"
    plt.rcParams.update({
        "font.family": family, "mathtext.fontset": "custom", "mathtext.rm": family,
        "mathtext.it": f"{family}:italic", "mathtext.bf": f"{family}:bold",
        "font.size": 7, "axes.labelsize": 7, "axes.titlesize": 7, "legend.fontsize": 6.5,
        "xtick.labelsize": 6.5, "ytick.labelsize": 6.5,
        "axes.linewidth": 0.5, "axes.edgecolor": "#444444", "axes.spines.top": False, "axes.spines.right": False,
        "xtick.major.width": 0.5, "ytick.major.width": 0.5, "xtick.minor.width": 0.4, "ytick.minor.width": 0.4,
        "xtick.major.size": 2.5, "ytick.major.size": 2.5, "xtick.minor.size": 1.5, "ytick.minor.size": 1.5,
        "axes.grid": True, "axes.grid.axis": "y", "grid.color": "#DDDDDD", "grid.linewidth": 0.4, "grid.linestyle": "-",
        "axes.axisbelow": True, "lines.linewidth": 1.1, "lines.markersize": 3.2, "errorbar.capsize": 1.5,
        "legend.frameon": False, "legend.handlelength": 2.0, "legend.columnspacing": 1.0,
        "legend.handletextpad": 0.4, "legend.borderaxespad": 0.2,
        "pdf.fonttype": 42, "ps.fonttype": 42, "savefig.dpi": 300,
    })


def _plot(ax, name: str, x, y, yerr=None, **kw):
    spec = {**SERIES[name], **kw}
    if yerr is None:
        return ax.plot(x, y, markeredgewidth=0, **spec)
    return ax.errorbar(x, y, yerr=yerr, elinewidth=0.6, markeredgewidth=0, **spec)


def _legend_above(ax_or_fig, handles=None, labels=None, ncol=3, anchor=None):
    kw = dict(loc="lower center", ncol=ncol)
    if handles is not None:
        kw.update(handles=handles, labels=labels)
    if anchor is not None:
        kw["bbox_to_anchor"] = anchor
    elif hasattr(ax_or_fig, "transAxes"):
        kw["bbox_to_anchor"] = (0.5, 1.01)
    return ax_or_fig.legend(**kw)


def _save(fig, name: str) -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    for ext in (".pdf", ".png"):
        fig.savefig(FIG_DIR / f"{name}{ext}", bbox_inches="tight", pad_inches=0.015)
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
    pts = _load("selectivity.json")["points"]
    x = [p["mean_fraction"] * 100 for p in pts]
    fig, ax = plt.subplots(figsize=(COL_W, 1.35), constrained_layout=True)
    for name in ("b1r", "semlake_static", "semlake"):
        _plot(ax, name, x, [p[f"{name}_false_block_rate"] for p in pts])
    ax.set_xscale("log")
    ax.set_xlabel("Rows deleted by the purge (% of lineitem)")
    ax.set_ylabel("False-block rate")
    ax.set_ylim(-0.04, 1.06)
    _legend_above(ax, ncol=3)
    _save(fig, "fig_selectivity")


def fig_scalability() -> None:
    rows = _csv("benchmark_run_summary.csv")
    fig, axes = plt.subplots(1, 3, figsize=(FULL_W, 1.45), constrained_layout=True)
    order = ("b1", "b2", "b3", "b4", "semlake")
    for name in order:
        pts = sorted((r for r in rows if r["workload"] == "W2L" and r["system"] == name),
                     key=lambda r: float(r["contention"]))
        _plot(axes[0], name, [float(r["contention"]) * 100 for r in pts], [float(r["merge_cpu_s"]) * 1e3 for r in pts])
        pts = sorted((r for r in rows if r["workload"] == "W4" and r["system"] == name),
                     key=lambda r: float(r["manifest_depth"]))
        _plot(axes[1], name, [float(r["manifest_depth"]) / 1e3 for r in pts],
              [float(r["merge_cpu_s"]) * 1e3 for r in pts], yerr=_err(pts))
    for name, label, extra in (("semlake", "three-way merge", {}),
                               ("lca", "LCA search", dict(color="#7F7F7F", marker="D", linestyle="--"))):
        pts = sorted((r for r in rows if r["workload"] == "H1" and r["system"] == name),
                     key=lambda r: float(r["manifest_depth"]))
        _plot(axes[2], "semlake", [float(r["manifest_depth"]) for r in pts], [float(r["merge_cpu_s"]) * 1e3 for r in pts],
              yerr=_err(pts) if name == "semlake" else None, label=label, **extra)
    axes[0].set(xlabel="Hot-row contention ρ (%)\n(a) W2 on 10K files", ylabel="Merge CPU time (ms)", yscale="log")
    axes[1].set(xlabel="Files in manifest (thousands)\n(b) W4, manifest size", ylabel="Merge CPU time (ms)", yscale="log")
    axes[2].set(xlabel="Snapshots in history\n(c) W4, history depth", ylabel="CPU time (ms)", yscale="log")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower left", ncol=5, bbox_to_anchor=(0.06, 1.0))
    axes[2].legend(loc="lower center", ncol=2, bbox_to_anchor=(0.5, 1.0))
    _save(fig, "fig_scalability")


def _err(pts: list[dict]):
    low = [float(r["merge_cpu_s"]) - float(r["merge_cpu_low_s"]) if r.get("merge_cpu_low_s") else 0 for r in pts]
    high = [float(r["merge_cpu_high_s"]) - float(r["merge_cpu_s"]) if r.get("merge_cpu_high_s") else 0 for r in pts]
    return [[v * 1e3 for v in low], [v * 1e3 for v in high]]


def _bar_labels(ax, bars, values, fmt):
    for bar, value in zip(bars, values):
        ax.annotate(fmt(value), (bar.get_x() + bar.get_width() / 2, bar.get_height()), xytext=(0, 1.5),
                    textcoords="offset points", ha="center", va="bottom", fontsize=5.8, color="#333333")


def fig_requests() -> None:
    rows = _csv("benchmark_run_summary.csv")
    depth = max(int(r["manifest_depth"]) for r in rows if r["workload"] == "W1S")
    by = {r["system"]: r for r in rows if r["workload"] == "W1S" and int(r["manifest_depth"]) == depth}
    order = ("b1", "b2", "b3", "b4", "semlake")
    fig, axes = plt.subplots(1, 2, figsize=(COL_W, 1.4), constrained_layout=True)
    reqs = [float(by[s]["s3_requests"]) for s in order]
    kb = [float(by[s]["put_bytes"]) / 1024 for s in order]
    for ax, vals, ylabel, fmt, title in (
            (axes[0], reqs, "Requests per merge", lambda v: f"{v:,.0f}", f"(a) Requests, {depth // 1000}K files"),
            (axes[1], kb, "KB written per merge", lambda v: f"{v:,.0f}" if v >= 10 else f"{v:.1f}", "(b) Bytes written")):
        bars = ax.bar([SHORT[s] for s in order], [max(v, 0.1) for v in vals], width=0.68,
                      color=[COLORS[s] for s in order], edgecolor="white", linewidth=0.6)
        _bar_labels(ax, bars, vals, fmt)
        ax.set_yscale("log")
        ax.set_ylim(top=max(vals) * 12)
        ax.set_ylabel(ylabel)
        ax.set_xlabel(title)
        ax.tick_params(axis="x", length=0, labelsize=6)
    _save(fig, "fig_requests")


def fig_cost() -> None:
    pts = _load("cost_efficiency.json")["usd_per_1k"]
    x = [max(0.5, p["localized"]["reads_per_merge"]) for p in pts]
    fig, ax = plt.subplots(figsize=(COL_W, 1.4), constrained_layout=True)
    _plot(ax, "b2", x, [p["localized"]["b2_usd"] for p in pts], label="B2 copy-on-write (128 MiB files)")
    _plot(ax, "semlake", x, [p["uniform"]["semlake_open_usd"] for p in pts], label="SemLake, uniform deletes",
          linestyle="--", marker="^")
    _plot(ax, "semlake", x, [p["localized"]["semlake_open_usd"] for p in pts], label="SemLake, localized deletes")
    ax.set(xscale="log", yscale="log", xlabel="Full-table scans per merge", ylabel="USD per 1,000 merges")
    ax.grid(True, axis="x", which="major")
    _legend_above(ax, ncol=2)
    _save(fig, "fig_cost")


def fig_compaction() -> None:
    doc = _load("compaction.json")
    fig, ax = plt.subplots(figsize=(COL_W, 1.4), constrained_layout=True)
    layouts = (("localized", SERIES["semlake"]["color"]), ("uniform", SERIES["b1"]["color"]))
    width = 0.26
    ticks = []
    for li, (layout, color) in enumerate(layouts):
        runs = sorted((r for r in doc["runs"] if r["layout"] == layout),
                      key=lambda r: -1.0 if r["threshold"] is None else -r["threshold"])
        for ri, run in enumerate(runs):
            vals = np.array([s["overhead"] * 100 for s in run["trace"]])
            half = 1.96 * vals.std(ddof=1) / np.sqrt(len(vals))
            x = li + (ri - 1) * (width + 0.04)
            thr = run["threshold"]
            ax.bar(x, vals.mean(), width, yerr=half, color=color, alpha=1.0 if thr is None else 0.55,
                   edgecolor="white", linewidth=0.6, error_kw=dict(elinewidth=0.6, capsize=1.5, ecolor="#333333"),
                   label=f"{layout} deletes" if thr is None else None)
            ticks.append((x, "none" if thr is None else f"{thr:.1%}"))
            rows = run["rows_rewritten_per_merge"]
            ax.annotate("0 rows" if rows == 0 else f"{rows / 1e6:.2g}M rows", (x, vals.mean() + half),
                        xytext=(0, 1.5), textcoords="offset points", ha="center", va="bottom", fontsize=5.5,
                        color="#333333")
    ax.axhline(0, color="#444444", linewidth=0.5)
    ax.set_xticks([t[0] for t in ticks], [t[1] for t in ticks])
    ax.tick_params(axis="x", length=0)
    ax.set_xlabel("Compaction threshold (pending deletes per row group)")
    ax.set_ylabel("Mean Q6 overhead (%)")
    ax.set_ylim(-3, 17)
    _legend_above(ax, ncol=2)
    _save(fig, "fig_compaction")


def fig_ablation() -> None:
    packing = _load("packing.json")
    swarm = _load("scion_sim_summary.json")
    fig, axes = plt.subplots(1, 2, figsize=(COL_W, 1.4), constrained_layout=True)
    pts = packing["points"]
    for name in ("b1", "b1r", "semlake"):
        _plot(axes[0], name, [p["rows_per_file"] for p in pts], [p[f"{name}_false_abort_rate"] * 100 for p in pts],
              label=SHORT.get(name, "B1R"), **(dict(linestyle="--", zorder=3) if name == "b1r" else {}))
    axes[0].set(xscale="log", xlabel="Rows per file\n(a) W1 packing", ylabel="False aborts (%)")
    axes[0].set_xticks([1, 16, 256], ["1", "16", "256"])
    axes[0].set_ylim(-4, 105)
    _legend_above(axes[0], ncol=3)
    rows = [r for r in swarm["rows"] if r["mix"] == "agentic" and r["hot_rho"] == 0.0]
    agents = sorted({r["agents"] for r in rows})
    width = 0.38
    for i, name in enumerate(("semlake", "semlake_group")):
        vals = [next(r for r in rows if r["agents"] == a and r["system"] == name) for a in agents]
        axes[1].bar([j + (i - 0.5) * width for j in range(len(agents))], [v["commits_per_s"] for v in vals],
                    width, yerr=[v["commits_per_s_ci"] for v in vals], color=COLORS[name],
                    alpha=1.0 if i else 0.45, edgecolor="white", linewidth=0.6,
                    error_kw=dict(elinewidth=0.6, capsize=1.5, ecolor="#333333"),
                    label="one CAS per commit" if i == 0 else "group commit (G=8)")
    axes[1].set_xticks(range(len(agents)), [str(a) for a in agents])
    axes[1].tick_params(axis="x", length=0)
    axes[1].set(xlabel="Concurrent agents\n(b) Commit path", ylabel="Commits / s")
    _legend_above(axes[1], ncol=1)
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
    from matplotlib.ticker import NullFormatter

    agents, series = _swarm_series("commits_per_s")
    fig, axes = plt.subplots(1, 2, figsize=(FULL_W, 1.45), constrained_layout=True)
    for name, (mean, ci) in series.items():
        _plot(axes[0], name, agents, mean, yerr=ci)
    axes[0].set(xscale="log", xlabel="Concurrent agents\n(a) Applied commits per second", ylabel="Commits / s")
    for metric, name, label in (("redo_false_per_commit", "b1", "B1 false redos"),
                                ("false_blocks_per_commit", "b1r", "B1R false blocks"),
                                ("false_blocks_per_commit", "semlake", "SemLake false blocks"),
                                ("false_blocks_per_commit", "semlake_group", "SemLake (G=8) false blocks")):
        _a, s = _swarm_series(metric)
        mean, ci = s[name]
        _plot(axes[1], name, agents, mean, yerr=ci, label=label)
    axes[1].set(xscale="log", xlabel="Concurrent agents\n(b) Wasted attempts per applied commit",
                ylabel="Wasted attempts / commit")
    axes[1].set_yscale("symlog", linthresh=0.01)
    axes[1].set_ylim(bottom=0)
    for ax in axes:
        ax.set_xticks(agents, [str(a) for a in agents])
        ax.xaxis.set_minor_formatter(NullFormatter())
        _legend_above(ax, ncol=3 if ax is axes[0] else 2)
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
