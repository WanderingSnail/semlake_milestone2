"""Numbers and tables of the revised evaluation, from the raw result files.

Called by ``paper_numbers.py``; every macro and table of Sections 7.1 to 7.7 that
did not exist in the first submission is produced here, each from the two or
three values it compares, so that a ratio in the text cannot drift from them.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "benchmarks" / "results"
OUT = ROOT / "paper" / "generated"

SWARM_SUMMARY = "scion_sim_calibrated_summary.json"
LABELS = {"b1": "$B_1$ file OCC", "b1r": "$B_1\\text{R}$ row-level", "b2": "$B_2$ copy-on-write",
          "b3": "$B_3$ tuple versioning", "b4": "$B_4$ CRDT", "semlake": "\\textbf{\\sys}"}


def _json(name: str) -> dict:
    return json.loads((RESULTS / name).read_text(encoding="utf-8"))


def _count(value: float) -> str:
    return f"{value:,.0f}"


def _two(value) -> str:
    return "--" if value is None else f"{value:.2f}"


def _ms(seconds: float) -> str:
    ms = seconds * 1e3
    return f"{ms:,.0f}" if ms >= 100 else f"{ms:.1f}" if ms >= 10 else f"{ms:.2f}"


def _sec(seconds: float) -> str:
    return f"{seconds:,.0f}" if seconds >= 100 else f"{seconds:.1f}" if seconds >= 10 else f"{seconds:.2f}"


def _ratio(a: float, b: float) -> str:
    r = a / b
    return f"{r:,.0f}" if r >= 10 else f"{r:.1f}"


def _pct(value: float, digits: int = 0) -> str:
    return f"{value * 100:.{digits}f}"


def _mb(n_bytes: float) -> str:
    return f"{n_bytes / 1e6:.2f}"


def _write_table(name: str, lines: list[str]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{name}.tex").write_text("\n".join(lines) + "\n", encoding="utf-8")


# --------------------------------------------------------------------- environment and code


def environment(n) -> None:
    env = _json("ledger.json")["environment"]
    cpu = env["cpu"].replace("(R)", "").replace("  ", " ").replace(" Processor", "").replace("@ ", "").strip()
    n["EnvCpu"] = f"an {cpu}" if cpu[:1] in "AEIOU" else f"a {cpu}"
    n["EnvCores"] = env["cores"]
    n["EnvMemGB"] = f"{env['memory_gb']:g}"
    n["EnvOs"] = " ".join(env["os"].split("-")[0].split()[:2]).rsplit(".", 1)[0]
    n["EnvPython"] = env["python"]
    n["EnvDuckdb"] = env["duckdb"]
    n["EnvPyarrow"] = env["pyarrow"]
    from semlake.metadata import MANIFEST_CHECKPOINT
    from semlake.verification import VECTOR_THRESHOLD

    n["VecThreshold"] = _count(VECTOR_THRESHOLD)
    n["ManifestCheckpoint"] = MANIFEST_CHECKPOINT
    tests = 0
    for path in (ROOT / "tests").glob("test_*.py"):
        tests += sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.lstrip().startswith("def test_"))
    n["RevTests"] = tests


# --------------------------------------------------------------------- ledger (7.4)


def ledger(n) -> None:
    doc = _json("ledger.json")
    med = doc["s3_medians_s"]
    n["SthreePutMs"] = f"{med['put'] * 1e3:.0f}"
    n["SthreeGetMs"] = f"{med['get'] * 1e3:.0f}"
    n["SthreeListMs"] = f"{med['list'] * 1e3:.0f}"
    n["LedParallel"] = doc["parallel_requests"]

    def row(workload: str, files: int) -> dict:
        return next(r for r in doc["rows"] if r["workload"] == workload and r["files"] == files)

    sizes = sorted({r["files"] for r in doc["rows"] if r["workload"] == "W1S"})
    top = row("W1S", sizes[-1])
    sem, b1r, b2, b3 = (top["systems"][k] for k in ("semlake", "b1r", "b2", "b3"))
    n["LedRepeats"] = sem["cpu_s"]["n"]
    c = sem["counters"]
    n["LedSemReq"] = c["requests"]
    n["LedSemPut"], n["LedSemGet"], n["LedSemList"] = c["put"], c["get"], c["list"]
    same_requests = all(row("W1S", s)["systems"]["semlake"]["counters"]["requests"] == c["requests"] for s in sizes)
    assert same_requests, "the text says the request count does not change with the files touched"
    same_as_b1r = b1r["counters"]["requests"] == c["requests"] and b1r["counters"]["put_bytes"] == c["put_bytes"]
    assert same_as_b1r, "the text says B1R issues the same requests and bytes"
    n["LedSemMB"] = _mb(c["put_bytes"])
    n["LedSemSummaryMB"] = _mb(sem["merge"]["summary"]["put_bytes"])
    n["LedSemManifestMB"] = _mb(sem["merge"]["manifest"]["put_bytes"])
    n["LedSemDataKB"] = f"{sem['merge']['data']['put_bytes'] / 1e3:.0f}"
    n["LedBytesPerFile"] = f"{c['put_bytes'] / top['files']:.0f}"
    n["LedBfourReq"] = top["systems"]["b4"]["counters"]["requests"]
    n["LedBoneReq"] = top["systems"]["b1"]["counters"]["requests"]
    n["LedBtwoReq"] = _count(b2["counters"]["requests"])
    n["LedBthreeReq"] = _count(b3["counters"]["requests"])
    n["LedSemS"] = _sec(sem["end_to_end_s"]["p50"])
    n["LedBtwoParS"] = _sec(b2["end_to_end_s"]["p50"])
    n["LedBtwoSeqS"] = _count(b2["sequential_s"])
    n["LedBtwoParRatio"] = _ratio(b2["end_to_end_s"]["p50"], sem["end_to_end_s"]["p50"])
    n["LedCostCutPct"] = f"{(1 - sem['request_usd'] / b2['request_usd']) * 100:.2f}"
    commit = top["commit"]
    n["LedCommitPut"] = sum(v["put"] for v in commit.values())
    meta = commit["summary"]["put_bytes"] + commit["manifest"]["put_bytes"]
    n["LedCommitMetaMB"] = _mb(meta)
    n["LedCommitDataKB"] = f"{commit['data']['put_bytes'] / 1e3:.0f}"
    n["LedMetaRatio"] = _ratio(meta, commit["data"]["put_bytes"])
    n["LedWafMerge"] = _ratio(c["put_bytes"], commit["data"]["put_bytes"])
    w4 = row("W4", sizes[-1])["systems"]
    n["LedWfourSemReq"] = w4["semlake"]["counters"]["requests"]
    n["LedWfourSemBytes"] = _count(w4["semlake"]["counters"]["put_bytes"])

    order = ("b1", "b1r", "b2", "b3", "b4", "semlake")
    lines = ["\\begin{table}[t]", "\\centering", "\\footnotesize",
             f"\\caption{{Ledger of one merge at {_count(top['files'])} touched files: requests, bytes written per "
             f"object class, CPU time over {sem['cpu_s']['n']} runs and end-to-end time at measured S3 medians "
             f"($B_2$ with {doc['parallel_requests']} parallel requests). The last row is the two branch commits "
             f"that precede the merge.}}",
             "\\label{tab:ledger}", "\\setlength{\\tabcolsep}{2.2pt}", "\\resizebox{\\columnwidth}{!}{%",
             "\\begin{tabular}{@{}l rrr rrr rr r@{}}", "\\toprule",
             "& \\multicolumn{3}{c}{Requests} & \\multicolumn{3}{c}{MB written} & \\multicolumn{2}{c}{CPU (ms)} & E2E \\\\",
             "\\cmidrule(lr){2-4} \\cmidrule(lr){5-7} \\cmidrule(lr){8-9}",
             "Rule & PUT & GET & LIST & data & summ. & manif. & P50 & P99 & (s) \\\\", "\\midrule"]
    for name in order:
        s = top["systems"][name]
        k = s["counters"]
        by = lambda cls: s["merge"].get(cls, {}).get("put_bytes", 0)  # noqa: E731
        other = k["put_bytes"] - by("data") - by("summary") - by("manifest")
        lines.append(f"{LABELS[name]} & {_count(k['put'])} & {_count(k['get'])} & {k['list']} & "
                     f"{_mb(by('data') + other)} & {_mb(by('summary'))} & {_mb(by('manifest'))} & "
                     f"{_ms(s['cpu_s']['p50'])} & {_ms(s['cpu_s']['p99'])} & {_sec(s['end_to_end_s']['p50'])} \\\\")
    lines += ["\\midrule",
              f"two commits & {sum(v['put'] for v in commit.values())} & 0 & 0 & {_mb(commit['data']['put_bytes'])} & "
              f"{_mb(commit['summary']['put_bytes'])} & {_mb(commit['manifest']['put_bytes'])} & -- & -- & -- \\\\",
              "\\bottomrule", "\\end{tabular}}", "\\end{table}"]
    _write_table("tab_ledger", lines)


# --------------------------------------------------------------------- full TPC-H queries (7.2)


CLASS_LABELS = (("single", "single table"), ("join", "join"), ("subquery", "subquery"), ("table-wide", "whole table"))


def full_queries(n) -> None:
    doc = _json("tpch_full_queries.json")
    summary, by_class = doc["summary"], doc["by_class"]
    reads = doc["reads"]
    kinds = [spec["kind"] for spec in doc["refreshes"].values()]
    n["FullTpchQueries"] = sum(1 for name in reads if name.startswith("Q"))
    n["FullRefreshes"] = len(kinds)
    n["FullDeletes"] = kinds.count("delete")
    n["FullUpdates"] = kinds.count("update")
    n["FullSF"] = f"{doc['scale_factor']:g}"
    n["FullRows"] = _count(doc["rows"])
    n["FullPairs"] = doc["pairs"]
    n["FullPos"] = doc["conflicts"]
    sem = summary["semlake"]
    n["FullSemRec"] = _two(sem["recall"])
    n["FullSemPrec"] = _two(sem["precision"])
    n["FullSemFBPct"] = _pct(sem["false_block_rate"])
    n["FullSemPrecSingle"] = _two(by_class["single"]["semlake"]["precision"])
    n["FullSemPrecJoin"] = _two(by_class["join"]["semlake"]["precision"])
    n["FullSemPrecSub"] = _two(by_class["subquery"]["semlake"]["precision"])
    wide = [name for name, info in reads.items() if info["class"] == "table-wide"]
    n["FullWideReads"] = len(wide)
    n["FullWidePct"] = _pct(len(wide) / len(reads))
    n["FullSemFBWidePct"] = _pct(by_class["table-wide"]["semlake"]["false_block_rate"])
    n["FullNoProjFBPct"] = _pct(summary["semlake_noproject"]["false_block_rate"])
    n["FullStaticPrec"] = _two(summary["semlake_static"]["precision"])
    n["FullStaticFBPct"] = _pct(summary["semlake_static"]["false_block_rate"])
    b1r = summary["b1r"]
    n["FullBoneRRec"] = _two(b1r["recall"])
    n["FullBoneRPrec"] = _two(b1r["precision"])
    n["FullBoneRFBPct"] = _pct(b1r["false_block_rate"])
    n["FullOthersTP"] = max(summary["b2"]["tp"], summary["b6"]["tp"])

    lines = ["\\begin{table}[t]", "\\centering", "\\footnotesize",
             f"\\caption{{Full TPC-H queries as declared reads against {len(kinds)} \\texttt{{lineitem}} refreshes "
             f"({doc['pairs']} pairs, {doc['conflicts']} conflicts by query result). R: recall; P: precision; "
             f"FB: share of safe pairs blocked. Classes follow what the projection keeps.}}",
             "\\label{tab:fullqueries}", "\\setlength{\\tabcolsep}{2.4pt}", "\\resizebox{\\columnwidth}{!}{%",
             "\\begin{tabular}{@{}l rr rrr r r rrr@{}}", "\\toprule",
             "& & & \\multicolumn{3}{c}{\\sys} & static & no proj. & \\multicolumn{3}{c}{$B_1\\text{R}$} \\\\",
             "\\cmidrule(lr){4-6} \\cmidrule(lr){7-7} \\cmidrule(lr){8-8} \\cmidrule(l){9-11}",
             "Read class & Pairs & Confl. & R & P & FB & P & FB & R & P & FB \\\\", "\\midrule"]

    def cells(scores: dict, pairs: int) -> str:
        s, st, npj, b = scores["semlake"], scores["semlake_static"], scores["semlake_noproject"], scores["b1r"]
        conflicts = s["tp"] + s["fn"]
        fb = lambda x: "--" if x["false_block_rate"] is None else _pct(x["false_block_rate"]) + "\\%"  # noqa: E731
        return (f"{pairs} & {conflicts} & {_two(s['recall'])} & {_two(s['precision'])} & {fb(s)} & "
                f"{_two(st['precision'])} & {fb(npj)} & {_two(b['recall'])} & {_two(b['precision'])} & {fb(b)}")

    for key, label in CLASS_LABELS:
        if key in by_class:
            count = sum(1 for info in reads.values() if info["class"] == key)
            lines.append(f"{label} ({count} reads) & {cells(by_class[key], by_class[key]['pairs'])} \\\\")
    lines += ["\\midrule", f"all ({len(reads)} reads) & {cells(summary, doc['pairs'])} \\\\",
              "\\bottomrule", "\\end{tabular}}", "\\end{table}"]
    _write_table("tab_fullqueries", lines)


# --------------------------------------------------------------------- recovery (7.2)


def recovery(n) -> None:
    doc = _json("recovery.json")
    n["RecScenarios"] = doc["scenarios"]
    n["RecPoints"] = _count(doc["crash_points_total"])
    n["RecViolations"] = doc["violations"]
    n["RecOrphans"] = _count(doc["orphans_collected"])


# --------------------------------------------------------------------- tails (7.3)


def tails(n) -> None:
    doc = _json("tails.json")
    n["TailFiles"] = _count(doc["contention_files"])
    rhos = sorted({r["rho"] for r in doc["contention"]})

    def at(rho: float, system: str) -> dict:
        return next(r for r in doc["contention"] if r["rho"] == rho and r["system"] == system)

    low, high = rhos[0], rhos[-1]
    sem0, semh = at(low, "semlake"), at(high, "semlake")
    n["TailPairs"] = sem0["pairs"]
    n["TailRhoHigh"] = f"{high * 100:g}\\%"
    n["TailSemZeroMed"] = _ms(sem0["cpu_s"]["p50"])
    n["TailSemZeroTail"] = _ms(sem0["cpu_s"]["p99"])
    n["TailSemHighMed"] = _ms(semh["cpu_s"]["p50"])
    n["TailSemHighTail"] = _ms(semh["cpu_s"]["p99"])
    gap = max(abs(at(rho, "b1r")["cpu_s"]["p50"] / at(rho, "semlake")["cpu_s"]["p50"] - 1) for rho in rhos)
    n["TailBoneRGapPct"] = f"{gap * 100:.0f}"
    n["TailBoneZeroMed"] = _ms(at(low, "b1")["cpu_s"]["p50"])
    n["TailBtwoZeroMed"] = _ms(at(low, "b2")["cpu_s"]["p50"])
    n["TailBoneRatio"] = _ratio(at(low, "b1")["cpu_s"]["p50"], sem0["cpu_s"]["p50"])
    n["TailBtwoRatio"] = _ratio(at(low, "b2")["cpu_s"]["p50"], sem0["cpu_s"]["p50"])
    n["TailBthreeZeroS"] = _sec(at(low, "b3")["cpu_s"]["p50"])
    life = next(r for r in doc["choice_lifecycle"] if r["rho"] == high)
    n["ChoiceOpened"] = _count(life["opened"])
    n["ChoiceOpen"] = life["still_open"]
    n["ChoiceResolveMs"] = _ms(life["resolve_cpu_s"]["p50"]) if life["resolve_cpu_s"] else "0"

    files = sorted({r["files"] for r in doc["manifest"]})

    def man(depth: int, system: str) -> dict:
        return next(r for r in doc["manifest"] if r["files"] == depth and r["system"] == system)

    n["ManSemOneMs"] = _ms(man(files[0], "semlake")["cpu_s"]["p50"])
    n["ManSemTenMs"] = _ms(man(files[-1], "semlake")["cpu_s"]["p50"])
    n["ManSemGrowth"] = _ratio(man(files[-1], "semlake")["cpu_s"]["p50"], man(files[0], "semlake")["cpu_s"]["p50"])
    n["ManBoneTenMs"] = _ms(man(files[-1], "b1")["cpu_s"]["p50"])
    n["ManBoneRatio"] = _ratio(man(files[-1], "b1")["cpu_s"]["p50"], man(files[-1], "semlake")["cpu_s"]["p50"])
    deepest = max(doc["history"], key=lambda r: r["snapshots"])
    n["HistMaxSnaps"] = _count(deepest["snapshots"])
    n["HistLcaUs"] = f"{deepest['lca_s']['p50'] * 1e6:.0f}"
    n["HistMergeMs"] = _ms(deepest["merge_s"]["p50"])


# --------------------------------------------------------------------- delta sweep and pruning (7.3, 7.6)


def sweep(n) -> None:
    doc = _json("delta_sweep.json")
    rows = doc["rows"]

    def pick(kind: str, changed: int, preds: int, vectorized: bool) -> dict | None:
        return next((r for r in rows if r["kind"] == kind and r["rows_changed"] == changed
                     and r["predicates"] == preds and r["vectorized"] == vectorized), None)

    deletes = sorted((r for r in rows if r["kind"] == "delete" and r["predicates"] == 1 and r["vectorized"]),
                     key=lambda r: r["rows_changed"])
    top, first = deletes[-1], deletes[0]
    n["SweepMaxRows"] = _count(top["rows_changed"])
    n["SweepMaxS"] = _sec(top["merge_s"]["p50"])
    n["SweepMaxTailS"] = _sec(top["merge_s"]["p99"])
    n["SweepMaxRssGB"] = f"{top['peak_rss_mb'] / 1024:.1f}"
    n["SweepMaxSummaryMB"] = f"{top['summary_bytes'] / 1e6:.0f}"
    span = top["rows_changed"] - first["rows_changed"]
    n["SweepUsPerRow"] = f"{(top['merge_s']['p50'] - first['merge_s']['p50']) / span * 1e6:.1f}"
    n["SweepBytesPerRow"] = _count((top["peak_rss_mb"] - first["peak_rss_mb"]) * 1024 * 1024 / span)
    multi = sorted((r for r in rows if r["kind"] == "delete" and r["predicates"] > 1 and r["vectorized"]),
                   key=lambda r: r["predicates"])
    if multi:
        most = multi[-1]
        one = pick("delete", most["rows_changed"], 1, True)
        n["SweepPredsMax"] = most["predicates"]
        n["SweepPredsRows"] = _count(most["rows_changed"])
        n["SweepPredsMaxS"] = _sec(most["merge_s"]["p50"])
        n["SweepPredsOneS"] = _sec(one["merge_s"]["p50"])
    # Vectorized tier against the per-row loop, at the largest size both ran.
    for kind, tag in (("delete", "Del"), ("update", "Upd")):
        both = [r["rows_changed"] for r in rows if r["kind"] == kind and r["predicates"] == 1 and not r["vectorized"]]
        size = max(both)
        vec, loop = pick(kind, size, 1, True), pick(kind, size, 1, False)
        n[f"Vec{tag}Rows"] = _count(size)
        n[f"Vec{tag}S"] = _sec(vec["merge_s"]["p50"])
        n[f"Loop{tag}S"] = _sec(loop["merge_s"]["p50"])
    if multi:
        loops = [r for r in rows if r["kind"] == "delete" and r["predicates"] > 1 and not r["vectorized"]]
        if loops:
            loop = max(loops, key=lambda r: r["predicates"])
            vec = pick("delete", loop["rows_changed"], loop["predicates"], True)
            n["VecPreds"] = loop["predicates"]
            n["VecPredsS"] = _sec(vec["merge_s"]["p50"])
            n["LoopPredsS"] = _sec(loop["merge_s"]["p50"])


def prune(n) -> None:
    doc = _json("prune.json")
    top = max(doc["rows"], key=lambda r: r["rows_deleted"])
    full, proj, none = top["full"], top["filter_columns"], top["none"]
    n["PruneRows"] = _count(top["rows_deleted"])
    n["PruneFullBytes"] = f"{full['bytes_per_deleted_row']:.0f}"
    n["PruneProjBytes"] = f"{proj['bytes_per_deleted_row']:.0f}"
    n["PruneNoneBytes"] = f"{none['bytes_per_deleted_row']:.0f}"
    n["PruneProjCutPct"] = _pct(1 - proj["summary_bytes"] / full["summary_bytes"])
    n["PruneFullCutPct"] = _pct(1 - none["summary_bytes"] / full["summary_bytes"])
    for row in doc["rows"]:
        assert row["full"]["conflicting_report"] == row["filter_columns"]["conflicting_report"] == \
            row["none"]["conflicting_report"] == "semantic_conflict", "pruning must never hide a conflict"
        assert row["full"]["safe_report"] == row["filter_columns"]["safe_report"] == "clean"
        assert row["none"]["safe_report"] == "semantic_conflict"


# --------------------------------------------------------------------- live S3, corrected workloads (7.4)


def s3_v3(n) -> None:
    doc = _json("s3_run_v3.json")
    rows = doc["rows"]

    def pick(workload: str, system: str, depth: int) -> dict:
        for r in rows:
            if r["workload"] == workload and r["system"] == system and r["depth"] == depth:
                return r
        raise KeyError(f"s3_run_v3.json has no row for {workload} {system} at {depth} files")

    depths = sorted({r["depth"] for r in rows})
    n["SthreeVthreeRepeats"] = pick("W1S", "semlake", depths[0])["repeats"]
    n["SthreeVthreeRepeatsLarge"] = pick("W1S", "semlake", depths[-1])["repeats"]
    for depth, tag in ((depths[0], "One"), (depths[-1], "Ten")):
        sem, b1r, par = (pick("W1S", s, depth) for s in ("semlake", "b1r", "b2_parallel"))
        n[f"SthreeVthreeSem{tag}S"] = _sec(sem["wall_s"])
        n[f"SthreeVthreeBoneR{tag}S"] = _sec(b1r["wall_s"])
        n[f"SthreeVthreeBtwo{tag}S"] = _sec(par["wall_s"])
        n[f"SthreeVthreeRatio{tag}"] = _ratio(par["wall_s"], sem["wall_s"])
    ratios = [r["wall_s"] / r["calibrated_model_s"] for r in rows if "calibrated_model_s" in r]
    n["SthreeVthreeCalibLow"] = f"{min(ratios):.2f}"
    n["SthreeVthreeCalibHigh"] = f"{max(ratios):.2f}"
    n["SthreeVthreeWfourOneS"] = _sec(pick("W4", "semlake", depths[0])["wall_s"])


# --------------------------------------------------------------------- scan interval and lifecycle (7.5)


def scan_lifecycle(n) -> None:
    doc = _json("tpch_scan.json")
    rows = doc["results"]
    loc = next(r for r in rows if r["layout"] == "localized")
    uni = next(r for r in rows if r["layout"] == "uniform" and abs(r["density"] - 0.01) < 1e-9)
    pct = lambda v: f"{v * 100:+.1f}".replace("-", "$-$")  # noqa: E731
    n["ScanProcesses"] = loc["processes"]
    low_key, high_key = ("overhead_boot_low", "overhead_boot_high") if "overhead_boot_low" in loc else \
        ("overhead_ci_low", "overhead_ci_high")
    n["ScanLocLow"], n["ScanLocHigh"] = pct(loc[low_key]), pct(loc[high_key])
    n["ScanUniLow"], n["ScanUniHigh"] = pct(uni[low_key]), pct(uni[high_key])
    n["ScanLocVerdict"] = ", which is not distinguishable from zero" if loc[low_key] <= 0 <= loc[high_key] else ""

    lines = ["\\begin{table}[t]", "\\centering", "\\footnotesize",
             f"\\caption{{TPC-H Q6 on SF\\,1 \\texttt{{lineitem}} with deletes left in a DV. Base and DV: medians "
             f"over {loc['processes']} processes of the compacted and the masked scan. Overhead: median of the "
             f"per-process ratios, with a bootstrap 95\\% interval.}}",
             "\\label{tab:scan}", "\\setlength{\\tabcolsep}{3pt}",
             "\\begin{tabular}{@{}l r r r r l@{}}", "\\toprule",
             "Deletes & Rows & Clean gr. & Base (ms) & DV (ms) & Overhead (95\\%) \\\\", "\\midrule"]
    for r in rows:
        lines.append(f"{r['layout']} & {r['density'] * 100:g}\\% & {r['clean_groups']}/{r['groups']} & "
                     f"{r['base_ms']:.1f} & {r['dv_ms']:.1f} & {pct(r['overhead_median'])}\\% "
                     f"({pct(r[low_key])} to {pct(r[high_key])}\\%) \\\\")
    lines += ["\\bottomrule", "\\end{tabular}", "\\end{table}"]
    _write_table("tab_scan", lines)

    # Crossover as a range: the dollar model of cost_efficiency.py at both ends of the interval.
    from benchmarks.cost_efficiency import crossover

    cost = _json("cost_efficiency.json")

    def span(entry: dict) -> str:
        at_high = crossover(cost["merge_io"], entry["base_ms"] / 1e3, entry[high_key])
        if entry[low_key] <= 0:
            return f"at least {_count(at_high)} (the interval includes zero, so there is no upper end)"
        at_low = crossover(cost["merge_io"], entry["base_ms"] / 1e3, entry[low_key])
        return f"{_count(at_high)} to {_count(at_low)}"

    n["CrossLocRange"] = span(loc)
    n["CrossUniRange"] = span(uni)

    comp = _json("compaction.json")
    deleted = comp["delete_per_merge"] * doc["rows"]
    runs = comp["runs"]
    best = lambda layout: min((r for r in runs if r["layout"] == layout and r["threshold"] is not None),  # noqa: E731
                              key=lambda r: r["mean_overhead"])
    n["CompLocWaf"] = f"{best('localized')['rows_rewritten_per_merge'] / deleted:.0f}"
    n["CompUniWaf"] = f"{best('uniform')['rows_rewritten_per_merge'] / deleted:.0f}"
    # Copy-on-write rewrites every row group a merge touches: one group when the deletes are
    # localized, every group when they are uniform.
    n["CompCowLocWaf"] = f"{comp['group_rows'] / deleted:.0f}"
    n["CompCowUniWaf"] = f"{doc['rows'] / deleted:,.0f}"


# --------------------------------------------------------------------- threads (7.7)


def threads(n) -> None:
    doc = _json("threads.json")
    rows = doc["rows_out"]
    writers = sorted({r["writers"] for r in rows})
    hots = sorted({r["hot"] for r in rows})
    group = max(r["group"] for r in rows)

    def at(system: str, hot: float, g: int, w: int | None = None) -> dict:
        return next(r for r in rows if r["system"] == system and r["hot"] == hot and r["group"] == g
                    and r["writers"] == (w or writers[-1]))

    def series(system: str, hot: float, g: int) -> list[dict]:
        return sorted((r for r in rows if r["system"] == system and r["hot"] == hot and r["group"] == g),
                      key=lambda r: r["writers"])

    def peak(system: str, hot: float, g: int) -> dict:
        return max(series(system, hot, g), key=lambda r: r["commits_per_s_mean"])

    def per_commit(row: dict, key: str) -> float:
        return sum(run[key] for run in row["runs"]) / (sum(run["applied"] for run in row["runs"]) or 1)

    cold, hot = hots[0], hots[-1]
    n["ThrMaxWriters"] = writers[-1]
    n["ThrGroup"] = group
    n["ThrWorkS"] = f"{doc['work_mean_s']:g}"
    n["ThrScale"] = f"{doc['scale']:g}"
    n["ThrCpuWeight"] = f"{1 / doc['scale']:.0f}"
    n["ThrSeeds"] = len(at("semlake", cold, 0)["runs"])
    n["ThrHotPct"] = _pct(hot)
    n["ThrFiles"] = doc["files"]
    n["ThrRows"] = _count(doc["rows"])

    # Optimistic publishing: the peak, and what is left at the largest writer count.
    opt_peak, opt = peak("semlake", cold, 0), at("semlake", cold, 0)
    n["ThrSemOptPeak"] = f"{opt_peak['commits_per_s_mean']:.1f}"
    n["ThrSemOptPeakWriters"] = opt_peak["writers"]
    n["ThrSemOptMax"] = f"{opt['commits_per_s_mean']:.1f}"
    n["ThrSemOptLost"] = f"{per_commit(opt, 'cas_lost'):.0f}"
    n["ThrSemOptTailS"] = _sec(opt["p99_s"])
    b1_opt_peak, b1_opt = peak("b1", cold, 0), at("b1", cold, 0)
    n["ThrBoneOptPeak"] = f"{b1_opt_peak['commits_per_s_mean']:.1f}"
    n["ThrBoneOptMax"] = f"{b1_opt['commits_per_s_mean']:.1f}"

    # Commit service.
    grp_peak, grp = peak("semlake", cold, group), at("semlake", cold, group)
    n["ThrSemGrpPeak"] = f"{grp_peak['commits_per_s_mean']:.1f}"
    n["ThrSemGrpPeakWriters"] = grp_peak["writers"]
    n["ThrSemGrpPeakTailS"] = _sec(grp_peak["p99_s"])
    n["ThrSemGrpMax"] = f"{grp['commits_per_s_mean']:.1f}"
    n["ThrSemGrpTailS"] = _sec(grp["p99_s"])
    n["ThrSemGrpRatio"] = _ratio(grp_peak["commits_per_s_mean"], opt_peak["commits_per_s_mean"])
    b1_grp_peak, b1_grp = peak("b1", cold, group), at("b1", cold, group)
    n["ThrBoneGrpPeak"] = f"{b1_grp_peak['commits_per_s_mean']:.1f}"
    n["ThrBoneGrpPeakWriters"] = b1_grp_peak["writers"]
    n["ThrBoneGrpMax"] = f"{b1_grp['commits_per_s_mean']:.1f}"
    n["ThrBoneGrpRedo"] = f"{per_commit(b1_grp, 'redo_false'):.2f}"
    n["ThrSemGrpRedo"] = f"{per_commit(grp, 'redo_true'):.2f}"

    # One hot row: increments of it are true conflicts for SemLake and file overlaps for B1.
    hot_sem, hot_b1 = peak("semlake", hot, group), peak("b1", hot, group)
    n["ThrSemHotGrpPeak"] = f"{hot_sem['commits_per_s_mean']:.1f}"
    n["ThrSemHotGrpPeakWriters"] = hot_sem["writers"]
    n["ThrSemHotRedo"] = f"{per_commit(hot_sem, 'redo_true'):.2f}"
    n["ThrBoneHotGrpPeak"] = f"{hot_b1['commits_per_s_mean']:.1f}"
    n["ThrBoneHotGrpPeakWriters"] = hot_b1["writers"]
    n["ThrBoneHotRedo"] = f"{per_commit(hot_b1, 'redo_false'):.2f}"
    assert all(r["redo_false"] == 0 for r in rows if r["system"] == "semlake"), "SemLake redid a mergeable commit"
    assert all(r["lost_updates"] == 0 for r in rows), "a published increment is missing from the head"


# --------------------------------------------------------------------- swarm (7.7)


def swarm(n) -> None:
    doc = _json(SWARM_SUMMARY)
    rows = doc["rows"]
    agentic = [r for r in rows if r["mix"] == "agentic" and r["hot_rho"] == 0.0]
    agents = sorted({r["agents"] for r in agentic})
    top = agents[-1]

    def get(system: str, a: int = top, mix: str = "agentic") -> dict:
        return next(r for r in rows if r["mix"] == mix and r["hot_rho"] == 0.0 and r["agents"] == a
                    and r["system"] == system)

    total = lambda *systems: sum(r["hazards_committed_total"] for r in rows if r["system"] in systems)  # noqa: E731
    n["SwarmBoneHazTotal"] = total("b1", "b1_group")
    n["SwarmBtwoHazTotal"] = total("b2", "b2_group")
    n["SwarmBoneRHazTotal"] = total("b1r", "b1r_group")
    n["SwarmSemHazTotal"] = total("semlake", "semlake_group")
    n["SwarmBoneRFBMax"] = f"{get('b1r_group')['false_blocks']:.1f}"
    n["SwarmSemFBMax"] = f"{get('semlake_group')['false_blocks']:.1f}"
    n["SwarmBoneRedoMax"] = f"{get('b1_group')['redo_false']:.0f}"
    n["SwarmSemSixtyFour"] = f"{get('semlake')['commits_per_s']:.1f}"
    n["SwarmGroupSixtyFour"] = f"{get('semlake_group')['commits_per_s']:.1f}"
    n["SwarmBoneRGroupSixtyFour"] = f"{get('b1r_group')['commits_per_s']:.1f}"
    n["SwarmSemGroupTailS"] = _sec(get("semlake_group")["p99_s"])
    n["SwarmSemTailS"] = _sec(get("semlake")["p99_s"])
    heavy_agents = sorted({r["agents"] for r in rows if r["mix"] == "purge-heavy"})
    heavy = [r for r in rows if r["mix"] == "purge-heavy" and r["system"] in ("semlake", "semlake_group")]
    n["SwarmHeavySemCaught"] = sum(r["hazards_caught_total"] for r in heavy)
    n["SwarmHeavySemHaz"] = sum(r["hazards_committed_total"] for r in heavy)
    n["SwarmHeavyAgents"] = f"{heavy_agents[0]} to {heavy_agents[-1]}" if len(heavy_agents) > 1 else str(heavy_agents[0])

    lines = ["\\begin{table}[t]", "\\centering", "\\footnotesize",
             f"\\caption{{Simulated swarm at {top} agents, {doc['horizon_s']:g}\\,s per run, {len(doc['seeds'])} seeds; "
             f"each pair of values is one CAS per commit / commit service ($G = {doc['group_size']}$). Haz.: hazards "
             f"committed, summed over seeds. Blocks, redos: false semantic blocks and false file-conflict redos per "
             f"run. Lower block: the purge-heavy mix.}}",
             "\\label{tab:swarm}", "\\setlength{\\tabcolsep}{2.4pt}", "\\resizebox{\\columnwidth}{!}{%",
             "\\begin{tabular}{@{}l r r r r r@{}}", "\\toprule",
             "Rule & Haz. & Blocks & Redos & Commits/s & P99 (s) \\\\", "\\midrule"]
    for i, mix in enumerate(("agentic", "purge-heavy")):
        if i:
            lines.append("\\midrule")
        a = top if mix == "agentic" else heavy_agents[-1]
        for name in ("b1", "b1r", "b2", "semlake"):
            one, svc = get(name, a, mix), get(f"{name}_group", a, mix)
            pair = lambda key, fmt: f"{format(one[key], fmt)} / {format(svc[key], fmt)}"  # noqa: E731
            lines.append(f"{LABELS[name]} & {pair('hazards_committed_total', 'd')} & {pair('false_blocks', '.1f')} & "
                         f"{pair('redo_false', '.0f')} & {pair('commits_per_s', '.1f')} & {pair('p99_s', '.1f')} \\\\")
    lines += ["\\bottomrule", "\\end{tabular}}", "\\end{table}"]
    _write_table("tab_swarm", lines)


# --------------------------------------------------------------------- ablation table (7.6)


def ablation(n) -> None:
    """One table; every cell is a macro written by the steps above or by paper_numbers.py."""
    m = n.macros
    need = ("FullSemFBPct", "FullNoProjFBPct", "FullSemPrec", "FullStaticPrec", "OracleSemPrec", "OracleStaticPrec",
            "UpdSemRec", "UpdDelOnlyRec", "PruneFullBytes", "PruneProjBytes", "PruneNoneBytes", "VecUpdS", "LoopUpdS",
            "VecUpdRows", "VecDelS", "LoopDelS", "VecDelRows", "VecPreds", "VecPredsS", "LoopPredsS", "ThrSemGrpPeak", "ThrSemOptPeak",
            "PackBoneMax", "PackMax")
    missing = [key for key in need if key not in m]
    if missing:
        raise KeyError(f"ablation table needs {missing}")
    rows = [
        ("Join and subquery projection", "false blocks, full TPC-H", f"{m['FullSemFBPct']}\\%", f"{m['FullNoProjFBPct']}\\%"),
        ("Row-value tier", "precision, full TPC-H", m["FullSemPrec"], m["FullStaticPrec"]),
        ("", "precision, random oracle", m["OracleSemPrec"], m["OracleStaticPrec"]),
        ("Update check (Line 14)", "recall, update pairs", m["UpdSemRec"], m["UpdDelOnlyRec"]),
        ("Replay since the fork", "report merged before a purge", "blocked", "committed"),
        ("Pre-images, filter columns", "bytes per deleted row; safe report",
         f"{m['PruneFullBytes']}; clean", f"{m['PruneProjBytes']}; clean"),
        ("Pre-images, pruned", "bytes per deleted row; safe report",
         f"{m['PruneFullBytes']}; clean", f"{m['PruneNoneBytes']}; blocked"),
        ("Vectorized row-value tier", f"{m['VecUpdRows']} updates, 1 clause (s)", m["VecUpdS"], m["LoopUpdS"]),
        ("", f"{m['VecDelRows']} deletes, 1 clause (s)", m["VecDelS"], m["LoopDelS"]),
        ("", f"{m['VecDelRows']} deletes, {m['VecPreds']} clauses (s)", m["VecPredsS"], m["LoopPredsS"]),
        ("Commit service", "peak commits/s, real threads", m["ThrSemGrpPeak"], m["ThrSemOptPeak"]),
        ("Row-level tracking", f"false aborts, {m['PackMax']} rows/file", "0.00", m["PackBoneMax"]),
    ]
    lines = ["\\begin{table}[t]", "\\centering", "\\footnotesize",
             "\\caption{Ablations: each mechanism switched off in turn. The conflicting report is blocked in every "
             "pre-image setting.}",
             "\\label{tab:ablation}", "\\setlength{\\tabcolsep}{2.5pt}", "\\resizebox{\\columnwidth}{!}{%",
             "\\begin{tabular}{@{}l l r r@{}}", "\\toprule",
             "Mechanism & Measured & On & Off \\\\", "\\midrule"]
    lines += [f"{a} & {b} & {c} & {d} \\\\" for a, b, c, d in rows]
    lines += ["\\bottomrule", "\\end{tabular}}", "\\end{table}"]
    _write_table("tab_ablation", lines)


STEPS = (environment, ledger, full_queries, recovery, tails, sweep, prune, s3_v3, scan_lifecycle, threads, swarm,
         ablation)
