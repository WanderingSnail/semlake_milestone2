#!/usr/bin/env bash
# SemLake: one entry point for the artifact.
#
#   ./reproduce.sh [paper]   Rebuild every number, table and figure of the paper, and the PDF,
#                            from the raw result files in benchmarks/results/ (about a minute).
#   ./reproduce.sh check     paper + the unit tests + a rerun of the labelled suite that must
#                            reproduce the committed decisions (about three minutes).
#   ./reproduce.sh quick     Smoke-run every local experiment at a small size in a scratch copy,
#                            so the committed raw results are not touched, then build the paper.
#   ./reproduce.sh rerun     Rerun every experiment that needs no cloud account or external
#                            server, overwrite benchmarks/results/, then build the paper (hours).
#   ./reproduce.sh docker [mode]   Build the image and run a mode inside it; output in ./out.
#
# Optional profiles (not run by default; their recorded raw results are in the repository):
#   SEMLAKE_S3_BUCKET=... python3 benchmarks/s3_run.py --depths 1000 10000 --out s3_run_v3.json
#   python3 benchmarks/lakefs_runs.py            (needs a lakeFS server, see ARTIFACT.md)
#   python3 benchmarks/delta_spark_baseline.py   (needs Java 21, pyspark 4.0.1, delta-spark 4.0.1)
#   python3 benchmarks/tpch_experiments.py pairs (with the dolt binary on PATH)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-paper}"
PY="${PYTHON:-python3}"
OUT_DIR="${SEMLAKE_OUT:-$ROOT/out}"

step() { printf '\n== %s\n' "$*"; }

build_paper() {
    local root="$1"
    cd "$root"
    step "numbers and tables (paper/generated) from benchmarks/results"
    "$PY" benchmarks/paper_numbers.py
    if grep -q 'providecommand' paper/generated/numbers.tex; then
        echo "error: some numbers have no result file (they would print as ??):" >&2
        grep 'providecommand' paper/generated/numbers.tex >&2
        exit 1
    fi
    step "figures (paper/figures) from benchmarks/results"
    "$PY" benchmarks/paper_experiments.py figures
    mkdir -p "$OUT_DIR"
    cp -r paper/figures "$OUT_DIR/" 2>/dev/null || true
    cp -r paper/generated "$OUT_DIR/" 2>/dev/null || true
    if [ -f "$root/paper/semlake_pvldb.tex" ]; then
        step "PDF"
        cd "$root/paper"
        pdflatex -interaction=nonstopmode -halt-on-error semlake_pvldb.tex >/dev/null
        bibtex semlake_pvldb >/dev/null
        pdflatex -interaction=nonstopmode -halt-on-error semlake_pvldb.tex >/dev/null
        pdflatex -interaction=nonstopmode -halt-on-error semlake_pvldb.tex >semlake_pvldb.build.log
        if grep -q 'LaTeX Warning: Reference\|LaTeX Warning: Citation' semlake_pvldb.build.log; then
            echo "error: undefined references or citations" >&2
            grep 'LaTeX Warning: Reference\|LaTeX Warning: Citation' semlake_pvldb.build.log >&2
            exit 1
        fi
        step "format checks"
        "$PY" "$root/benchmarks/check_pdf.py" semlake_pvldb.pdf
        cp semlake_pvldb.pdf "$OUT_DIR/"
        echo "wrote $OUT_DIR/semlake_pvldb.pdf, $OUT_DIR/figures/ and $OUT_DIR/generated/"
    else
        echo "wrote $OUT_DIR/figures/ and $OUT_DIR/generated/"
    fi
}

run_checks() {
    cd "$ROOT"
    step "unit tests"
    "$PY" -m pytest -q tests
    step "labelled suite against the committed decisions"
    "$PY" benchmarks/verify_committed.py --quick
}

run_experiments() {
    # $1: repository root to run in; $2: "quick" or "full".
    local root="$1" size="$2" q=""
    [ "$size" = quick ] && q="--quick"
    cd "$root"
    step "storage ledger, tail latency, delta sweep, threads, fault injection, pruning"
    "$PY" benchmarks/revision_experiments.py all $q
    step "full TPC-H queries under the result oracle"
    "$PY" benchmarks/tpch_full_queries.py $q
    step "swarm simulation with measured S3 latencies"
    "$PY" benchmarks/scion_sim.py --calibrated $q
    if [ "$size" = full ]; then
        step "labelled suite and TPC-H pairs against the committed decisions"
        "$PY" benchmarks/verify_committed.py
        step "randomized result oracle (300 pairs)"
        "$PY" benchmarks/oracle_experiments.py
        step "selectivity curve, SF 1 scan overhead, compaction trace"
        "$PY" benchmarks/tpch_experiments.py selectivity
        "$PY" benchmarks/tpch_experiments.py scan
        "$PY" benchmarks/tpch_experiments.py compaction
        step "pre-image cost, file packing, cost model"
        "$PY" benchmarks/preimage_cost.py
        "$PY" benchmarks/paper_experiments.py packing
        "$PY" benchmarks/cost_efficiency.py
    else
        "$PY" benchmarks/verify_committed.py --quick
    fi
}

case "$MODE" in
    paper)
        build_paper "$ROOT"
        ;;
    check)
        run_checks
        build_paper "$ROOT"
        ;;
    quick)
        SCRATCH="$(mktemp -d)"
        trap 'rm -rf "$SCRATCH"' EXIT
        step "scratch copy in $SCRATCH (committed raw results stay untouched)"
        cp -r "$ROOT/benchmarks" "$ROOT/semlake" "$ROOT/tests" "$ROOT/paper" "$SCRATCH/"
        rm -rf "$SCRATCH/benchmarks/data"
        run_experiments "$SCRATCH" quick
        step "the paper itself is built from the committed full-size results"
        build_paper "$ROOT"
        ;;
    rerun)
        run_checks
        run_experiments "$ROOT" full
        build_paper "$ROOT"
        ;;
    docker)
        cd "$ROOT"
        docker build -t semlake .
        mkdir -p "$ROOT/out"
        docker run --rm -v "$ROOT/out:/out" semlake "${2:-paper}"
        ;;
    *)
        sed -n '2,19p' "${BASH_SOURCE[0]}"
        exit 2
        ;;
esac
