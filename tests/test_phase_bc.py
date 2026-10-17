"""Tests for the merge service, SQL extraction, and the added workloads."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from benchmarks.cost_efficiency import ASSUMPTIONS, crossover, long_lived_outcomes, sample_merge_io
from benchmarks.delta_baseline import delta_available, second_writer_conflicts
from benchmarks.swarm_service import run_seeds, summarize
from benchmarks.workloads import (
    workload_branchbench_curation,
    workload_branchbench_swe,
    workload_tpch_refresh,
)
from semlake.merge import load_delta, three_way_merge
from semlake.metadata import Repository, Schema, SchemaField
from semlake.sql_predicates import predicates_from_sql
from semlake.storage import MemoryObjectStore


class CostEfficiencyTest(unittest.TestCase):
    def test_a_long_lived_branch_has_no_false_choice(self) -> None:
        outcome = long_lived_outcomes(n_agents=4, rounds=4, n_rows=8)
        self.assertEqual(outcome["vsa"], 0)
        self.assertEqual(outcome["clean"], outcome["merges"])

    def test_copy_on_write_rewrites_more_than_the_delta(self) -> None:
        io = sample_merge_io(n_merges=4)
        self.assertLess(io["semlake_put_bytes"], ASSUMPTIONS["rewritten_file_bytes"])
        point = crossover(io, scan_s=0.015, overhead=0.10)
        self.assertIsNotNone(point)
        self.assertGreater(point, 0)


class ReplayCostTest(unittest.TestCase):
    def test_one_summary_get_per_non_merge_commit(self) -> None:
        repo = Repository(MemoryObjectStore())
        schema = Schema((SchemaField("id", "int64"), SchemaField("value", "int64")))
        base = repo.create_table("main", schema, rows={1: {"id": 1, "value": 1}}, rows_per_file=1)
        first = repo.apply(base, "a", updates={1: {"value": 2}})
        second = repo.apply(first, "a", updates={1: {"value": 3}})
        third = repo.apply(second, "a", updates={1: {"value": 4}})
        scratch = repo.store.clone()
        delta = load_delta(repo, scratch, base.snapshot_id, third.snapshot_id)
        self.assertEqual(scratch.counters.get, 3)
        self.assertEqual(delta.row_ops[1][1]["value"], 4)


class SwarmServiceTest(unittest.TestCase):
    def test_five_seeds_have_no_hazard_and_group_commit_raises_throughput(self) -> None:
        rows = run_seeds()
        single = summarize(rows, 1)
        grouped = summarize(rows, 8)
        self.assertEqual(single["seeds"], 5)
        self.assertEqual(single["hazards"], 0)
        self.assertEqual(grouped["hazards"], 0)
        self.assertLess(single["tps_median"], 8)
        self.assertGreater(grouped["tps_median"], single["tps_median"] * 3)
        parts = summarize(rows, 1, heads="partition")
        self.assertEqual(parts["seeds"], 5)
        self.assertEqual(parts["hazards"], 0)
        self.assertGreater(parts["tps_median"], single["tps_median"] * 3)


class SqlPredicateTest(unittest.TestCase):
    def test_delete_and_select_where_clauses(self) -> None:
        deleted = predicates_from_sql(
            "DELETE FROM lineitem WHERE l_shipdate < 19930201",
            name="rf1",
        )
        read = predicates_from_sql(
            "SELECT SUM(l_extendedprice * l_discount) FROM lineitem "
            "WHERE l_shipdate >= 19930101 AND l_discount >= 0.05 AND l_quantity < 24",
            name="q6",
        )
        self.assertEqual(deleted[0].kind, "deletes_where")
        self.assertIn("l_shipdate < 19930201", deleted[0].where)
        self.assertEqual(read[0].kind, "reads_where")
        self.assertIn("l_quantity < 24", read[0].where)


class AddedWorkloadTest(unittest.TestCase):
    def test_tpch_refresh_conflicts_with_q6(self) -> None:
        repo, refresh, report = workload_tpch_refresh()
        result = three_way_merge(repo, refresh.snapshot_id, report.snapshot_id, repo.store.clone())
        self.assertEqual(result.status, "semantic_conflict")

    def test_curation_merge_is_clean_until_the_same_cell(self) -> None:
        repo, left, right, clash = workload_branchbench_curation()
        clean = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(clean.status, "clean")
        conflict = three_way_merge(repo, left.snapshot_id, clash.snapshot_id, repo.store.clone())
        self.assertEqual(conflict.status, "vsa")

    def test_swe_merge_is_clean_until_the_same_path(self) -> None:
        repo, left, right, clash = workload_branchbench_swe()
        clean = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(clean.status, "clean")
        conflict = three_way_merge(repo, left.snapshot_id, clash.snapshot_id, repo.store.clone())
        self.assertEqual(conflict.status, "vsa")


class SharedRowsTest(unittest.TestCase):
    def test_a_child_commit_keeps_the_parent_row_map(self) -> None:
        repo = Repository(MemoryObjectStore())
        schema = Schema((SchemaField("id", "int64"), SchemaField("value", "int64")))
        source = {i: {"id": i, "value": i} for i in range(4096)}
        base = repo.create_table("main", schema, rows=source, rows_per_file=256)
        child = repo.apply(base, "a", updates={3: {"value": 9}})
        self.assertIs(child.rows.parent, base.rows)
        self.assertEqual(set(child.rows.overlay), {3})
        self.assertEqual(base.rows[3]["value"], 3)
        self.assertEqual(child.rows[3]["value"], 9)
        self.assertEqual(len(child.rows), 4096)
        self.assertNotIn(1, repo.apply(base, "b", deletes=[1]).rows)
        self.assertIn(1, base.rows)


@unittest.skipUnless(delta_available(), "deltalake is not installed")
class DeltaBaselineTest(unittest.TestCase):
    def test_second_writer_from_the_same_version_conflicts(self) -> None:
        self.assertTrue(second_writer_conflicts())
