"""Unit tests for the merge rules, the baselines and the semantic check."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from semlake.baselines import (
    WriteConflictException,
    copy_on_write,
    crdt_merge,
    file_level_occ,
    tuple_version_merge,
)
from semlake.merge import Delta, resolve_rows, three_way_merge
from semlake.metadata import CommitGraph, Repository, Schema, SchemaField, merge_schemas
from semlake.storage import DeletionVector, MemoryObjectStore, materialize, read_parquet
from semlake.verification import Predicate, where_overlap
from benchmarks.dolt_baseline import dolt_binary, dolt_merge
from benchmarks.workloads import run_swarm, semantic_cases


def _brute_lca(graph: CommitGraph, left: str, right: str) -> str:
    left_ancestors = graph.ancestors(left)
    best_id = ""
    best_gen = -1
    for node, gen in graph.ancestors(right).items():
        if node not in left_ancestors:
            continue
        if gen > best_gen or (gen == best_gen and node < best_id):
            best_gen = gen
            best_id = node
    return best_id


def _schema() -> Schema:
    return Schema(
        (
            SchemaField("id", "int64"),
            SchemaField("value", "int64"),
            SchemaField("note", "string"),
        )
    )


class CommitGraphTest(unittest.TestCase):
    def test_lca_matches_a_full_ancestor_walk(self) -> None:
        graph = CommitGraph()
        graph.add("base", ())
        tips = ["base"]
        for index in range(25):
            left = f"L{index}"
            right = f"R{index}"
            merged = f"M{index}"
            parent = tips[-1]
            graph.add(left, (parent,))
            graph.add(right, (parent,))
            graph.add(merged, (left, right))
            tips.append(merged)
        self.assertEqual(graph.lca("L24", "R24"), "M23")
        self.assertEqual(graph.lca("M24", "L0"), "L0")
        for left, right in (("M20", "M10"), ("R5", "M24"), ("L24", "L24")):
            self.assertEqual(graph.lca(left, right), _brute_lca(graph, left, right))

    def test_ancestor_set_is_stored_with_the_snapshot(self) -> None:
        graph = CommitGraph()
        graph.add("base", ())
        graph.add("a", ("base",))
        graph.add("b", ("a",))
        stored = graph.ancestors("b")
        self.assertIs(graph.ancestors("b"), stored)
        self.assertEqual(set(stored), {"base", "a", "b"})
        self.assertEqual(stored["a"], graph.generation["a"])
        graph.add("side", ("base",))
        graph.add("merged", ("b", "side"))
        self.assertEqual(set(graph.ancestors("b")), {"base", "a", "b"})
        self.assertEqual(set(graph.ancestors("merged")), {"base", "a", "b", "side", "merged"})

    def test_lca_is_the_fork_point(self) -> None:
        graph = CommitGraph()
        graph.add("base", ())
        graph.add("a", ("base",))
        graph.add("b", ("base",))
        graph.add("a2", ("a",))
        self.assertEqual(graph.lca("a2", "b"), "base")
        self.assertEqual(graph.lca("a2", "a"), "a")


class DeletionVectorTest(unittest.TestCase):
    def test_union_is_bitwise_or(self) -> None:
        left = DeletionVector.from_positions([1, 2])
        right = DeletionVector.from_positions([2, 5])
        merged = DeletionVector.from_bytes(left.to_bytes()).union(
            DeletionVector.from_bytes(right.to_bytes())
        )
        self.assertEqual(set(merged.bitmap), {1, 2, 5})

    def test_a_row_live_in_two_files_is_an_error(self) -> None:
        repo = Repository(MemoryObjectStore())
        rows = {rid: {"id": rid, "value": rid, "note": "base"} for rid in range(4)}
        base = repo.create_table("main", _schema(), rows=rows, rows_per_file=4)
        meta = base.files["file-00000"]
        base.files["copy"] = type(meta)("copy", meta.object_key, meta.size_bytes, "copy", meta.dv_bytes)
        with self.assertRaises(RuntimeError):
            materialize(repo.store, base)


class MergeRuleTest(unittest.TestCase):
    def test_disjoint_columns_merge(self) -> None:
        schema = _schema()
        left = Delta({1: ("update", {"value": 9}, "f")}, {"f"}, schema, (), {})
        right = Delta({1: ("update", {"note": "b"}, "f")}, {"f"}, schema, (), {})
        merged, choices, conflicts = resolve_rows(left, right, policy="vsa")
        self.assertEqual(conflicts, 0)
        self.assertEqual(choices, [])
        self.assertEqual(merged[1][1], {"value": 9, "note": "b"})

    def test_same_column_becomes_vsa(self) -> None:
        schema = _schema()
        left = Delta({1: ("update", {"value": 9}, "f")}, {"f"}, schema, (), {})
        right = Delta({1: ("update", {"value": 4}, "f")}, {"f"}, schema, (), {})
        _merged, choices, conflicts = resolve_rows(left, right, policy="vsa")
        self.assertEqual(conflicts, 1)
        self.assertEqual(choices[0]["kind"], "choice")
        self.assertEqual(choices[0]["column"], "value")

    def test_added_column_is_compatible(self) -> None:
        base = Schema((SchemaField("id", "int64"),))
        left = Schema((SchemaField("id", "int64"), SchemaField("flag", "int64", True)))
        merged, error = merge_schemas(base, left, base)
        self.assertIsNone(error)
        self.assertEqual(merged.names(), ["id", "flag"])


class WorkloadRuleTest(unittest.TestCase):
    def _repo(self):
        repo = Repository(MemoryObjectStore())
        rows = {rid: {"id": rid, "value": rid, "note": "base"} for rid in range(20)}
        base = repo.create_table("main", _schema(), rows=rows, rows_per_file=20)
        return repo, base

    def test_false_conflict_and_zero_copy(self) -> None:
        repo, base = self._repo()
        left = repo.apply(base, "A", updates={0: {"value": 100}})
        right = repo.apply(base, "B", updates={5: {"note": "other"}})
        self.assertEqual(base.rows[0]["value"], 0)

        sem = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(sem.status, "clean")
        self.assertEqual(sem.cell_conflicts, 0)

        with self.assertRaises(WriteConflictException):
            file_level_occ(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())

        before = repo.store.objects[base.files["file-00000"].object_key]
        cow = copy_on_write(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(cow.status, "clean")
        self.assertEqual(repo.store.objects[base.files["file-00000"].object_key], before)

    def test_semantic_delete_is_blocked_only_by_semlake(self) -> None:
        repo, base = self._repo()
        left = repo.apply(base, "A", deletes=[0, 1, 2])
        keep = Predicate("keep", "rows_live", frozenset({0, 1, 2}), "report depends on these rows")
        right = repo.apply(
            base,
            "B",
            inserts={99: {"id": 99, "value": 3, "note": "report"}},
            predicates=(keep,),
        )
        sem = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(sem.status, "semantic_conflict")
        occ = file_level_occ(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(occ.status, "clean")
        cow = copy_on_write(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(cow.status, "clean")
        versions = tuple_version_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(versions.status, "clean")
        converged = crdt_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(converged.status, "clean")


    def test_crdt_overwrites_a_real_conflict(self) -> None:
        repo, base = self._repo()
        left = repo.apply(base, "A", updates={1: {"value": 8}})
        right = repo.apply(base, "B", updates={1: {"value": 9}})
        crdt = crdt_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(crdt.status, "clean")
        self.assertEqual(crdt.cell_conflicts, 1)
        sem = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(sem.status, "vsa")
        self.assertEqual(sem.cell_conflicts, crdt.cell_conflicts)

    def test_where_clauses_decide_the_semantic_check(self) -> None:
        repo, base = self._repo()
        left = repo.apply(
            base,
            "A",
            deletes=[0, 1],
            predicates=(Predicate("purge", "deletes_where", where="value < 2"),),
        )
        overlap = repo.apply(
            base,
            "B",
            inserts={99: {"id": 99, "value": 1, "note": "report"}},
            predicates=(Predicate("report", "reads_where", where="value BETWEEN 0 AND 5"),),
        )
        disjoint = repo.apply(
            base,
            "B",
            inserts={98: {"id": 98, "value": 1, "note": "report"}},
            predicates=(Predicate("report", "reads_where", where="value >= 2"),),
        )
        hit = three_way_merge(repo, left.snapshot_id, overlap.snapshot_id, repo.store.clone())
        self.assertEqual(hit.status, "semantic_conflict")
        miss = three_way_merge(repo, left.snapshot_id, disjoint.snapshot_id, repo.store.clone())
        self.assertEqual(miss.status, "clean")


class WhereOverlapTest(unittest.TestCase):
    def test_overlap_cases(self) -> None:
        self.assertTrue(where_overlap("kind = 'vip'", "kind = 'vip' AND value >= 10")[0])
        self.assertFalse(where_overlap("kind = 'spam'", "kind = 'vip'")[0])
        self.assertFalse(where_overlap("value < 10", "value >= 10")[0])
        self.assertTrue(where_overlap("value <= 10", "value >= 10")[0])
        self.assertFalse(where_overlap("value BETWEEN 1 AND 3", "value > 3")[0])
        self.assertTrue(where_overlap("kind IN ('a', 'b')", "kind != 'a'")[0])
        self.assertFalse(where_overlap("kind IN ('a', 'b')", "kind = 'c'")[0])
        self.assertFalse(where_overlap("note = 'x AND y'", "note = 'z'")[0])

    def test_unparsed_syntax_is_treated_as_overlap(self) -> None:
        overlap, reason = where_overlap("kind = 'a' OR kind = 'b'", "kind = 'c'")
        self.assertTrue(overlap)
        self.assertIn("not analyzed", reason)

    def test_deleted_rows_clear_an_unparsed_false_alarm(self) -> None:
        cases = {case["name"]: case for case in semantic_cases()}
        for name in ("or is not a real overlap", "not the same value"):
            case = cases[name]
            result = three_way_merge(
                case["repo"], case["left_id"], case["right_id"], case["repo"].store.clone()
            )
            self.assertEqual(result.status, "clean", name)
            self.assertEqual(result.outcome, "keep", name)
        real = cases["unparsed not that overlaps"]
        blocked = three_way_merge(
            real["repo"], real["left_id"], real["right_id"], real["repo"].store.clone()
        )
        self.assertEqual(blocked.status, "semantic_conflict")


class ReadableSnapshotTest(unittest.TestCase):
    def _table(self):
        repo = Repository(MemoryObjectStore())
        rows = {rid: {"id": rid, "value": rid, "note": "base"} for rid in range(40)}
        base = repo.create_table("main", _schema(), rows=rows, rows_per_file=10)
        return repo, base

    def test_deletion_vector_stores_the_position_in_the_file(self) -> None:
        repo, base = self._table()
        updated = repo.apply(base, "A", updates={25: {"value": 250}})
        vector = DeletionVector.from_bytes(updated.files["file-00002"].dv_bytes)
        self.assertEqual(set(vector.bitmap), {5})
        delta = updated.files[f"delta-{updated.snapshot_id}"]
        stored = read_parquet(repo.store.get(delta.object_key))
        self.assertEqual(stored, [{"__row_id": 25, "id": 25, "value": 250, "note": "base"}])

    def test_merged_table_matches_both_branches(self) -> None:
        repo, base = self._table()
        left = repo.apply(base, "A", updates={25: {"value": 250}})
        right = repo.apply(base, "B", updates={3: {"note": "other"}}, deletes=[11])
        result = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(result.status, "clean")
        merged = repo.snapshots[result.snapshot_id]
        live = materialize(repo.store, merged)
        self.assertEqual(live[25]["value"], 250)
        self.assertEqual(live[3]["note"], "other")
        self.assertNotIn(11, live)
        self.assertEqual(len(live), 39)
        again = repo.apply(merged, "C", updates={25: {"value": 251}})
        self.assertEqual(materialize(repo.store, again)[25]["value"], 251)

    def test_unresolved_conflict_keeps_the_base_row(self) -> None:
        repo, base = self._table()
        left = repo.apply(base, "A", updates={25: {"value": 8}})
        right = repo.apply(base, "B", updates={25: {"value": 9}})
        result = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(result.status, "vsa")
        live = materialize(repo.store, repo.snapshots[result.snapshot_id])
        self.assertEqual(live[25]["value"], 25)
        self.assertEqual(result.vsa[0]["column"], "value")
        self.assertEqual(result.outcome, "replan")

    def test_an_open_choice_survives_the_next_merge(self) -> None:
        repo, base = self._table()
        left = repo.apply(base, "A", updates={25: {"value": 8}})
        right = repo.apply(base, "B", updates={25: {"value": 9}})
        parked = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        other = repo.apply(base, "C", updates={3: {"note": "later"}})
        again = three_way_merge(
            repo, parked.snapshot_id, other.snapshot_id, repo.store.clone()
        )
        self.assertEqual(again.status, "vsa")
        self.assertEqual(again.outcome, "replan")
        live = materialize(repo.store, repo.snapshots[again.snapshot_id])
        self.assertEqual(live[25]["value"], 25)
        self.assertEqual(live[3]["note"], "later")

    def test_a_predicate_survives_a_clean_merge(self) -> None:
        repo, base = self._table()
        deleted = repo.apply(
            base,
            "A",
            deletes=[0],
            predicates=(Predicate("purge", "deletes_where", where="value < 1"),),
        )
        side = repo.apply(base, "B", updates={3: {"note": "side"}})
        merged = three_way_merge(repo, deleted.snapshot_id, side.snapshot_id, repo.store.clone())
        self.assertEqual(merged.status, "clean")
        reader = repo.apply(
            base,
            "C",
            predicates=(Predicate("report", "reads_where", where="value < 2"),),
        )
        blocked = three_way_merge(
            repo, merged.snapshot_id, reader.snapshot_id, repo.store.clone()
        )
        self.assertEqual(blocked.status, "semantic_conflict")
        self.assertEqual(blocked.outcome, "block")

    def test_a_report_survives_a_merge_and_still_blocks_a_purge(self) -> None:
        repo, base = self._table()
        report = repo.apply(
            base,
            "report",
            predicates=(Predicate("report", "reads_where", where="value < 5"),),
        )
        updated = repo.apply(base, "U", updates={20: {"value": 200}})
        once = three_way_merge(repo, report.snapshot_id, updated.snapshot_id, repo.store.clone())
        self.assertEqual(once.status, "clean")
        purge = repo.apply(
            updated,
            "purge",
            deletes=[0, 1, 2],
            predicates=(Predicate("purge", "deletes_where", where="value < 5"),),
        )
        blocked = three_way_merge(repo, once.snapshot_id, purge.snapshot_id, repo.store.clone())
        self.assertEqual(blocked.status, "semantic_conflict")
        self.assertEqual(blocked.outcome, "block")

    def test_shared_prefix_is_not_a_later_conflict(self) -> None:
        repo, base = self._table()
        first = repo.apply(base, "x", updates={5: {"value": 1}})
        advanced = repo.apply(first, "x", updates={5: {"value": 2}})
        side = repo.apply(first, "y", updates={30: {"value": -1}})
        merged = three_way_merge(repo, advanced.snapshot_id, side.snapshot_id, repo.store.clone())
        self.assertEqual(merged.status, "clean")
        self.assertEqual(materialize(repo.store, repo.snapshots[merged.snapshot_id])[5]["value"], 2)
        other = repo.apply(base, "z", updates={33: {"value": 7}})
        later = three_way_merge(repo, merged.snapshot_id, other.snapshot_id, repo.store.clone())
        self.assertEqual(later.status, "clean")
        self.assertEqual(later.vsa, [])
        self.assertEqual(materialize(repo.store, repo.snapshots[later.snapshot_id])[5]["value"], 2)

    def test_a_branch_merged_twice_does_not_conflict_with_itself(self) -> None:
        repo, base = self._table()
        branch = base
        head = base.snapshot_id
        for value in (1, 2, 3):
            local = repo.apply(branch, "a", updates={5: {"value": value}})
            merged = three_way_merge(repo, local.snapshot_id, head, repo.store.clone())
            self.assertEqual(merged.status, "clean", value)
            self.assertEqual(merged.vsa, [])
            head = merged.snapshot_id
            branch = local
        live = materialize(repo.store, repo.snapshots[head])
        self.assertEqual(live[5]["value"], 3)

    def test_delete_check_uses_the_value_at_delete_time(self) -> None:
        repo, base = self._table()
        updated = repo.apply(base, "d", updates={3: {"value": 50}})
        deleted = repo.apply(
            updated,
            "d",
            deletes=[3],
            predicates=(Predicate("purge", "deletes_where", where="value >= 50 OR kind = 'X'"),),
        )
        reader = repo.apply(
            base,
            "r",
            inserts={1000: {"id": 1000, "value": 1, "note": "agg"}},
            predicates=(Predicate("report", "reads_where", where="value >= 40"),),
        )
        blocked = three_way_merge(repo, deleted.snapshot_id, reader.snapshot_id, repo.store.clone())
        self.assertEqual(blocked.status, "semantic_conflict")

    def test_column_add_and_append_round_trip(self) -> None:
        repo = Repository(MemoryObjectStore())
        schema = Schema((SchemaField("id", "int64"), SchemaField("value", "int64")))
        rows = {rid: {"id": rid, "value": rid} for rid in range(10)}
        base = repo.create_table("main", schema, rows=rows, rows_per_file=10)
        widened = Schema(schema.fields + (SchemaField("flag", "int64", True),))
        left = repo.apply(base, "A", schema=widened, message="add flag")
        right = repo.apply(base, "B", inserts={20: {"id": 20, "value": 7}}, message="append")
        before = base.files["file-00000"].object_key
        result = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        merged = repo.snapshots[result.snapshot_id]
        self.assertEqual(merged.files["file-00000"].object_key, before)
        live = materialize(repo.store, merged)
        self.assertEqual(live[20], {"id": 20, "value": 7, "flag": None})
        self.assertIsNone(live[0]["flag"])


class InvariantSuiteTest(unittest.TestCase):
    def test_domain_and_foreign_key(self) -> None:
        repo = Repository(MemoryObjectStore())
        schema = Schema((SchemaField("id", "int64"), SchemaField("value", "int64")))
        rows = {rid: {"id": rid, "value": rid} for rid in range(5)}
        base = repo.create_table("main", schema, rows=rows, rows_per_file=5)
        domain = Predicate("non_negative", "domain", where="value >= 0")
        left = repo.apply(base, "A", predicates=(domain,), message="check value >= 0")
        right = repo.apply(base, "B", updates={1: {"value": -1}})
        blocked = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(blocked.status, "semantic_conflict")

        parent = Predicate("user_fk", "foreign_key", frozenset({1}), "orders.user_id -> users.id")
        child = repo.apply(
            base,
            "B",
            inserts={9: {"id": 9, "value": 1}},
            predicates=(parent,),
        )
        removed = repo.apply(base, "A", deletes=[1])
        missing = three_way_merge(repo, removed.snapshot_id, child.snapshot_id, repo.store.clone())
        self.assertEqual(missing.status, "semantic_conflict")

        kept = repo.apply(base, "A", deletes=[2])
        allowed = three_way_merge(repo, kept.snapshot_id, child.snapshot_id, repo.store.clone())
        self.assertEqual(allowed.status, "clean")


class SwarmTest(unittest.TestCase):
    def test_disjoint_writers_all_commit(self) -> None:
        samples = run_swarm(4, 1, 0.0, n_rows=32, seed=3)
        by_name = {item["system"]: item for item in samples}
        self.assertEqual(by_name["semlake"]["successes"], 4)
        self.assertEqual(by_name["b1"]["successes"], 4)
        self.assertGreater(by_name["semlake"]["retries_per_commit"], 0)
        self.assertGreater(by_name["b1"]["retries_per_commit"], 0)

    def test_same_row_writers_still_commit(self) -> None:
        samples = run_swarm(4, 1, 1.0, n_rows=32, seed=3)
        by_name = {item["system"]: item for item in samples}
        self.assertEqual(by_name["semlake"]["successes"], 4)
        self.assertEqual(by_name["b1"]["successes"], 4)
        self.assertGreater(by_name["semlake"]["parked"], 0)
        self.assertEqual(by_name["b1"]["parked"], 0)
        self.assertEqual(by_name["semlake"]["applied"] + by_name["semlake"]["parked"], 4)


class SchemaEvolutionTest(unittest.TestCase):
    def test_column_add_does_not_rewrite_untouched_files(self) -> None:
        repo = Repository(MemoryObjectStore())
        schema = Schema((SchemaField("id", "int64"),))
        base = repo.create_table("main", schema, n_files=4, file_size=128)
        widened = Schema(schema.fields + (SchemaField("flag", "int64", True),))
        left = repo.apply(base, "A", schema=widened, message="add flag")
        right = repo.apply(base, "B", inserts={1: {"id": 1}}, message="append one row")
        scratch = repo.store.clone()
        cow = copy_on_write(repo, left.snapshot_id, right.snapshot_id, scratch)
        self.assertEqual(cow.status, "clean")
        rewritten = [key for key in scratch.objects if key.startswith("data/cow-")]
        self.assertEqual(len(rewritten), 1)
        sem = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
        self.assertEqual(sem.status, "clean")


@unittest.skipUnless(dolt_binary(), "dolt is not installed")
class DoltBaselineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cases = {case["name"]: case for case in semantic_cases()}

    def _dolt(self, name: str):
        case = self.cases[name]
        return dolt_merge(case["repo"], case["left_id"], case["right_id"], case["repo"].store.clone())

    def _semlake(self, name: str):
        case = self.cases[name]
        return three_way_merge(case["repo"], case["left_id"], case["right_id"], case["repo"].store.clone())

    def test_foreign_key_is_caught_and_a_miss_is_not(self) -> None:
        self.assertEqual(self._dolt("foreign key").status, "semantic_conflict")
        self.assertEqual(self._dolt("foreign key miss").status, "clean")

    def test_where_and_rows_live_are_not_constraints(self) -> None:
        self.assertEqual(self._semlake("eq overlap").status, "semantic_conflict")
        self.assertEqual(self._dolt("eq overlap").status, "clean")
        self.assertEqual(self._semlake("rows live").status, "semantic_conflict")
        self.assertEqual(self._dolt("rows live").status, "clean")

    def test_check_on_a_satisfied_column_is_caught(self) -> None:
        self.assertEqual(self._semlake("check on a satisfied column").status, "semantic_conflict")
        caught = self._dolt("check on a satisfied column")
        self.assertEqual(caught.status, "semantic_conflict")
        self.assertIn("constraint", caught.message)

    def test_existing_rows_block_the_domain_check(self) -> None:
        self.assertEqual(self._semlake("domain violation").status, "semantic_conflict")
        missed = self._dolt("domain violation")
        self.assertEqual(missed.status, "clean")
        self.assertIn("check_rejected", missed.message)


if __name__ == "__main__":
    unittest.main()
