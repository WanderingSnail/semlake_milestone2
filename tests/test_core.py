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
from semlake.storage import DeletionVector, MemoryObjectStore
from semlake.verification import Predicate, where_overlap


def _schema() -> Schema:
    return Schema(
        (
            SchemaField("id", "int64"),
            SchemaField("value", "int64"),
            SchemaField("note", "string"),
        )
    )


class CommitGraphTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
