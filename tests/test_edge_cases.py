"""Merge chains and row histories that earlier versions of the engine got wrong."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from semlake.merge import three_way_merge
from semlake.metadata import Repository, Schema, SchemaField
from semlake.storage import MemoryObjectStore, materialize
from semlake.verification import Predicate

SCHEMA = Schema((SchemaField("id", "int64"), SchemaField("kind", "string"), SchemaField("value", "int64")))


def _fresh():
    store = MemoryObjectStore()
    repo = Repository(store)
    rows = {i: {"id": i, "kind": "K", "value": i} for i in range(40)}
    return store, repo, repo.create_table("main", SCHEMA, rows=rows, rows_per_file=10)


def _merge(repo, store, left, right):
    return three_way_merge(repo, left.snapshot_id, right.snapshot_id, store)


class EdgeCaseTest(unittest.TestCase):
    def test_report_rebased_then_merged_with_purge(self) -> None:
        store, repo, base = _fresh()
        update = repo.apply(base, "u", updates={1: {"value": 100}})
        report = repo.apply(base, "rep", inserts={1000: {"id": 1000, "kind": "agg", "value": 5}},
                            predicates=(Predicate("report", "reads_where", where="kind = 'K' AND value >= 20"),))
        first = _merge(repo, store, report, update)
        self.assertEqual(first.status, "clean")
        purge = repo.apply(update, "pur", deletes=[25, 26],
                           predicates=(Predicate("purge", "deletes_where", where="kind = 'K' AND value >= 25"),))
        second = _merge(repo, store, repo.snapshots[first.snapshot_id], purge)
        self.assertEqual(second.status, "semantic_conflict")

    def test_shared_history_is_not_a_conflict(self) -> None:
        store, repo, base = _fresh()
        shared = repo.apply(base, "x", updates={5: {"value": 1}})
        ahead = repo.apply(shared, "x", updates={5: {"value": 2}})
        other = repo.apply(shared, "y", updates={30: {"value": -1}})
        merged = _merge(repo, store, ahead, other)
        self.assertEqual(merged.status, "clean")
        merged_snap = repo.snapshots[merged.snapshot_id]
        self.assertEqual(materialize(store, merged_snap)[5]["value"], 2)
        later = _merge(repo, store, merged_snap, repo.apply(base, "z", updates={33: {"value": 7}}))
        self.assertEqual(later.status, "clean")
        self.assertFalse(later.vsa)
        self.assertEqual(materialize(store, repo.snapshots[later.snapshot_id])[5]["value"], 2)

    def test_open_choice_is_carried_until_resolved(self) -> None:
        store, repo, base = _fresh()
        left = repo.apply(base, "a", updates={5: {"value": 1}})
        right = repo.apply(base, "c", updates={5: {"value": 2}})
        first = _merge(repo, store, left, right)
        self.assertEqual(first.status, "vsa")
        first_snap = repo.snapshots[first.snapshot_id]
        unrelated = repo.apply(base, "z", updates={33: {"value": 7}})
        carried = _merge(repo, store, first_snap, unrelated)
        self.assertEqual(carried.status, "vsa")
        self.assertEqual(len(carried.vsa), 1)
        resolved = repo.apply(first_snap, "fix", updates={5: {"value": 3}})
        final = _merge(repo, store, resolved, unrelated)
        self.assertEqual(final.status, "clean")
        self.assertEqual(materialize(store, repo.snapshots[final.snapshot_id])[5]["value"], 3)

    def test_row_updated_then_deleted_keeps_its_delete_time_value(self) -> None:
        store, repo, base = _fresh()
        moved = repo.apply(base, "d", updates={3: {"value": 50}})
        purge = repo.apply(moved, "d", deletes=[3],
                           predicates=(Predicate("purge", "deletes_where", where="value >= 50 OR kind = 'X'"),))
        report = repo.apply(base, "r", inserts={1000: {"id": 1000, "kind": "agg", "value": 1}},
                            predicates=(Predicate("report", "reads_where", where="value >= 40"),))
        self.assertEqual(_merge(repo, store, purge, report).status, "semantic_conflict")

    def test_row_moved_out_of_the_read_range_then_deleted(self) -> None:
        # The reader saw row 30 at the fork (value 30). The other branch lowers it and
        # deletes it with a clause disjoint from the read clause: still a conflict.
        store, repo, base = _fresh()
        moved = repo.apply(base, "d", updates={30: {"value": 1}})
        purge = repo.apply(moved, "d", deletes=[30],
                           predicates=(Predicate("purge", "deletes_where", where="value < 5"),))
        report = repo.apply(base, "r", inserts={1000: {"id": 1000, "kind": "agg", "value": 1}},
                            predicates=(Predicate("report", "reads_where", where="value >= 20"),))
        self.assertEqual(_merge(repo, store, purge, report).status, "semantic_conflict")
        static = three_way_merge(repo, purge.snapshot_id, report.snapshot_id, store, post_check=False)
        self.assertEqual(static.status, "semantic_conflict")


if __name__ == "__main__":
    unittest.main()
