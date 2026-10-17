"""Randomized checks of the read-delete rule against a brute-force oracle.

Each trial forks two branches from one base table. Branch A may update some
rows, then deletes the rows matching a random clause (sometimes outside the
static fragment). Branch B reads with another random clause. The oracle says
the pair conflicts when a row A deleted satisfied B's clause at the fork, which
is what B read, or when it was deleted. SemLake must never miss such a pair,
and with the row-value check on it must never block a pair the oracle clears.
A second test chains the pair through an intermediate clean merge, so the
check also runs on deltas replayed across a merge commit.
"""

from __future__ import annotations

import random
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from benchmarks.rule_baselines import rows_matching
from semlake.merge import three_way_merge
from semlake.metadata import Repository, Schema, SchemaField
from semlake.storage import MemoryObjectStore
from semlake.verification import Predicate

KINDS = ("vip", "user", "spam")
SEEDS = 200


def _schema() -> Schema:
    return Schema((SchemaField("id", "int64"), SchemaField("kind", "string"), SchemaField("val", "int64")))


def _clause(rng: random.Random, outside_fragment: bool) -> str:
    kind = rng.choice(KINDS)
    lo = rng.randrange(0, 40)
    hi = lo + rng.randrange(1, 30)
    if outside_fragment:
        return rng.choice([
            f"NOT (kind = '{kind}') AND val >= {lo}",
            f"(kind = '{kind}' OR val < {lo}) AND val < {hi}",
            f"val + 1 > {hi}",
        ])
    return rng.choice([
        f"kind = '{kind}'",
        f"kind = '{kind}' AND val >= {lo}",
        f"val BETWEEN {lo} AND {hi}",
        f"kind IN ('{kind}', '{rng.choice(KINDS)}') AND val < {hi}",
        f"kind <> '{kind}' AND val > {lo}",
    ])


def _build(rng: random.Random, chained: bool):
    repo = Repository(MemoryObjectStore())
    rows = {rid: {"id": rid, "kind": rng.choice(KINDS), "val": rng.randrange(0, 60)} for rid in range(48)}
    base = repo.create_table("main", _schema(), rows=rows, rows_per_file=16)

    updates = {rid: {"val": rng.randrange(0, 60)} for rid in rng.sample(sorted(rows), rng.randrange(0, 6))}
    after_update = {rid: {**row, **updates.get(rid, {})} for rid, row in rows.items()}
    delete_where = _clause(rng, rng.random() < 0.3)
    matched = rows_matching(delete_where, after_update)
    assert matched is not None
    tip_a = base
    if updates:
        tip_a = repo.apply(tip_a, "A", updates=updates)
    tip_a = repo.apply(tip_a, "A", deletes=sorted(matched),
                       predicates=(Predicate("purge", "deletes_where", where=delete_where),))

    read_where = _clause(rng, rng.random() < 0.3)
    report_id = 1000 + rng.randrange(100)
    tip_b = repo.apply(base, "B", inserts={report_id: {"id": report_id, "kind": "user", "val": 0}},
                       predicates=(Predicate("report", "reads_where", where=read_where),))
    if chained:
        # B first merges cleanly with an unrelated branch C; the merge commit then meets A.
        tip_c = repo.apply(base, "C", inserts={2000: {"id": 2000, "kind": "user", "val": 1}})
        mid = three_way_merge(repo, tip_b.snapshot_id, tip_c.snapshot_id, repo.store)
        assert mid.status == "clean", mid.message
        tip_b = repo.snapshots[mid.snapshot_id]

    seen_at_fork = rows_matching(read_where, {rid: rows[rid] for rid in matched})
    seen_at_delete = rows_matching(read_where, {rid: after_update[rid] for rid in matched})
    assert seen_at_fork is not None and seen_at_delete is not None
    truth = bool(seen_at_fork or seen_at_delete)
    return repo, tip_a, tip_b, truth


class ReadDeleteFuzzTest(unittest.TestCase):
    def _run(self, chained: bool) -> None:
        missed, false_blocks = [], []
        for seed in range(SEEDS):
            repo, tip_a, tip_b, truth = _build(random.Random(seed), chained)
            for left, right in ((tip_a, tip_b), (tip_b, tip_a)):
                result = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone())
                flagged = result.status == "semantic_conflict"
                if truth and not flagged:
                    missed.append(seed)
                if flagged and not truth:
                    false_blocks.append(seed)
                static = three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store.clone(),
                                         post_check=False)
                if truth:
                    self.assertEqual(static.status, "semantic_conflict", f"static check missed seed {seed}")
        self.assertEqual(missed, [], "hazards merged without a conflict")
        self.assertEqual(false_blocks, [], "safe pairs blocked")

    def test_direct_pairs(self) -> None:
        self._run(chained=False)

    def test_pairs_after_an_intermediate_merge(self) -> None:
        self._run(chained=True)


if __name__ == "__main__":
    unittest.main()
