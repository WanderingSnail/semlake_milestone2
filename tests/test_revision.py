"""Tests for the changes made in response to the reviews.

Each test names the review comment it answers (R1.x for Reviewer 1, MCx and
mx for Reviewer 2) or the rubric item (crash recovery).
"""

from __future__ import annotations

import random
import unittest

from benchmarks.workloads import workload_w1s
from semlake import reader
from semlake.maintenance import (
    FaultyStore,
    check_repository,
    collect_garbage,
    prunable,
    prune_preimages,
    reachable_objects,
    resolve_choices,
    unresolved,
)
from semlake.merge import three_way_merge
from semlake.metadata import (
    CrashError,
    OpenChoicesError,
    Repository,
    Schema,
    SchemaField,
    merge_schemas,
)
from semlake.sql_predicates import predicates_from_sql, project_read
from semlake.storage import MemoryObjectStore, load_json
from semlake.verification import Predicate


def _schema() -> Schema:
    return Schema((SchemaField("id", "int64"), SchemaField("kind", "string"), SchemaField("value", "int64")))


def _repo(n_rows: int = 40, rows_per_file: int = 8, store=None):
    repo = Repository(store or MemoryObjectStore())
    rows = {i: {"id": i, "kind": "vip" if i % 4 == 0 else "user", "value": i} for i in range(n_rows)}
    base = repo.create_table("main", _schema(), rows=rows, rows_per_file=rows_per_file)
    return repo, base


def _merge(repo, left, right, **kwargs):
    return three_way_merge(repo, left.snapshot_id, right.snapshot_id, repo.store, **kwargs)


class LosslessWidening(unittest.TestCase):
    """m1: int64 -> double is not lossless."""

    def test_int64_to_double_is_a_schema_conflict(self) -> None:
        base = Schema((SchemaField("x", "int64"),))
        wide = Schema((SchemaField("x", "double"),))
        merged, error = merge_schemas(base, base, wide)
        self.assertIsNone(merged)
        self.assertIn("incompatible", error)
        # The value the reviewer named really does change.
        self.assertNotEqual(int(float(2**53 + 1)), 2**53 + 1)

    def test_lossless_promotions_still_merge(self) -> None:
        for narrow, wide in (("int32", "int64"), ("int32", "double"), ("float", "double")):
            base = Schema((SchemaField("x", narrow),))
            merged, error = merge_schemas(base, base, Schema((SchemaField("x", wide),)))
            self.assertIsNone(error)
            self.assertEqual(merged.fields[0].type_name, wide)


class SharedFileBenchmark(unittest.TestCase):
    """MC3: the Figure 5 merge must publish both branches' changes."""

    def test_merge_carries_both_branches_deletion_vectors(self) -> None:
        scenario = workload_w1s(40, 0, rows_per_file=8)
        repo = scenario.repo
        base = repo.snapshots[scenario.base_id]
        store = repo.store.clone()
        result = three_way_merge(repo, scenario.left_id, scenario.right_id, store)
        self.assertEqual(result.status, "clean")
        merged = repo.snapshots[result.snapshot_id]
        touched = [fid for fid in base.files if merged.files[fid].dv_bytes != base.files[fid].dv_bytes]
        self.assertEqual(len(touched), 40)
        # The published manifests alone give back the merged rows.
        live = reader.materialize(repo, result.snapshot_id, store)
        self.assertEqual(len(live), len(base.rows))
        self.assertEqual(live[0]["amount"], 1000.0)
        self.assertEqual(live[20 * 8 + 1]["amount"], 2000.0)
        delta = load_json(store.objects[f"manifests/{result.snapshot_id}.delta.json"])
        self.assertEqual(len(delta), 41)  # 40 base files with new DVs and one delta file

    def test_commit_metadata_grows_with_the_change_not_the_table(self) -> None:
        sizes = {}
        for n_files in (50, 400):
            repo = Repository(MemoryObjectStore())
            rows = {i: {"id": i, "kind": "user", "value": i} for i in range(n_files * 4)}
            base = repo.create_table("main", _schema(), rows=rows, rows_per_file=4)
            before = repo.store.counters.put_bytes
            repo.apply(base, "A", updates={0: {"value": 99}})
            sizes[n_files] = repo.store.counters.put_bytes - before
        # Eight times the files, the same single-row commit: bytes written stay flat.
        self.assertLess(sizes[400], sizes[50] * 1.2)

    def test_manifest_chain_reads_back_after_a_checkpoint(self) -> None:
        repo, base = _repo()
        head = base
        for step in range(40):
            head = repo.apply(head, "A", updates={step % 40: {"value": 1000 + step}})
        live = reader.materialize(repo, head.snapshot_id)
        self.assertEqual({rid: row["value"] for rid, row in live.items()},
                         {rid: row["value"] for rid, row in head.rows.items()})
        self.assertLessEqual(len(head.manifest_parts), 17)


class PurgeWins(unittest.TestCase):
    """MC5 (b): a purged row must not stay readable."""

    def test_purge_delete_wins_over_concurrent_update(self) -> None:
        repo, base = _repo()
        purge = repo.apply(base, "A", deletes=[4], purge=True,
                           predicates=[Predicate("purge", "deletes_where", where="id = 4")])
        edit = repo.apply(base, "B", updates={4: {"value": 400}})
        for left, right in ((purge, edit), (edit, purge)):
            result = _merge(repo, left, right)
            self.assertEqual(result.status, "clean")
            self.assertEqual(result.vsa, [])
            merged = repo.snapshots[result.snapshot_id]
            self.assertNotIn(4, merged.rows)
            self.assertNotIn(4, reader.materialize(repo, result.snapshot_id))
            self.assertEqual(result.overridden[0]["row_id"], 4)
            self.assertEqual(result.overridden[0]["value"], {"value": 400})
            summary = load_json(repo.store.objects[merged.summary_key])
            self.assertEqual(summary["overridden"][0]["row_id"], 4)

    def test_unmarked_delete_stays_an_open_choice(self) -> None:
        repo, base = _repo()
        delete = repo.apply(base, "A", deletes=[4])
        edit = repo.apply(base, "B", updates={4: {"value": 400}})
        result = _merge(repo, delete, edit)
        self.assertEqual(result.status, "vsa")
        merged = repo.snapshots[result.snapshot_id]
        self.assertEqual(unresolved(merged), {4: ["*"]})
        self.assertEqual(merged.rows[4]["value"], 4)  # shown at its fork value


class ChoiceResolution(unittest.TestCase):
    """MC5: reader-visible semantics and a resolution protocol for choice nodes."""

    def _collide(self):
        repo, base = _repo()
        left = repo.apply(base, "A", updates={5: {"value": 50}, 6: {"kind": "vip"}})
        right = repo.apply(base, "B", updates={5: {"value": 55}, 7: {"value": 70}})
        result = _merge(repo, left, right)
        return repo, repo.snapshots[result.snapshot_id], result

    def test_open_choice_shows_the_fork_value_and_is_listed(self) -> None:
        repo, merged, result = self._collide()
        self.assertEqual(result.status, "vsa")
        self.assertEqual(unresolved(merged), {5: ["value"]})
        self.assertEqual(merged.rows[5]["value"], 5)
        self.assertEqual(merged.rows[6]["kind"], "vip")  # the non-colliding writes landed
        self.assertEqual(merged.rows[7]["value"], 70)

    def test_protected_branch_refuses_open_choices_until_resolved(self) -> None:
        repo, merged, _result = self._collide()
        repo.protected.add("main")
        with self.assertRaises(OpenChoicesError):
            repo.cas_head("main", repo.heads["main"], merged.snapshot_id)
        for pick, expected in (("A", 50), ("B", 55), (("value", 52), 52)):
            fixed = resolve_choices(repo, merged, {(5, "value"): pick})
            self.assertEqual(unresolved(fixed), {})
            self.assertEqual(fixed.rows[5]["value"], expected)
            self.assertEqual(reader.materialize(repo, fixed.snapshot_id)[5]["value"], expected)
        self.assertTrue(repo.cas_head("main", repo.heads["main"], fixed.snapshot_id))

    def test_whole_row_choice_resolves_to_either_operation(self) -> None:
        repo, base = _repo()
        delete = repo.apply(base, "A", deletes=[4])
        edit = repo.apply(base, "B", updates={4: {"value": 400}})
        merged = repo.snapshots[_merge(repo, delete, edit).snapshot_id]
        gone = resolve_choices(repo, merged, {(4, "*"): "A"})
        self.assertNotIn(4, gone.rows)
        kept = resolve_choices(repo, merged, {(4, "*"): "B"})
        self.assertEqual(kept.rows[4]["value"], 400)

    def test_resolution_is_checked_by_a_later_merge(self) -> None:
        repo, merged, _result = self._collide()
        fixed = resolve_choices(repo, merged, {(5, "value"): "A"})
        reader_branch = repo.apply(
            merged, "R", inserts={900: {"id": 900, "kind": "report", "value": 0}},
            predicates=[Predicate("report", "reads_where", where="value >= 50", columns=frozenset({"value"}))],
        )
        result = _merge(repo, fixed, reader_branch)
        self.assertEqual(result.status, "semantic_conflict")


class ReadCapture(unittest.TestCase):
    """MC1: undeclared reads must not slip through silently."""

    def test_uncaptured_commit_blocks_a_concurrent_delete_in_strict_mode(self) -> None:
        repo, base = _repo()
        report = repo.apply(base, "B", inserts={900: {"id": 900, "kind": "report", "value": 1}},
                            reads_captured=False)
        purge = repo.apply(base, "A", deletes=[4],
                           predicates=[Predicate("purge", "deletes_where", where="id = 4")])
        self.assertEqual(_merge(repo, purge, report).status, "semantic_conflict")
        self.assertEqual(_merge(repo, purge, report, strict_reads=False).status, "clean")

    def test_uncaptured_commit_still_merges_with_disjoint_inserts(self) -> None:
        repo, base = _repo()
        report = repo.apply(base, "B", inserts={900: {"id": 900, "kind": "report", "value": 1}},
                            reads_captured=False)
        other = repo.apply(base, "A", predicates=[], message="no change")
        self.assertEqual(_merge(repo, other, report).status, "clean")

    def test_select_without_where_reads_the_whole_table(self) -> None:
        (pred,) = predicates_from_sql("SELECT sum(value) FROM users", "report")
        self.assertEqual((pred.kind, pred.where), ("reads_where", ""))


LINEITEM = frozenset({"l_orderkey", "l_partkey", "l_quantity", "l_discount", "l_shipdate",
                      "l_commitdate", "l_receiptdate", "l_returnflag", "l_shipmode"})


class JoinProjection(unittest.TestCase):
    """R1.2 / MC1: joins and subqueries as declared reads."""

    def test_join_keeps_only_single_table_conjuncts(self) -> None:
        sql = ("SELECT sum(l_discount) FROM customer, orders, lineitem "
               "WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey "
               "AND o_orderdate < 19950315 AND l_shipdate > 19950315")
        ((where, columns),) = project_read(sql, "lineitem", LINEITEM)
        self.assertEqual(where, "(l_shipdate > 19950315)")
        self.assertEqual(columns, frozenset({"l_discount", "l_orderkey"}))

    def test_subquery_block_contributes_its_own_clause(self) -> None:
        sql = ("SELECT count(*) FROM orders WHERE o_orderdate >= 19930701 AND EXISTS ("
               "SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)")
        ((where, columns),) = project_read(sql, "lineitem", LINEITEM)
        self.assertEqual(where, "(l_commitdate < l_receiptdate)")
        self.assertEqual(columns, frozenset({"l_orderkey"}))  # EXISTS (SELECT * ...) uses no column

    def test_correlated_only_subquery_reads_the_whole_table(self) -> None:
        sql = ("SELECT sum(l_discount) FROM lineitem, part WHERE p_partkey = l_partkey AND l_quantity < ("
               "SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)")
        clauses = [where for where, _columns in project_read(sql, "lineitem", LINEITEM)]
        self.assertEqual(clauses, [""])

    def test_or_keeps_the_single_table_part_of_every_branch(self) -> None:
        sql = ("SELECT sum(l_discount) FROM lineitem, part WHERE "
               "(p_partkey = l_partkey AND p_size < 5 AND l_quantity >= 1 AND l_quantity <= 11) OR "
               "(p_partkey = l_partkey AND p_size < 10 AND l_quantity >= 10 AND l_quantity <= 20)")
        ((where, columns),) = project_read(sql, "lineitem", LINEITEM)
        self.assertEqual(where, "(((l_quantity >= 1) AND (l_quantity <= 11)) OR ((l_quantity >= 10) AND (l_quantity <= 20)))")
        # A row can move between the two branches, so l_quantity stays a checked column.
        self.assertEqual(columns, frozenset({"l_discount", "l_partkey", "l_quantity"}))

    def test_or_with_a_branch_that_has_nothing_to_keep_reads_the_whole_table(self) -> None:
        sql = ("SELECT count(*) FROM lineitem, part WHERE p_partkey = l_partkey AND "
               "(l_quantity < 5 OR p_size = 3)")
        clauses = [where for where, _columns in project_read(sql, "lineitem", LINEITEM)]
        self.assertEqual(clauses, [""])

    def test_each_block_gets_its_own_clause(self) -> None:
        sql = ("SELECT count(*) FROM lineitem WHERE l_shipdate < 19940101 AND l_orderkey IN ("
               "SELECT l_orderkey FROM lineitem WHERE l_returnflag = 'R')")
        preds = predicates_from_sql(sql, "q", table="lineitem", table_columns=LINEITEM)
        self.assertEqual([pred.where for pred in preds], ["(l_shipdate < 19940101)", "(l_returnflag = 'R')"])

    def test_update_that_moves_a_row_between_blocks_conflicts(self) -> None:
        rows = {i: {"id": i, "kind": "vip" if i % 2 else "user", "value": i} for i in range(20)}
        repo = Repository(MemoryObjectStore())
        base = repo.create_table("main", _schema(), rows=rows, rows_per_file=5)
        sql = ("SELECT count(*) FROM users WHERE value < 10 AND id IN ("
               "SELECT id FROM users WHERE kind = 'vip')")
        preds = predicates_from_sql(sql, "report", table="users", table_columns=frozenset({"id", "kind", "value"}))
        report = repo.apply(base, "B", predicates=preds)
        # Row 3 is in both blocks; moving it out of the first block changes the count.
        writer = repo.apply(base, "A", updates={3: {"value": 50}})
        self.assertEqual(_merge(repo, writer, report).status, "semantic_conflict")
        # Row 12 (a 'user' with value >= 10) is in neither block.
        harmless = repo.apply(base, "A2", updates={12: {"value": 60}})
        self.assertEqual(_merge(repo, harmless, report).status, "clean")

    def test_outer_join_reads_the_whole_table(self) -> None:
        sql = ("SELECT count(*) FROM orders LEFT JOIN lineitem ON l_orderkey = o_orderkey "
               "WHERE l_shipdate IS NULL")
        clauses = [where for where, _columns in project_read(sql, "lineitem", LINEITEM)]
        self.assertEqual(clauses, [""])

    def test_query_that_does_not_read_the_table_declares_nothing(self) -> None:
        self.assertEqual(
            predicates_from_sql("SELECT count(*) FROM orders, customer WHERE o_custkey = c_custkey",
                                "q", table="lineitem", table_columns=LINEITEM), ())

    def test_projection_is_sound_on_random_join_reads(self) -> None:
        """A delete the projection lets through never changes the join's result."""
        import duckdb

        rng = random.Random(11)
        con = duckdb.connect()
        con.execute("CREATE TABLE dim AS SELECT range AS d_key, range % 3 AS d_group FROM range(10)")
        for _trial in range(60):
            rows = {i: {"id": i, "kind": rng.choice(["vip", "user"]), "value": rng.randrange(30)}
                    for i in range(40)}
            repo = Repository(MemoryObjectStore())
            base = repo.create_table("main", _schema(), rows=rows, rows_per_file=8)
            low = rng.randrange(25)
            sql = (f"SELECT sum(value), count(*) FROM users, dim WHERE value % 10 = d_key "
                   f"AND d_group = {rng.randrange(3)} AND value >= {low} AND kind = 'vip'")
            preds = predicates_from_sql(sql, "report", table="users",
                                        table_columns=frozenset({"id", "kind", "value"}))
            report = repo.apply(base, "B", inserts={900 + _trial: {"id": 900, "kind": "out", "value": -1}},
                                predicates=preds)
            cut = rng.randrange(30)
            doomed = [rid for rid, row in rows.items() if row["value"] < cut and rng.random() < 0.7]
            purge = repo.apply(base, "A", deletes=doomed,
                               predicates=[Predicate("purge", "deletes_where", where=f"value < {cut}")])
            result = _merge(repo, purge, report)

            def answer(live: dict) -> tuple:
                import pyarrow as pa

                table = pa.table({col: [row[col] for row in live.values()] for col in ("id", "kind", "value")})
                con.register("users", table)
                out = con.execute(sql).fetchall()
                con.unregister("users")
                return out

            changed = answer(rows) != answer({rid: row for rid, row in rows.items() if rid not in doomed})
            if changed:
                self.assertEqual(result.status, "semantic_conflict", sql)


class PreimagePruning(unittest.TestCase):
    """MC5 (c): pruning pre-images can only add blocks."""

    def _pair(self, read_where: str):
        repo, base = _repo()
        doomed = [rid for rid in range(40) if rid % 4 == 0 and rid >= 20]
        purge = repo.apply(base, "A", deletes=doomed, purge=True,
                           predicates=[Predicate("purge", "deletes_where", where="kind = 'vip' AND value >= 20")])
        report = repo.apply(base, "B", inserts={900: {"id": 900, "kind": "report", "value": 0}},
                            predicates=[Predicate("report", "reads_where", where=read_where,
                                                  columns=frozenset({"value"}))])
        return repo, purge, report

    def test_full_prune_removes_the_values_and_keeps_true_conflicts(self) -> None:
        repo, purge, report = self._pair("value >= 30")
        self.assertEqual(_merge(repo, purge, report).status, "semantic_conflict")
        sizes = prune_preimages(repo, purge.snapshot_id)
        self.assertLess(sizes["after_bytes"], sizes["before_bytes"])
        summary = load_json(repo.store.objects[purge.summary_key])
        self.assertEqual(summary["before"], {})
        self.assertEqual(summary["pruned"], [20, 24, 28, 32, 36])
        self.assertEqual(_merge(repo, purge, report).status, "semantic_conflict")

    def test_full_prune_turns_an_undecidable_safe_merge_into_a_block(self) -> None:
        # Not statically disjoint (value overlaps), but no deleted row has value < 10.
        repo, purge, report = self._pair("value < 10")
        self.assertEqual(_merge(repo, purge, report).status, "clean")
        prune_preimages(repo, purge.snapshot_id)
        self.assertEqual(_merge(repo, purge, report).status, "semantic_conflict")

    def test_projection_onto_filter_columns_keeps_precision(self) -> None:
        repo, purge, report = self._pair("value < 10")
        prune_preimages(repo, purge.snapshot_id, keep_columns={"value"})
        summary = load_json(repo.store.objects[purge.summary_key])
        self.assertEqual(set(summary["before"]["20"]), {"value"})
        self.assertEqual(_merge(repo, purge, report).status, "clean")

    def test_statically_disjoint_read_blocks_once_fully_pruned(self) -> None:
        # Fully pruned rows may have changed before the delete, so a static miss is not enough.
        repo, purge, report = self._pair("kind = 'user'")
        self.assertEqual(_merge(repo, purge, report).status, "clean")
        prune_preimages(repo, purge.snapshot_id)
        self.assertEqual(_merge(repo, purge, report).status, "semantic_conflict")

    def test_pruning_never_hides_a_conflict(self) -> None:
        rng = random.Random(5)
        for _trial in range(80):
            repo, base = _repo()
            cut = rng.randrange(5, 35)
            doomed = [rid for rid in range(40) if rid >= cut and rng.random() < 0.6]
            if not doomed:
                continue
            purge = repo.apply(base, "A", deletes=doomed,
                               predicates=[Predicate("purge", "deletes_where", where=f"value >= {cut}")])
            low = rng.randrange(40)
            clause = rng.choice([f"value >= {low}", f"kind = 'vip' AND value < {low}", f"NOT value < {low}"])
            report = repo.apply(base, "B", inserts={900: {"id": 900, "kind": "report", "value": 0}},
                                predicates=[Predicate("report", "reads_where", where=clause)])
            before = _merge(repo, purge, report).status
            prune_preimages(repo, purge.snapshot_id, keep_columns=rng.choice([(), ("value",), ("kind",)]))
            after = _merge(repo, purge, report).status
            if before == "semantic_conflict":
                self.assertEqual(after, "semantic_conflict", clause)

    def test_a_commit_is_prunable_once_every_branch_contains_it(self) -> None:
        repo, purge, report = self._pair("kind = 'user'")
        repo.heads["A"] = purge.snapshot_id
        repo.heads["B"] = report.snapshot_id
        self.assertFalse(prunable(repo, purge.snapshot_id))
        merged = _merge(repo, purge, report)
        for branch in ("main", "A", "B"):
            repo.heads[branch] = merged.snapshot_id
        self.assertTrue(prunable(repo, purge.snapshot_id))


class VectorizedChecks(unittest.TestCase):
    """MC4: one pass per read clause gives the same answers as the row loop."""

    def test_vectorized_and_row_by_row_agree(self) -> None:
        from semlake import verification

        rng = random.Random(3)
        old = verification.VECTOR_THRESHOLD
        verification.VECTOR_THRESHOLD = 1
        try:
            for _trial in range(120):
                repo, base = _repo(n_rows=60)
                cut = rng.randrange(60)
                doomed = [rid for rid in range(60) if rid < cut and rng.random() < 0.5]
                edits = {rid: {"value": rng.randrange(80)} for rid in range(60)
                         if rid not in doomed and rng.random() < 0.3}
                writer = repo.apply(base, "A", updates=edits, deletes=doomed,
                                    predicates=[Predicate("purge", "deletes_where", where=f"id < {cut}")])
                low = rng.randrange(80)
                clause = rng.choice([f"value >= {low}", f"kind = 'vip' AND value < {low}",
                                     f"NOT value < {low}", f"value < {low} OR kind = 'vip'",
                                     f"value BETWEEN {low} AND {low + 9}"])
                report = repo.apply(base, "B", inserts={900: {"id": 900, "kind": "report", "value": -5}},
                                    predicates=[Predicate("report", "reads_where", where=clause,
                                                          columns=frozenset({"value"}))])
                fast = _merge(repo, writer, report, vectorized=True).status
                slow = _merge(repo, writer, report, vectorized=False).status
                self.assertEqual(fast, slow, clause)
        finally:
            verification.VECTOR_THRESHOLD = old


class CrashRecovery(unittest.TestCase):
    """Rubric: concurrency protocol and crash/recovery invariants (I1 to I5)."""

    def _setup(self, crash_at=None):
        store = FaultyStore()
        repo, base = _repo(store=store)
        left = repo.apply(base, "A", updates={1: {"value": 100}, 9: {"value": 900}}, deletes=[2])
        right = repo.apply(base, "B", updates={17: {"value": 170}}, inserts={500: {"id": 500, "kind": "user", "value": 5}})
        repo.heads["A"] = left.snapshot_id
        repo.heads["B"] = right.snapshot_id
        return repo, store, base, left, right

    def _expected_rows(self) -> dict:
        repo, _store, _base, left, right = self._setup()
        result = _merge(repo, left, right)
        return reader.materialize(repo, result.snapshot_id)

    def test_crash_at_every_write_of_a_merge_keeps_the_invariants(self) -> None:
        expected = self._expected_rows()
        repo, store, _base, left, right = self._setup()
        writes_before = store.puts_seen
        clean = _merge(repo, left, right)
        n_writes = store.puts_seen - writes_before
        self.assertGreaterEqual(n_writes, 4)
        del clean
        for k in range(1, n_writes + 1):
            repo, store, base, left, right = self._setup()
            head_before = dict(repo.heads)
            snapshots_before = set(repo.snapshots)
            store.crash_at = store.puts_seen + k
            with self.assertRaises(CrashError):
                _merge(repo, left, right)
            store.crash_at = None
            # I2: nothing a reader can see changed.
            self.assertEqual(repo.heads, head_before)
            self.assertEqual(set(repo.snapshots), snapshots_before)
            self.assertEqual(check_repository(repo), [])  # I1
            # I5: the half-written objects are unreachable and are collected.
            orphans = collect_garbage(repo)
            self.assertEqual(len(orphans), k - 1)
            self.assertEqual(check_repository(repo), [])
            self.assertEqual(set(store.objects), reachable_objects(repo))
            # I3: the retry gives the same rows, and the head moves in one step.
            result = _merge(repo, left, right)
            self.assertEqual(result.status, "clean")
            self.assertTrue(repo.cas_head("main", base.snapshot_id, result.snapshot_id))
            self.assertEqual(check_repository(repo), [])
            self.assertEqual(reader.materialize(repo, repo.heads["main"]), expected)

    def test_crash_at_every_write_of_a_commit_keeps_the_invariants(self) -> None:
        repo, store, base, _left, _right = self._setup()
        before = store.puts_seen
        repo.apply(base, "C", updates={3: {"value": 33}}, inserts={600: {"id": 600, "kind": "user", "value": 6}})
        n_writes = store.puts_seen - before
        for k in range(1, n_writes + 1):
            repo, store, base, _left, _right = self._setup()
            snapshots_before = set(repo.snapshots)
            store.crash_at = store.puts_seen + k
            with self.assertRaises(CrashError):
                repo.apply(base, "C", updates={3: {"value": 33}},
                           inserts={600: {"id": 600, "kind": "user", "value": 6}})
            store.crash_at = None
            self.assertEqual(set(repo.snapshots), snapshots_before)
            self.assertEqual(check_repository(repo), [])
            self.assertEqual(len(collect_garbage(repo)), k - 1)
            retry = repo.apply(base, "C", updates={3: {"value": 33}},
                               inserts={600: {"id": 600, "kind": "user", "value": 6}})
            self.assertEqual(reader.materialize(repo, retry.snapshot_id)[3]["value"], 33)

    def test_group_commit_is_all_or_nothing(self) -> None:
        store = FaultyStore()
        repo, base = _repo(store=store)
        branches = [repo.apply(base, f"a{i}", updates={i: {"value": 1000 + i}}) for i in range(4)]

        def publish_batch() -> str:
            current = repo.heads["main"]
            for snap in branches:
                result = three_way_merge(repo, current, snap.snapshot_id, store)
                current = result.snapshot_id
            repo.cas_head("main", repo.heads["main"], current)
            return current

        store.crash_at = store.puts_seen + 6  # inside the second of four merges
        with self.assertRaises(CrashError):
            publish_batch()
        store.crash_at = None
        self.assertEqual(repo.heads["main"], base.snapshot_id)  # I4: none of the batch is visible
        self.assertEqual(reader.materialize(repo, repo.heads["main"])[0]["value"], 0)
        collect_garbage(repo, roots=tuple(snap.snapshot_id for snap in branches))
        self.assertEqual(check_repository(repo), [])
        publish_batch()
        live = reader.materialize(repo, repo.heads["main"])
        self.assertEqual([live[i]["value"] for i in range(4)], [1000, 1001, 1002, 1003])

    def test_a_lost_compare_and_swap_changes_nothing(self) -> None:
        repo, _store, base, left, right = self._setup()
        first = _merge(repo, left, right)
        self.assertTrue(repo.cas_head("main", base.snapshot_id, first.snapshot_id))
        other = repo.apply(base, "C", updates={30: {"value": 3000}})
        self.assertFalse(repo.cas_head("main", base.snapshot_id, other.snapshot_id))
        self.assertEqual(repo.heads["main"], first.snapshot_id)


if __name__ == "__main__":
    unittest.main()
