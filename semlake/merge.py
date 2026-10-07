"""3-way diff, zero-copy manifest merge, and version-space conflicts."""

from __future__ import annotations

from dataclasses import dataclass, field

from semlake.metadata import FileMeta, Repository, Schema, merge_schemas
from semlake.storage import (
    DeletionVector,
    MemoryObjectStore,
    dump_json,
    file_from_doc,
    file_to_doc,
    load_json,
)
from semlake.verification import Predicate, check_invariants, deleted_ids


@dataclass
class Delta:
    row_ops: dict[int, tuple]
    changed_files: set[str]
    schema: Schema
    predicates: tuple[Predicate, ...]
    files: dict[str, FileMeta]


@dataclass
class MergeResult:
    system: str
    status: str
    snapshot_id: str | None = None
    vsa: list[dict] = field(default_factory=list)
    violations: list[dict] = field(default_factory=list)
    cell_conflicts: int = 0
    message: str = ""


def load_delta(repo: Repository, store: MemoryObjectStore, base_id: str, tip_id: str) -> Delta:
    """Replay commit summaries from ``base_id`` to ``tip_id``.

    Only the per-commit summary objects are read, so the cost grows with the
    number of changed files on the branch.
    """
    base = repo.snapshots[base_id]
    if tip_id == base_id:
        return _delta_from_doc(load_json(store.get(base.summary_key)))

    ops: dict[int, tuple] = {}
    files: dict[str, FileMeta] = {}
    changed: set[str] = set()
    predicates: list[Predicate] = []
    schema = base.schema
    for snap_id in reversed(repo.graph.path_to(tip_id, base_id)):
        step = _delta_from_doc(load_json(store.get(repo.snapshots[snap_id].summary_key)))
        changed |= step.changed_files
        files.update(step.files)
        ops.update(step.row_ops)
        predicates.extend(step.predicates)
        schema = step.schema
    return Delta(ops, changed, schema, tuple(predicates), files)


def _delta_from_doc(doc: dict) -> Delta:
    return Delta(
        row_ops={
            int(rid): (item["op"], item["values"], item["file"])
            for rid, item in doc["row_ops"].items()
        },
        changed_files=set(doc["changed_files"]),
        schema=Schema.from_doc(doc["schema"]),
        predicates=tuple(Predicate.from_doc(item) for item in doc["predicates"]),
        files={fid: file_from_doc(fid, meta) for fid, meta in doc["files"].items()},
    )


def resolve_rows(
    left_delta: Delta,
    right_delta: Delta,
    policy: str,
) -> tuple[dict[int, tuple], list[dict], int]:
    """Combine row ops. ``vsa`` keeps real conflicts open; ``lww`` lets the right side win.

    Both policies count conflicts the same way: one per contested cell, and
    one for a row that is deleted on one side and changed on the other.
    """
    merged: dict[int, tuple] = {}
    choices: list[dict] = []
    conflicts = 0
    for rid in sorted(set(left_delta.row_ops) | set(right_delta.row_ops)):
        left = left_delta.row_ops.get(rid)
        right = right_delta.row_ops.get(rid)
        if left is None:
            merged[rid] = right
            continue
        if right is None:
            merged[rid] = left
            continue
        if policy == "lww":
            merged[rid] = _last_writer_wins(left, right)
            conflicts += _count_conflicts(left, right)
            continue
        op, row_choices = _merge_row_vsa(rid, left, right)
        conflicts += len(row_choices)
        choices.extend(row_choices)
        if op is not None:
            merged[rid] = op
    return merged, choices, conflicts


def _count_conflicts(left: tuple, right: tuple) -> int:
    if left == right:
        return 0
    if left[0] == "update" and right[0] == "update":
        shared = set(left[1]) & set(right[1])
        return sum(1 for col in shared if left[1][col] != right[1][col])
    if left[0] == "delete" and right[0] == "delete":
        return 0
    return 1


def _last_writer_wins(left: tuple, right: tuple) -> tuple:
    if left[0] == "update" and right[0] == "update":
        return ("update", {**left[1], **right[1]}, right[2] or left[2])
    if left[0] == "delete" and right[0] == "delete":
        return left
    return right


def _merge_row_vsa(rid: int, left: tuple, right: tuple) -> tuple[tuple | None, list[dict]]:
    """Merge one row changed on both sides. Return the merged op and any choice nodes."""
    if left == right:
        return left, []
    if left[0] == "update" and right[0] == "update":
        cols: dict = {}
        choices: list[dict] = []
        for col in sorted(set(left[1]) | set(right[1])):
            if col in left[1] and col in right[1] and left[1][col] != right[1][col]:
                choices.append(
                    {
                        "kind": "choice",
                        "row_id": rid,
                        "column": col,
                        "options": [
                            {"branch": "A", "value": left[1][col]},
                            {"branch": "B", "value": right[1][col]},
                        ],
                    }
                )
            else:
                cols[col] = left[1][col] if col in left[1] else right[1][col]
        op = ("update", cols, left[2]) if cols else None
        return op, choices
    if left[0] == "delete" and right[0] == "delete":
        return left, []
    # A delete on one side and an update or insert on the other leaves the whole row open.
    choice = {
        "kind": "choice",
        "row_id": rid,
        "column": "*",
        "options": [
            {"branch": "A", "op": left[0], "value": left[1]},
            {"branch": "B", "op": right[0], "value": right[1]},
        ],
    }
    return None, [choice]


def three_way_merge(
    repo: Repository,
    left_id: str,
    right_id: str,
    store: MemoryObjectStore,
) -> MergeResult:
    """Merge two branch tips by writing a delta manifest. Base data files are not rewritten."""
    store.list("manifests/")
    base_id = repo.graph.lca(left_id, right_id)
    base_doc = load_json(store.get(repo.snapshots[base_id].summary_key))
    base_schema = Schema.from_doc(base_doc["schema"])
    left_delta = load_delta(repo, store, base_id, left_id)
    right_delta = load_delta(repo, store, base_id, right_id)

    violations = check_invariants(
        deleted_ids(left_delta.row_ops),
        left_delta.predicates,
        deleted_ids(right_delta.row_ops),
        right_delta.predicates,
    )
    if violations:
        return MergeResult(
            system="semlake",
            status="semantic_conflict",
            violations=[item.to_doc() for item in violations],
            message=violations[0].detail,
        )

    schema, schema_error = merge_schemas(base_schema, left_delta.schema, right_delta.schema)
    if schema_error or schema is None:
        return MergeResult(system="semlake", status="schema_conflict", message=schema_error or "")

    merged_ops, vsa, conflicts = resolve_rows(left_delta, right_delta, policy="vsa")
    files_delta = _merge_file_delta(repo.snapshots[base_id].files, left_delta, right_delta)
    snap_id = repo.new_snapshot_id()
    _put_merge_manifest(store, snap_id, base_id, left_id, right_id, schema, merged_ops, files_delta, vsa)
    return MergeResult(
        system="semlake",
        status="vsa" if vsa else "clean",
        snapshot_id=snap_id,
        vsa=vsa,
        cell_conflicts=conflicts,
        message="merged without rewriting base files",
    )


def _union_file(base_files: dict[str, FileMeta], left: FileMeta, right: FileMeta) -> FileMeta:
    dv = DeletionVector.from_bytes(left.dv_bytes).union(DeletionVector.from_bytes(right.dv_bytes))
    source = base_files.get(left.file_id, left)
    return FileMeta(
        file_id=left.file_id,
        object_key=source.object_key,
        size_bytes=source.size_bytes,
        token="merged-dv",
        dv_bytes=dv.to_bytes(),
    )


def _merge_file_delta(base_files, left_delta: Delta, right_delta: Delta) -> dict[str, FileMeta]:
    both = left_delta.changed_files & right_delta.changed_files
    merged: dict[str, FileMeta] = {}
    for fid in sorted(left_delta.changed_files | right_delta.changed_files):
        left = left_delta.files.get(fid)
        right = right_delta.files.get(fid)
        if fid in both and left is not None and right is not None:
            merged[fid] = _union_file(base_files, left, right)
        elif left is not None:
            merged[fid] = left
        elif right is not None:
            merged[fid] = right
    return merged


def _put_merge_manifest(store, snap_id, base_id, left_id, right_id, schema, ops, files_delta, vsa) -> None:
    # Row ids are dense per table here, so one bitmap of row ids stands in for
    # the per-file positional vectors already stored in files_delta.
    positions = [rid for rid, (kind, _values, _fid) in ops.items() if kind in ("update", "delete")]
    doc = {
        "snapshot_id": snap_id,
        "base": base_id,
        "parents": [left_id, right_id],
        "schema": schema.to_doc(),
        "row_ops": {
            str(rid): {"op": kind, "values": values, "file": fid}
            for rid, (kind, values, fid) in ops.items()
        },
        "changed_files": sorted(files_delta),
        "files": {fid: file_to_doc(meta) for fid, meta in files_delta.items()},
        "vsa": vsa,
    }
    store.put(f"manifests/{snap_id}.summary.json", dump_json(doc))
    store.put(f"dv/{snap_id}.roaring", DeletionVector.from_positions(positions).to_bytes())
