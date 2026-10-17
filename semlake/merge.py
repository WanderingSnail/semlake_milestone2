"""3-way diff, zero-copy manifest merge, and version-space conflicts."""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from semlake.metadata import FileMeta, Repository, Schema, Snapshot, merge_schemas
from semlake.storage import (
    MemoryObjectStore,
    dump_json,
    empty_dv_bytes,
    file_from_doc,
    file_to_doc,
    load_json,
    with_positions,
    write_parquet,
)
from semlake.verification import Predicate, check_invariants, deleted_ids


@dataclass
class Delta:
    row_ops: dict[int, tuple]
    changed_files: set[str]
    schema: Schema
    predicates: tuple[Predicate, ...]
    files: dict[str, FileMeta]
    vsa: tuple[dict, ...] = ()
    row_state: dict[int, dict] = field(default_factory=dict)


@dataclass
class MergeResult:
    system: str
    status: str
    snapshot_id: str | None = None
    vsa: list[dict] = field(default_factory=list)
    violations: list[dict] = field(default_factory=list)
    cell_conflicts: int = 0
    message: str = ""
    outcome: str = ""


def _agent_outcome(status: str) -> str:
    """Map a merge status onto the action an agent should take next."""
    if status == "clean":
        return "keep"
    if status == "vsa":
        return "replan"
    return "block"


def load_delta(repo: Repository, store: MemoryObjectStore, base_id: str, tip_id: str) -> Delta:
    """Replay each non-merge commit between ``base_id`` and ``tip_id`` once.

    A merge commit stores the combination of its parents. Its summary has no
    predicates and omits rows left open as VSA choices, so the parents are
    replayed in its place. One summary GET is issued per non-merge commit.
    """
    base = repo.snapshots[base_id]
    if tip_id == base_id:
        return Delta({}, set(), base.schema, (), {})

    order = _commits_after(repo, base_id, tip_id)
    folded: dict[str, Delta] = {base_id: Delta({}, set(), base.schema, (), {})}

    def known(snap_id: str) -> Delta:
        # A parent older than the base has no writes in this replay.
        return folded.get(snap_id, folded[base_id])

    for snap_id in order:
        snap = repo.snapshots[snap_id]
        if len(snap.parent_ids) >= 2:
            fork_id = repo.graph.lca(snap.parent_ids[0], snap.parent_ids[1])
            if fork_id not in folded:
                fork_id = base_id
            fork = folded[fork_id]
            branched = _combine_deltas(
                _since(known(snap.parent_ids[0]), fork),
                _since(known(snap.parent_ids[1]), fork),
                base.schema,
            )
            folded[snap_id] = _apply_commit(fork, branched, base.rows)
            continue
        parent_id = snap.parent_ids[0] if snap.parent_ids else base_id
        step = _delta_from_doc(load_json(store.get(snap.summary_key)))
        folded[snap_id] = _apply_commit(known(parent_id), step, base.rows)
    return folded[tip_id]


def _commits_after(repo: Repository, base_id: str, tip_id: str) -> list[str]:
    """Return the commits ``tip_id`` has and ``base_id`` lacks, parents first.

    A commit the base already contains is skipped even when the walk reaches
    it through another merge parent, so it is never replayed as a new write.
    A commit the base lacks is kept even when its own parent is older than
    the base, so its ops and predicates are not lost.
    """
    if tip_id == base_id:
        return []
    contained = repo.graph.ancestors(base_id)
    state: dict[str, int] = {}
    order: list[str] = []
    stack = [tip_id]
    while stack:
        node = stack[-1]
        if node in contained or state.get(node) == 2:
            stack.pop()
            continue
        if state.get(node) == 1:
            stack.pop()
            state[node] = 2
            order.append(node)
            continue
        state[node] = 1
        for parent in repo.snapshots[node].parent_ids:
            if parent not in contained and state.get(parent) != 2:
                stack.append(parent)
    return order


def _apply_commit(parent: Delta, step: Delta, base_rows: dict) -> Delta:
    """Lay one commit onto a delta. A new op on a row closes an open choice.

    ``row_state`` keeps the values a deleted row had when it was deleted, so a
    later check does not read the base snapshot.
    """
    ops = dict(parent.row_ops)
    ops.update(step.row_ops)
    touched = set(step.row_ops)
    vsa = tuple(choice for choice in (*parent.vsa, *step.vsa) if choice["row_id"] not in touched)
    state = dict(parent.row_state)
    for rid, (kind, values, _fid) in step.row_ops.items():
        if kind == "delete":
            if rid in step.row_state:
                state[rid] = dict(step.row_state[rid])
            elif rid not in state and rid in base_rows:
                state[rid] = dict(base_rows[rid])
            continue
        prior = dict(state.get(rid) or base_rows.get(rid) or {})
        prior.update(values or {})
        state[rid] = prior
    return Delta(
        ops,
        parent.changed_files | step.changed_files,
        step.schema,
        parent.predicates + step.predicates,
        {**parent.files, **step.files},
        vsa,
        state,
    )


def _since(tip: Delta, fork: Delta) -> Delta:
    """Drop the prefix both parents already share, so it is not a conflict."""
    ops = {
        rid: op
        for rid, op in tip.row_ops.items()
        if fork.row_ops.get(rid) != op
    }
    fork_preds = set(fork.predicates)
    preds = tuple(pred for pred in tip.predicates if pred not in fork_preds)
    fork_choices = {(choice.get("row_id"), choice.get("column")) for choice in fork.vsa}
    vsa = tuple(
        choice
        for choice in tip.vsa
        if (choice.get("row_id"), choice.get("column")) not in fork_choices
    )
    state = {rid: dict(tip.row_state[rid]) for rid in ops if rid in tip.row_state}
    return Delta(
        ops,
        tip.changed_files - fork.changed_files,
        tip.schema,
        preds,
        {fid: meta for fid, meta in tip.files.items() if fid not in fork.files},
        vsa,
        state,
    )


def _combine_deltas(left: Delta, right: Delta, base_schema: Schema) -> Delta:
    """Combine two branches. Open choices stay open instead of taking one side."""
    ops, choices, _conflicts = resolve_rows(left, right, policy="vsa")
    ops, choices = _carry_open(left, right, ops, choices)
    schema, _error = merge_schemas(base_schema, left.schema, right.schema)
    state = {}
    for rid in ops:
        if rid in left.row_state:
            state[rid] = dict(left.row_state[rid])
        elif rid in right.row_state:
            state[rid] = dict(right.row_state[rid])
    return Delta(
        ops,
        left.changed_files | right.changed_files,
        schema or left.schema,
        left.predicates + right.predicates,
        {**left.files, **right.files},
        tuple(choices),
        state,
    )


def _carry_open(
    left: Delta,
    right: Delta,
    ops: dict[int, tuple],
    choices: list[dict],
) -> tuple[dict[int, tuple], list[dict]]:
    """Add choices left open by an earlier merge, and drop any op that would hide them."""
    ops = dict(ops)
    already = {choice["row_id"] for choice in choices}
    extra: list[dict] = []
    grouped: dict[int, list[dict]] = {}
    for choice in (*left.vsa, *right.vsa):
        if choice["row_id"] in already:
            continue
        grouped.setdefault(choice["row_id"], []).append(choice)
    for rid, group in grouped.items():
        extra.extend(group)
        for source in (left, right):
            if rid not in source.row_ops:
                continue
            op = source.row_ops[rid]
            extra.append(
                {
                    "kind": "choice",
                    "row_id": rid,
                    "column": group[0].get("column", "*"),
                    "options": [{"branch": "carried", "op": op[0], "value": op[1]}],
                }
            )
        ops.pop(rid, None)
    return ops, choices + extra


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
    post_check: bool = True,
) -> MergeResult:
    """Merge two branch tips by writing a delta manifest. Base data files are not rewritten.

    ``post_check=False`` turns off the row-value check of deleted rows, so every
    clause pair the static test cannot rule out counts as a conflict (ablation only).
    """
    store.list("manifests/")
    base_id = repo.graph.lca(left_id, right_id)
    base = repo.snapshots[base_id]
    base_doc = load_json(store.get(base.summary_key))
    base_schema = Schema.from_doc(base_doc["schema"])
    left_delta = load_delta(repo, store, base_id, left_id)
    right_delta = load_delta(repo, store, base_id, right_id)

    violations = check_invariants(
        deleted_ids(left_delta.row_ops),
        left_delta.predicates,
        deleted_ids(right_delta.row_ops),
        right_delta.predicates,
        left_delta.row_ops,
        right_delta.row_ops,
        row_values=base.rows,
        left_state=left_delta.row_state,
        right_state=right_delta.row_state,
        post_check=post_check,
    )
    if violations:
        return MergeResult(
            system="semlake",
            status="semantic_conflict",
            violations=[item.to_doc() for item in violations],
            message=violations[0].detail,
            outcome=_agent_outcome("semantic_conflict"),
        )

    schema, schema_error = merge_schemas(base_schema, left_delta.schema, right_delta.schema)
    if schema_error or schema is None:
        return MergeResult(
            system="semlake",
            status="schema_conflict",
            message=schema_error or "",
            outcome=_agent_outcome("schema_conflict"),
        )

    merged_ops, vsa, conflicts = resolve_rows(left_delta, right_delta, policy="vsa")
    merged_ops, vsa = _carry_open(left_delta, right_delta, merged_ops, vsa)
    conflicts = len(vsa)
    snap_id = repo.new_snapshot_id()
    _commit_merge(
        repo,
        store,
        snap_id,
        repo.snapshots[base_id],
        repo.snapshots[left_id],
        repo.snapshots[right_id],
        left_delta,
        right_delta,
        schema,
        merged_ops,
        vsa,
    )
    status = "vsa" if vsa else "clean"
    return MergeResult(
        system="semlake",
        status=status,
        snapshot_id=snap_id,
        vsa=vsa,
        cell_conflicts=conflicts,
        message="merged without rewriting base files",
        outcome=_agent_outcome(status),
    )


def _put_object(repo: Repository, store: MemoryObjectStore, key: str, data: bytes) -> None:
    """PUT ``data`` on the measured store and keep a copy on the repository store."""
    store.put(key, data)
    if store is not repo.store:
        repo.store.objects[key] = data


def _commit_merge(
    repo: Repository,
    store: MemoryObjectStore,
    snap_id: str,
    base: Snapshot,
    left: Snapshot,
    right: Snapshot,
    left_delta: Delta,
    right_delta: Delta,
    schema: Schema,
    ops: dict[int, tuple],
    vsa: list[dict],
) -> Snapshot:
    """Publish one merge snapshot and register it.

    Base Parquet files stay where they are. Resolved updates are written to one
    new delta file, and each changed base file gains positional deletion-vector
    bits. The manifest list points at the base listing plus that delta, so the
    unchanged files are not written again. A row that is still a VSA choice
    keeps its base version.
    """
    rows = base.rows.fork()
    row_file = dict(base.row_file)
    file_rows = dict(base.file_rows)
    row_pos = dict(base.row_pos)
    files = dict(base.files)
    for source in (left, right):
        for fid, order in source.file_rows.items():
            file_rows.setdefault(fid, order)
        for rid, pos in source.row_pos.items():
            if rid not in row_pos:
                row_pos[rid] = pos

    for rid, (kind, values, _fid) in ops.items():
        if kind == "delete":
            rows.pop(rid, None)
        elif kind == "insert":
            rows[rid] = dict(values)
        elif kind == "update" and rid in rows:
            rows[rid] = {**rows[rid], **values}

    hidden: dict[str, list[int]] = {}
    for rid, (kind, _values, _fid) in ops.items():
        if kind not in ("update", "delete") or rid not in base.row_file:
            continue
        src = base.row_file[rid]
        hidden.setdefault(src, []).append(base.row_pos[rid])
    for fid, positions in hidden.items():
        meta = files[fid]
        files[fid] = replace(
            meta,
            token=f"merged-{snap_id}-{fid}",
            dv_bytes=with_positions(meta.dv_bytes, positions),
        )

    for rid, (kind, _values, fid) in list(ops.items()):
        if kind != "insert":
            continue
        meta = left_delta.files.get(fid) or right_delta.files.get(fid)
        if meta is None:
            continue
        order = file_rows.get(fid, ())
        extra = [pos for pos, row_id in enumerate(order) if row_id not in rows]
        if extra:
            meta = replace(meta, token=f"merged-{snap_id}-{fid}", dv_bytes=with_positions(meta.dv_bytes, extra))
        files[fid] = meta
        row_file[rid] = fid

    updated = sorted(rid for rid, (kind, _values, _fid) in ops.items() if kind == "update")
    if updated:
        delta_id = f"delta-{snap_id}"
        payload = [{"__row_id": rid, **rows[rid]} for rid in updated]
        blob = write_parquet(payload, schema)
        key = f"data/{snap_id}/{delta_id}.parquet"
        _put_object(repo, store, key, blob)
        files[delta_id] = FileMeta(delta_id, key, len(blob), f"delta-{snap_id}", empty_dv_bytes())
        file_rows[delta_id] = tuple(updated)
        for offset, rid in enumerate(updated):
            row_file[rid] = delta_id
            row_pos[rid] = offset
            _kind, values, _fid = ops[rid]
            ops[rid] = ("update", values, delta_id)

    changed = tuple(
        fid for fid in sorted(files) if fid not in base.files or files[fid] != base.files[fid]
    )
    snap = Snapshot(
        snapshot_id=snap_id,
        parent_ids=(left.snapshot_id, right.snapshot_id),
        branch="merge",
        schema=schema,
        rows=rows,
        row_file=row_file,
        files=files,
        row_ops=dict(ops),
        changed_files=changed,
        file_rows=file_rows,
        row_pos=row_pos,
        message="merge",
    )
    _publish_merge(repo, store, snap, base, vsa, left_delta.predicates + right_delta.predicates)
    repo.adopt(snap)
    return snap


def _publish_merge(
    repo: Repository,
    store: MemoryObjectStore,
    snap: Snapshot,
    base: Snapshot,
    vsa: list[dict],
    predicates: tuple[Predicate, ...],
) -> None:
    """Write the merge summary, the changed-file manifest, and a manifest list."""
    snap.summary_key = f"manifests/{snap.snapshot_id}.summary.json"
    delta_key = f"manifests/{snap.snapshot_id}.delta.json"
    snap.manifest_key = f"manifests/{snap.snapshot_id}.files.json"
    summary = {
        "snapshot_id": snap.snapshot_id,
        "base": base.snapshot_id,
        "parents": list(snap.parent_ids),
        "branch": snap.branch,
        "message": snap.message,
        "schema": snap.schema.to_doc(),
        "changed_files": list(snap.changed_files),
        "row_ops": {
            str(rid): {"op": kind, "values": values, "file": fid}
            for rid, (kind, values, fid) in snap.row_ops.items()
        },
        "predicates": [pred.to_doc() for pred in predicates],
        "files": {fid: file_to_doc(snap.files[fid]) for fid in snap.changed_files},
        "vsa": vsa,
    }
    delta = {fid: file_to_doc(snap.files[fid]) for fid in snap.changed_files}
    listing = {"manifests": [base.manifest_key, delta_key]}
    _put_object(repo, store, snap.summary_key, dump_json(summary))
    _put_object(repo, store, delta_key, dump_json(delta))
    _put_object(repo, store, snap.manifest_key, dump_json(listing))
