"""In-memory commit DAG, schemas, and snapshot commits."""

from __future__ import annotations

import heapq
from collections.abc import MutableMapping
from dataclasses import dataclass, field, replace
from typing import Any, Iterable

from semlake.verification import Predicate


@dataclass(frozen=True)
class SchemaField:
    name: str
    type_name: str
    nullable: bool = True


@dataclass(frozen=True)
class Schema:
    fields: tuple[SchemaField, ...]

    def names(self) -> list[str]:
        return [item.name for item in self.fields]

    def field_map(self) -> dict[str, SchemaField]:
        return {item.name: item for item in self.fields}

    def to_doc(self) -> list[dict]:
        return [
            {"name": item.name, "type": item.type_name, "nullable": item.nullable}
            for item in self.fields
        ]

    @classmethod
    def from_doc(cls, doc: list[dict]) -> Schema:
        return cls(
            tuple(
                SchemaField(item["name"], item["type"], item.get("nullable", True))
                for item in doc
            )
        )


# Type changes that readers of the old type can still decode.
WIDENING = {
    ("int32", "int64"),
    ("int32", "double"),
    ("int64", "double"),
    ("float", "double"),
}


def _is_compatible(left: SchemaField, right: SchemaField) -> bool:
    if left.type_name == right.type_name:
        return True
    pair = (left.type_name, right.type_name)
    return pair in WIDENING or (pair[1], pair[0]) in WIDENING


def _wider_field(left: SchemaField, right: SchemaField) -> SchemaField:
    if left.type_name == right.type_name or (right.type_name, left.type_name) in WIDENING:
        return left
    return right


def merge_schemas(base: Schema, left: Schema, right: Schema) -> tuple[Schema | None, str | None]:
    """Union the columns added on either branch. Drops and incompatible types fail."""
    base_map = base.field_map()
    left_map = left.field_map()
    right_map = right.field_map()
    names = list(base.names())
    for name in left.names() + right.names():
        if name not in names:
            names.append(name)

    merged: list[SchemaField] = []
    for name in names:
        in_base = base_map.get(name)
        in_left = left_map.get(name)
        in_right = right_map.get(name)
        if in_base is not None and (in_left is None or in_right is None):
            return None, f"column drop is not a zero-copy merge: {name}"
        if in_left is not None and in_right is not None:
            if not _is_compatible(in_left, in_right):
                return None, (
                    f"incompatible types for {name}: {in_left.type_name} vs {in_right.type_name}"
                )
            merged.append(_wider_field(in_left, in_right))
        else:
            merged.append(in_left or in_right)
    return Schema(tuple(merged)), None


@dataclass(frozen=True)
class FileMeta:
    file_id: str
    object_key: str
    size_bytes: int
    token: str
    dv_bytes: bytes = b""


class RowView(MutableMapping):
    """Row map that shares unchanged rows with its parent.

    A commit stores only the rows it wrote and the ids it deleted. Reading a
    row walks the parent chain. The parent map is not copied.
    """

    def __init__(self, parent: RowView | None = None, initial: dict[int, dict] | None = None) -> None:
        self.parent = parent
        self.overlay: dict[int, dict] = dict(initial or {})
        self.removed: set[int] = set()

    def fork(self) -> RowView:
        """Return a child view. Writes on the child do not change this view."""
        return RowView(self)

    def __getitem__(self, rid: int) -> dict:
        node: RowView | None = self
        while node is not None:
            if rid in node.removed:
                raise KeyError(rid)
            if rid in node.overlay:
                return node.overlay[rid]
            node = node.parent
        raise KeyError(rid)

    def __setitem__(self, rid: int, row: dict) -> None:
        self.removed.discard(rid)
        self.overlay[rid] = row

    def __delitem__(self, rid: int) -> None:
        if rid not in self:
            raise KeyError(rid)
        self.overlay.pop(rid, None)
        self.removed.add(rid)

    def __iter__(self):
        hidden: set[int] = set()
        seen: set[int] = set()
        node: RowView | None = self
        while node is not None:
            hidden.update(node.removed)
            for rid in node.overlay:
                if rid in seen or rid in hidden:
                    continue
                seen.add(rid)
                yield rid
            node = node.parent

    def __len__(self) -> int:
        return sum(1 for _ in self)


@dataclass
class Snapshot:
    snapshot_id: str
    parent_ids: tuple[str, ...]
    branch: str
    schema: Schema
    rows: RowView
    row_file: dict[int, str]
    files: dict[str, FileMeta]
    row_ops: dict[int, tuple]
    changed_files: tuple[str, ...]
    file_rows: dict[str, tuple[int, ...]]
    predicates: tuple[Predicate, ...] = ()
    summary_key: str = ""
    manifest_key: str = ""
    message: str = ""
    row_pos: dict[int, int] = field(default_factory=dict)


class CommitGraph:
    """Parent pointers for every snapshot. The LCA is the deepest shared snapshot.

    ``generation`` is 0 for a root and one more than the highest parent
    generation otherwise. The LCA walk stops once every open snapshot is
    older than the best candidate, so a recent fork does not scan the
    whole history.
    """

    def __init__(self) -> None:
        self.parents: dict[str, tuple[str, ...]] = {}
        self.generation: dict[str, int] = {}
        self._ancestors: dict[str, dict[str, int]] = {}

    def add(self, snap_id: str, parent_ids: Iterable[str]) -> None:
        parents = tuple(parent_ids)
        self.parents[snap_id] = parents
        if not parents:
            self.generation[snap_id] = 0
            self._ancestors[snap_id] = {snap_id: 0}
            return
        missing = [parent for parent in parents if parent not in self.generation]
        if missing:
            raise KeyError(f"unknown parent snapshots: {missing}")
        self.generation[snap_id] = max(self.generation[parent] for parent in parents) + 1
        found = {snap_id: self.generation[snap_id]}
        for parent in parents:
            found.update(self._ancestors[parent])
        self._ancestors[snap_id] = found

    def ancestors(self, snap_id: str) -> dict[str, int]:
        """Return every ancestor of ``snap_id``, itself included, with its generation.

        The map is stored when the snapshot is added. A later commit does not
        change it. Callers should not modify the returned dict.
        """
        try:
            return self._ancestors[snap_id]
        except KeyError:
            raise KeyError(snap_id) from None

    def lca(self, left_id: str, right_id: str) -> str:
        if left_id == right_id:
            return left_id
        if left_id not in self.parents or right_id not in self.parents:
            raise KeyError(left_id if left_id not in self.parents else right_id)
        seen = [set(), set()]
        heap: list[tuple[int, str, int]] = []

        def push(node: str, side: int) -> None:
            if node in seen[side]:
                return
            seen[side].add(node)
            heapq.heappush(heap, (-self.generation[node], node, side))

        push(left_id, 0)
        push(right_id, 1)
        best_id = ""
        best_gen = -1
        while heap:
            gen = -heap[0][0]
            if best_id and gen < best_gen:
                break
            _neg, node, side = heapq.heappop(heap)
            # Ties go to the smaller id so the result does not depend on walk order.
            if node in seen[1 - side] and (gen > best_gen or (gen == best_gen and node < best_id)):
                best_gen = gen
                best_id = node
            for parent in self.parents[node]:
                push(parent, side)
        if not best_id:
            raise ValueError(f"no common ancestor for {left_id} and {right_id}")
        return best_id


class Repository:
    """Snapshot store for one table. Each new snapshot is published to the object store.

    semlake.storage imports this module, so its helpers are imported inside
    the methods that need them.
    """

    def __init__(self, store: Any) -> None:
        self.store = store
        self.graph = CommitGraph()
        self.snapshots: dict[str, Snapshot] = {}
        self.heads: dict[str, str] = {}
        self._seq = 0

    def new_snapshot_id(self) -> str:
        self._seq += 1
        return f"snap-{self._seq:05d}"

    def create_table(
        self,
        branch: str,
        schema: Schema,
        rows: dict[int, dict] | None = None,
        n_files: int = 0,
        rows_per_file: int = 1024,
        file_size: int = 4096,
    ) -> Snapshot:
        """Create the root snapshot from ``rows``, or from ``n_files`` placeholder files."""
        from semlake.storage import empty_dv_bytes, write_parquet

        source = {rid: dict(row) for rid, row in (rows or {}).items()}
        snap_id = self.new_snapshot_id()
        files: dict[str, FileMeta] = {}
        row_file: dict[int, str] = {}
        file_rows: dict[str, tuple[int, ...]] = {}
        row_pos: dict[int, int] = {}
        empty_dv = empty_dv_bytes()

        if source:
            ordered = sorted(source)
            step = max(1, rows_per_file)
            groups = [ordered[i : i + step] for i in range(0, len(ordered), step)]
            for index, group in enumerate(groups):
                fid = f"file-{index:05d}"
                payload = [{"__row_id": rid, **source[rid]} for rid in group]
                blob = write_parquet(payload, schema)
                key = f"data/{snap_id}/{fid}.parquet"
                self.store.put(key, blob)
                files[fid] = FileMeta(fid, key, len(blob), "base", empty_dv)
                file_rows[fid] = tuple(group)
                for offset, rid in enumerate(group):
                    row_file[rid] = fid
                    row_pos[rid] = offset
        else:
            # All files share one buffer. Each PUT is still billed at the full
            # size, but memory stays flat at 10K files.
            shared = bytes(file_size)
            for index in range(n_files):
                fid = f"file-{index:05d}"
                key = f"data/{snap_id}/{fid}.bin"
                self.store.put(key, shared)
                files[fid] = FileMeta(fid, key, file_size, "base", empty_dv)

        snap = Snapshot(
            snapshot_id=snap_id,
            parent_ids=(),
            branch=branch,
            schema=schema,
            rows=RowView(initial=source),
            row_file=row_file,
            files=files,
            row_ops={},
            changed_files=(),
            file_rows=file_rows,
            row_pos=row_pos,
            message="create table",
        )
        self._register(snap)
        self.heads[branch] = snap.snapshot_id
        return snap

    def apply(
        self,
        parent: Snapshot,
        branch: str,
        updates: dict[int, dict] | None = None,
        deletes: Iterable[int] | None = None,
        inserts: dict[int, dict] | None = None,
        schema: Schema | None = None,
        predicates: Iterable[Predicate] = (),
        message: str = "",
    ) -> Snapshot:
        """Commit row changes on top of ``parent``.

        Each update is appended to one new delta file and its old position is
        marked in the source file's deletion vector. Inserts go to one new file.
        """
        from semlake.storage import empty_dv_bytes, write_parquet

        updates = updates or {}
        inserts = inserts or {}
        snap_id = self.new_snapshot_id()
        rows = parent.rows.fork()
        row_file = dict(parent.row_file)
        files = dict(parent.files)
        file_rows = dict(parent.file_rows)
        row_pos = dict(parent.row_pos)
        current = schema or parent.schema
        ops: dict[int, tuple] = {}
        touched: dict[str, list[int]] = {}

        for rid, cols in updates.items():
            fid = row_file[rid]
            rows[rid] = {**rows[rid], **cols}
            ops[rid] = ("update", dict(cols), fid)
            touched.setdefault(fid, []).append(row_pos[rid])

        for rid in deletes or ():
            fid = row_file[rid]
            rows.pop(rid, None)
            ops[rid] = ("delete", {}, fid)
            touched.setdefault(fid, []).append(row_pos[rid])

        if updates:
            delta_id = f"delta-{snap_id}"
            ordered = tuple(sorted(updates))
            payload = [{"__row_id": rid, **rows[rid]} for rid in ordered]
            blob = write_parquet(payload, current)
            key = f"data/{snap_id}/{delta_id}.parquet"
            self.store.put(key, blob)
            files[delta_id] = FileMeta(delta_id, key, len(blob), f"delta-{snap_id}", empty_dv_bytes())
            file_rows[delta_id] = ordered
            for offset, rid in enumerate(ordered):
                row_file[rid] = delta_id
                row_pos[rid] = offset
                _kind, values, _fid = ops[rid]
                ops[rid] = ("update", values, delta_id)

        if inserts:
            fid = f"file-{snap_id}"
            payload = [{"__row_id": rid, **cols} for rid, cols in sorted(inserts.items())]
            blob = write_parquet(payload, schema or parent.schema)
            key = f"data/{snap_id}/{fid}.parquet"
            self.store.put(key, blob)
            files[fid] = FileMeta(fid, key, len(blob), f"append-{snap_id}", empty_dv_bytes())
            ordered_inserts = tuple(sorted(inserts))
            file_rows[fid] = ordered_inserts
            for offset, rid in enumerate(ordered_inserts):
                cols = inserts[rid]
                rows[rid] = dict(cols)
                row_file[rid] = fid
                row_pos[rid] = offset
                ops[rid] = ("insert", dict(cols), fid)

        for fid, positions in touched.items():
            files[fid] = _mark_rows_replaced(files[fid], snap_id, positions)

        changed = set(touched)
        if updates:
            changed.add(f"delta-{snap_id}")
        if inserts:
            changed.add(f"file-{snap_id}")
        snap = Snapshot(
            snapshot_id=snap_id,
            parent_ids=(parent.snapshot_id,),
            branch=branch,
            schema=schema or parent.schema,
            rows=rows,
            row_file=row_file,
            files=files,
            row_ops=ops,
            changed_files=tuple(sorted(changed)),
            file_rows=file_rows,
            row_pos=row_pos,
            predicates=tuple(predicates),
            message=message,
        )
        self._register(snap)
        return snap

    def mark_files_changed(
        self,
        parent: Snapshot,
        branch: str,
        file_ids: Iterable[str],
        message: str = "",
    ) -> Snapshot:
        """Mark row 0 of each file as replaced. Used for tables without decoded rows."""
        snap_id = self.new_snapshot_id()
        files = dict(parent.files)
        file_rows = dict(parent.file_rows)
        row_pos = dict(parent.row_pos)
        changed = tuple(sorted(file_ids))
        for fid in changed:
            files[fid] = _mark_rows_replaced(files[fid], snap_id, [0])
        snap = Snapshot(
            snapshot_id=snap_id,
            parent_ids=(parent.snapshot_id,),
            branch=branch,
            schema=parent.schema,
            rows=parent.rows.fork(),
            row_file=dict(parent.row_file),
            files=files,
            row_ops={},
            changed_files=changed,
            file_rows=file_rows,
            row_pos=row_pos,
            message=message,
        )
        self._register(snap)
        return snap

    def cas_head(self, branch: str, expected: str, new: str) -> bool:
        """Move ``branch`` from ``expected`` to ``new``. Return whether this caller won."""
        if self.heads.get(branch) != expected:
            return False
        self.heads[branch] = new
        return True

    def publish(self, snap: Snapshot) -> None:
        """PUT the snapshot's delta summary and its full file listing."""
        from semlake.storage import dump_json, file_to_doc

        snap.summary_key = f"manifests/{snap.snapshot_id}.summary.json"
        snap.manifest_key = f"manifests/{snap.snapshot_id}.files.json"
        summary = {
            "snapshot_id": snap.snapshot_id,
            "parents": list(snap.parent_ids),
            "branch": snap.branch,
            "message": snap.message,
            "schema": snap.schema.to_doc(),
            "changed_files": list(snap.changed_files),
            "row_ops": {
                str(rid): {"op": kind, "values": values, "file": fid}
                for rid, (kind, values, fid) in snap.row_ops.items()
            },
            "predicates": [pred.to_doc() for pred in snap.predicates],
            "files": {
                fid: file_to_doc(snap.files[fid])
                for fid in snap.changed_files
                if fid in snap.files
            },
        }
        listing = {fid: file_to_doc(meta) for fid, meta in snap.files.items()}
        self.store.put(snap.summary_key, dump_json(summary))
        self.store.put(snap.manifest_key, dump_json(listing))

    def adopt(self, snap: Snapshot) -> None:
        """Record a snapshot whose objects were already written."""
        self.graph.add(snap.snapshot_id, snap.parent_ids)
        self.snapshots[snap.snapshot_id] = snap

    def _register(self, snap: Snapshot) -> None:
        self.publish(snap)
        self.graph.add(snap.snapshot_id, snap.parent_ids)
        self.snapshots[snap.snapshot_id] = snap




def _mark_rows_replaced(meta: FileMeta, snap_id: str, positions: Iterable[int]) -> FileMeta:
    """Return the file with a new token and ``positions`` added to its deletion vector."""
    from semlake.storage import with_positions

    return replace(
        meta,
        token=f"edit-{snap_id}-{meta.file_id}",
        dv_bytes=with_positions(meta.dv_bytes, positions),
    )
