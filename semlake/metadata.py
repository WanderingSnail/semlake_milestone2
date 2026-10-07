"""In-memory commit DAG, schemas, and snapshot commits."""

from __future__ import annotations

from dataclasses import dataclass, replace
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


@dataclass
class Snapshot:
    snapshot_id: str
    parent_ids: tuple[str, ...]
    branch: str
    schema: Schema
    rows: dict[int, dict[str, Any]]
    row_file: dict[int, str]
    files: dict[str, FileMeta]
    row_ops: dict[int, tuple]
    changed_files: tuple[str, ...]
    predicates: tuple[Predicate, ...] = ()
    summary_key: str = ""
    manifest_key: str = ""
    message: str = ""


class CommitGraph:
    """Parent pointers for every snapshot. The LCA is the deepest shared snapshot."""

    def __init__(self) -> None:
        self.parents: dict[str, tuple[str, ...]] = {}
        self.depth: dict[str, int] = {}

    def add(self, snap_id: str, parent_ids: Iterable[str]) -> None:
        parents = tuple(parent_ids)
        self.parents[snap_id] = parents
        if not parents:
            self.depth[snap_id] = 0
            return
        missing = [parent for parent in parents if parent not in self.depth]
        if missing:
            raise KeyError(f"unknown parent snapshots: {missing}")
        self.depth[snap_id] = max(self.depth[parent] for parent in parents) + 1

    def ancestors(self, snap_id: str) -> dict[str, int]:
        """Return every ancestor of ``snap_id``, itself included, with its depth."""
        found: dict[str, int] = {}
        stack = [snap_id]
        while stack:
            node = stack.pop()
            if node in found:
                continue
            if node not in self.parents:
                raise KeyError(snap_id)
            found[node] = self.depth[node]
            stack.extend(self.parents[node])
        return found

    def lca(self, left_id: str, right_id: str) -> str:
        left_ancestors = self.ancestors(left_id)
        best_id = ""
        best_depth = -1
        stack = [right_id]
        seen: set[str] = set()
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            if node in left_ancestors:
                depth = left_ancestors[node]
                # Ties go to the smaller id so the result does not depend on walk order.
                if depth > best_depth or (depth == best_depth and node < best_id):
                    best_depth = depth
                    best_id = node
            stack.extend(self.parents.get(node, ()))
        if not best_id:
            raise ValueError(f"no common ancestor for {left_id} and {right_id}")
        return best_id

    def path_to(self, tip_id: str, ancestor_id: str) -> list[str]:
        """Return snapshot ids from ``tip_id`` back to ``ancestor_id``, excluding the ancestor."""
        path: list[str] = []
        node = tip_id
        seen: set[str] = set()
        while node != ancestor_id:
            if node in seen:
                raise ValueError("cycle while walking toward ancestor")
            seen.add(node)
            path.append(node)
            parents = self.parents[node]
            if not parents:
                raise ValueError(f"{ancestor_id} is not an ancestor of {tip_id}")
            if len(parents) == 1:
                node = parents[0]
                continue
            next_node = None
            for parent in parents:
                if parent == ancestor_id or ancestor_id in self.ancestors(parent):
                    next_node = parent
                    break
            if next_node is None:
                raise ValueError(f"{ancestor_id} is not an ancestor of {tip_id}")
            node = next_node
        return path


class Repository:
    """Snapshot store for one table. Each new snapshot is published to the object store.

    semlake.storage imports this module, so its helpers are imported inside
    the methods that need them.
    """

    def __init__(self, store: Any) -> None:
        self.store = store
        self.graph = CommitGraph()
        self.snapshots: dict[str, Snapshot] = {}
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

        rows = dict(rows or {})
        snap_id = self.new_snapshot_id()
        files: dict[str, FileMeta] = {}
        row_file: dict[int, str] = {}
        empty_dv = empty_dv_bytes()

        if rows:
            ordered = sorted(rows)
            step = max(1, rows_per_file)
            groups = [ordered[i : i + step] for i in range(0, len(ordered), step)]
            for index, group in enumerate(groups):
                fid = f"file-{index:05d}"
                payload = [{"__row_id": rid, **rows[rid]} for rid in group]
                blob = write_parquet(payload, schema)
                key = f"data/{snap_id}/{fid}.parquet"
                self.store.put(key, blob)
                files[fid] = FileMeta(fid, key, len(blob), "base", empty_dv)
                for rid in group:
                    row_file[rid] = fid
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
            rows=rows,
            row_file=row_file,
            files=files,
            row_ops={},
            changed_files=(),
            message="create table",
        )
        self._register(snap)
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
        """Commit row changes on top of ``parent``. Inserts go to one new file."""
        from semlake.storage import empty_dv_bytes, write_parquet

        updates = updates or {}
        inserts = inserts or {}
        snap_id = self.new_snapshot_id()
        rows = dict(parent.rows)
        row_file = dict(parent.row_file)
        files = dict(parent.files)
        ops: dict[int, tuple] = {}
        touched: dict[str, list[int]] = {}

        for rid, cols in updates.items():
            fid = row_file[rid]
            rows[rid] = {**rows[rid], **cols}
            ops[rid] = ("update", dict(cols), fid)
            touched.setdefault(fid, []).append(rid)

        for rid in deletes or ():
            fid = row_file[rid]
            rows.pop(rid, None)
            ops[rid] = ("delete", {}, fid)
            touched.setdefault(fid, []).append(rid)

        if inserts:
            fid = f"file-{snap_id}"
            payload = [{"__row_id": rid, **cols} for rid, cols in sorted(inserts.items())]
            blob = write_parquet(payload, schema or parent.schema)
            key = f"data/{snap_id}/{fid}.parquet"
            self.store.put(key, blob)
            files[fid] = FileMeta(fid, key, len(blob), f"append-{snap_id}", empty_dv_bytes())
            for rid, cols in inserts.items():
                rows[rid] = dict(cols)
                row_file[rid] = fid
                ops[rid] = ("insert", dict(cols), fid)

        for fid, positions in touched.items():
            files[fid] = _mark_rows_replaced(files[fid], snap_id, positions)

        changed = set(touched)
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
        changed = tuple(sorted(file_ids))
        for fid in changed:
            files[fid] = _mark_rows_replaced(files[fid], snap_id, [0])
        snap = Snapshot(
            snapshot_id=snap_id,
            parent_ids=(parent.snapshot_id,),
            branch=branch,
            schema=parent.schema,
            rows=dict(parent.rows),
            row_file=dict(parent.row_file),
            files=files,
            row_ops={},
            changed_files=changed,
            message=message,
        )
        self._register(snap)
        return snap

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
