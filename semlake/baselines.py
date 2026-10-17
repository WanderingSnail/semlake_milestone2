"""Comparison baselines from the proposal.

B1 file-level OCC, B2 copy-on-write, B3 tuple-version dictionary, B4 CRDT op log.
Every baseline takes the same arguments as ``three_way_merge`` and returns a
``MergeResult``. B1 raises ``WriteConflictException`` when it aborts.
"""

from __future__ import annotations

from semlake.merge import MergeResult, load_delta, resolve_rows
from semlake.metadata import FileMeta, Repository, Schema, Snapshot, merge_schemas
from semlake.storage import (
    MemoryObjectStore,
    dump_json,
    file_from_doc,
    file_to_doc,
    load_json,
    read_parquet,
    write_parquet,
)


class WriteConflictException(Exception):
    """Two branches changed the same data file."""

    def __init__(self, files: set[str]) -> None:
        self.files = frozenset(files)
        super().__init__(f"write conflict on {len(files)} file(s)")


def file_level_occ(
    repo: Repository,
    left_id: str,
    right_id: str,
    store: MemoryObjectStore,
) -> MergeResult:
    """B1: Abort when both branches changed the same file. Semantics are not checked.

    The changed set comes from a scan of each snapshot's full file list, which
    is how Iceberg- and Delta-style OCC finds it.
    """
    store.list("manifests/")
    base_id = repo.graph.lca(left_id, right_id)
    base_files = _load_listing(store, repo.snapshots[base_id].manifest_key)
    left_files = _load_listing(store, repo.snapshots[left_id].manifest_key)
    right_files = _load_listing(store, repo.snapshots[right_id].manifest_key)
    overlap = _changed_files(base_files, left_files) & _changed_files(base_files, right_files)
    if overlap:
        raise WriteConflictException(overlap)

    # The changed files are disjoint, so the two listings are combined as they
    # are. B1 never looks at row contents, which is why it misses W3.
    merged = _union_listings(base_files, right_files, left_files)
    snap_id = repo.new_snapshot_id()
    manifest_key = f"manifests/{snap_id}.files.json"
    body = dump_json({fid: file_to_doc(meta) for fid, meta in merged.items()})
    store.put(manifest_key, body)
    if store is not repo.store:
        repo.store.objects[manifest_key] = body
    schema = _wider_schema(repo, left_id, right_id)
    _remember(repo, snap_id, left_id, right_id, schema, merged, manifest_key)
    return MergeResult(
        system="b1",
        status="clean",
        snapshot_id=snap_id,
        message="file sets are disjoint",
    )


def copy_on_write(
    repo: Repository,
    left_id: str,
    right_id: str,
    store: MemoryObjectStore,
) -> MergeResult:
    """B2: Merge row by row, then rewrite every changed data file.

    Same-cell conflicts go to the right branch (last writer wins). There is no
    predicate check and no VSA node. Adding a nullable column only changes the
    schema, so files whose bytes did not change are not rewritten.
    """
    store.list("manifests/")
    base_id = repo.graph.lca(left_id, right_id)
    base_snap = repo.snapshots[base_id]
    base_files = _load_listing(store, base_snap.manifest_key)
    left_files = _load_listing(store, repo.snapshots[left_id].manifest_key)
    right_files = _load_listing(store, repo.snapshots[right_id].manifest_key)

    left_delta = load_delta(repo, store, base_id, left_id)
    right_delta = load_delta(repo, store, base_id, right_id)
    schema, schema_error = merge_schemas(base_snap.schema, left_delta.schema, right_delta.schema)
    if schema_error or schema is None:
        return MergeResult(system="b2", status="schema_conflict", message=schema_error or "")

    merged_ops, _choices, conflicts = resolve_rows(left_delta, right_delta, policy="lww")
    changed = _changed_files(base_files, left_files) | _changed_files(base_files, right_files)

    all_files = _union_listings(base_files, left_files, right_files)
    rows, row_file = _load_rows_to_rewrite(store, changed, base_files, all_files)
    _apply_ops(rows, row_file, merged_ops)

    snap_id = repo.new_snapshot_id()
    written = _put_rewritten_files(store, snap_id, changed, rows, row_file, schema, all_files)
    _remember(repo, snap_id, left_id, right_id, schema, None, "")
    return MergeResult(
        system="b2",
        status="clean",
        snapshot_id=snap_id,
        cell_conflicts=conflicts,
        message=f"rewrote {written} file(s)",
    )


def tuple_version_merge(
    repo: Repository,
    left_id: str,
    right_id: str,
    store: MemoryObjectStore,
) -> MergeResult:
    """B3: Rebuild a tuple-version dictionary. Predicates are not checked.

    Decibel-style versioning walks every tuple, or every file when the table
    has no decoded rows, and writes a new version record for each. Same-row
    updates go to the right branch. Two branches in one file do not abort.
    """
    store.list("manifests/")
    base_id = repo.graph.lca(left_id, right_id)
    base_snap = repo.snapshots[base_id]
    base_files = _load_listing(store, base_snap.manifest_key)
    left_files = _load_listing(store, repo.snapshots[left_id].manifest_key)
    right_files = _load_listing(store, repo.snapshots[right_id].manifest_key)
    left_delta = load_delta(repo, store, base_id, left_id)
    right_delta = load_delta(repo, store, base_id, right_id)
    schema, schema_error = merge_schemas(base_snap.schema, left_delta.schema, right_delta.schema)
    if schema_error or schema is None:
        return MergeResult(system="b3", status="schema_conflict", message=schema_error or "")

    merged_ops, _choices, conflicts = resolve_rows(left_delta, right_delta, policy="lww")
    rows, row_file = _load_all_base_rows(store, base_files)
    _apply_ops(rows, row_file, merged_ops)

    snap_id = repo.new_snapshot_id()
    all_files = _union_listings(base_files, left_files, right_files)
    written = _put_version_records(store, snap_id, rows, row_file, all_files)
    _remember(repo, snap_id, left_id, right_id, schema, None, "")
    return MergeResult(
        system="b3",
        status="clean",
        snapshot_id=snap_id,
        cell_conflicts=conflicts,
        message=f"rebuilt {written} tuple-version records",
    )


def crdt_merge(
    repo: Repository,
    left_id: str,
    right_id: str,
    store: MemoryObjectStore,
) -> MergeResult:
    """B4: Append one converged op-log entry. Conflicts are overwritten.

    There is no branch checkpoint and no predicate check, so the merge always
    commits. Cell conflicts are counted and then resolved for the right branch.
    """
    store.list("manifests/")
    base_id = repo.graph.lca(left_id, right_id)
    base_schema = Schema.from_doc(
        load_json(store.get(repo.snapshots[base_id].summary_key))["schema"]
    )
    left_delta = load_delta(repo, store, base_id, left_id)
    right_delta = load_delta(repo, store, base_id, right_id)
    schema, schema_error = merge_schemas(base_schema, left_delta.schema, right_delta.schema)
    if schema_error or schema is None:
        return MergeResult(system="b4", status="schema_conflict", message=schema_error or "")

    merged_ops, _choices, conflicts = resolve_rows(left_delta, right_delta, policy="lww")
    snap_id = repo.new_snapshot_id()
    store.put(
        f"oplog/{snap_id}.json",
        dump_json(
            {
                "policy": "lww",
                "schema": schema.to_doc(),
                "cell_conflicts_overwritten": conflicts,
                "row_ops": {
                    str(rid): {"op": kind, "values": values, "file": fid}
                    for rid, (kind, values, fid) in merged_ops.items()
                },
            }
        ),
    )
    _remember(repo, snap_id, left_id, right_id, schema, None, "")
    return MergeResult(
        system="b4",
        status="clean",
        snapshot_id=snap_id,
        cell_conflicts=conflicts,
        message="last writer wins; open conflicts were overwritten",
    )


def _load_listing(store: MemoryObjectStore, key: str) -> dict[str, FileMeta]:
    """Load a file listing. A manifest list is read by following its child manifests."""
    payload = load_json(store.get(key))
    children = payload.get("manifests") if isinstance(payload, dict) else None
    if isinstance(children, list):
        merged: dict[str, FileMeta] = {}
        for child in children:
            merged.update(_load_listing(store, child))
        return merged
    return {fid: file_from_doc(fid, doc) for fid, doc in payload.items()}


def _wider_schema(repo: Repository, left_id: str, right_id: str) -> Schema:
    left = repo.snapshots[left_id].schema
    right = repo.snapshots[right_id].schema
    return left if len(left.fields) >= len(right.fields) else right


def _overlay(base_rows: dict, branch_rows: dict, into: dict) -> None:
    """Copy rows that ``branch_rows`` changed relative to ``base_rows`` into ``into``."""
    for rid, row in branch_rows.items():
        if rid not in base_rows or row != base_rows[rid]:
            into[rid] = dict(row)
    for rid in base_rows:
        if rid not in branch_rows:
            into.pop(rid, None)


def _remember(repo, snap_id, left_id, right_id, schema, files, manifest_key: str) -> None:
    """Register a baseline result so later commits can name its snapshot."""
    base = repo.snapshots[repo.graph.lca(left_id, right_id)]
    left = repo.snapshots[left_id]
    right = repo.snapshots[right_id]
    rows = base.rows.fork()
    _overlay(base.rows, left.rows, rows)
    _overlay(base.rows, right.rows, rows)
    row_file = dict(base.row_file)
    file_rows = dict(base.file_rows)
    row_pos = dict(base.row_pos)
    for snap in (left, right):
        for rid, fid in snap.row_file.items():
            if rid in rows:
                row_file[rid] = fid
                if rid in snap.row_pos:
                    row_pos[rid] = snap.row_pos[rid]
        for fid, order in snap.file_rows.items():
            file_rows.setdefault(fid, order)
    repo.adopt(
        Snapshot(
            snapshot_id=snap_id,
            parent_ids=(left_id, right_id),
            branch="merge",
            schema=schema,
            rows=rows,
            row_file=row_file,
            files=dict(base.files) if files is None else files,
            row_ops={},
            changed_files=(),
            file_rows=file_rows,
            row_pos=row_pos,
            manifest_key=manifest_key,
            message="baseline merge",
        )
    )


def _union_listings(*listings: dict[str, FileMeta]) -> dict[str, FileMeta]:
    """Return the union of file listings. Later listings win on the same file id."""
    merged: dict[str, FileMeta] = {}
    for listing in listings:
        merged.update(listing)
    return merged


def _changed_files(base_files: dict[str, FileMeta], tip_files: dict[str, FileMeta]) -> set[str]:
    """Return file ids that were added, removed, or given a new token since the base."""
    changed = set()
    for fid, meta in tip_files.items():
        before = base_files.get(fid)
        if before is None or before.token != meta.token:
            changed.add(fid)
    for fid in base_files:
        if fid not in tip_files:
            changed.add(fid)
    return changed


def _apply_ops(rows: dict[int, dict], row_file: dict[int, str], ops: dict[int, tuple]) -> None:
    for rid, (kind, values, fid) in ops.items():
        if kind == "delete":
            rows.pop(rid, None)
            row_file.pop(rid, None)
        elif kind == "insert":
            rows[rid] = dict(values)
            row_file[rid] = fid
        elif rid in rows:
            rows[rid] = {**rows[rid], **values}


def _decode_rows(blob: bytes, fid: str, rows: dict[int, dict], row_file: dict[int, str]) -> None:
    for record in read_parquet(blob):
        rid = int(record.pop("__row_id"))
        rows[rid] = record
        row_file[rid] = fid


def _load_all_base_rows(store: MemoryObjectStore, base_files: dict[str, FileMeta]):
    """GET every base file and decode the ones that are Parquet."""
    rows: dict[int, dict] = {}
    row_file: dict[int, str] = {}
    for fid, meta in base_files.items():
        blob = store.get(meta.object_key)
        if blob[:4] == b"PAR1":
            _decode_rows(blob, fid, rows, row_file)
    return rows, row_file


def _load_rows_to_rewrite(store, changed, base_files, all_files):
    """GET every changed file. Only base files that are Parquet are decoded."""
    rows: dict[int, dict] = {}
    row_file: dict[int, str] = {}
    for fid in sorted(changed):
        blob = store.get(all_files[fid].object_key)
        if fid in base_files and blob[:4] == b"PAR1":
            _decode_rows(blob, fid, rows, row_file)
    return rows, row_file


def _put_version_records(store, snap_id, rows, row_file, all_files) -> int:
    """PUT one version record per file and one per live tuple. Return the count."""
    count = 0
    for fid, meta in all_files.items():
        store.put(
            f"versions/{snap_id}/file-{fid}.json",
            dump_json({"file": fid, "token": meta.token, "size": meta.size_bytes}),
        )
        count += 1
    for rid in sorted(rows):
        store.put(
            f"versions/{snap_id}/row-{rid}.json",
            dump_json({"row": rid, "file": row_file.get(rid, ""), "values": rows[rid]}),
        )
        count += 1
    store.put(f"manifests/{snap_id}.versions.json", dump_json({"records": count}))
    return count


def _put_rewritten_files(store, snap_id, changed, rows, row_file, schema: Schema, all_files) -> int:
    """PUT a new copy of every changed file. Return the number of files written."""
    count = 0
    for fid in sorted(changed):
        owned = [rid for rid, owner in row_file.items() if owner == fid and rid in rows]
        if owned:
            blob = write_parquet([{"__row_id": rid, **rows[rid]} for rid in owned], schema)
        else:
            # A file with no decoded rows is rewritten at its full size.
            blob = bytes(all_files[fid].size_bytes)
        store.put(f"data/cow-{snap_id}/{fid}.parquet", blob)
        count += 1
    store.put(
        f"manifests/{snap_id}.files.json",
        dump_json({"rewritten": sorted(changed), "files": count}),
    )
    return count
