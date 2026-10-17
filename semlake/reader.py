"""Read a snapshot back from the object store.

``materialize`` returns the live rows of any snapshot, whichever system wrote
it. ``export_files`` copies a file-based snapshot to a local directory so
DuckDB can scan the Parquet files and apply the deletion vectors.
"""

from __future__ import annotations

from pathlib import Path

from semlake.baselines import apply_ops
from semlake.metadata import Repository
from semlake.storage import (
    DeletionVector,
    MemoryObjectStore,
    live_records,
    load_json,
    load_listing,
    ops_from_doc,
)


def materialize(repo: Repository, snap_id: str, store: MemoryObjectStore) -> dict[int, dict]:
    """Return {row id: row} for the live rows of ``snap_id``, with every schema column present."""
    snap = repo.snapshots[snap_id]
    if snap.layout == "versions":
        rows = {}
        for key in store.list(f"versions/{snap_id}/row-"):
            record = load_json(store.get(key))
            rows[int(record["row"])] = record["values"]
    elif snap.layout == "oplog":
        rows = materialize(repo, snap.base_id, store)
        apply_ops(rows, ops_from_doc(load_json(store.get(snap.summary_key))["oplog"]))
    else:
        rows = {}
        for fid, meta in sorted(load_listing(store, snap.manifest_key).items()):
            if meta.row_count == 0:
                continue
            for _pos, record in live_records(store.get(meta.object_key), meta.dv_bytes):
                rid = int(record.pop("__row_id"))
                if rid in rows:
                    raise ValueError(f"row {rid} is live in two files, the second is {fid}")
                rows[rid] = record
    names = snap.schema.names()
    return {rid: {name: row.get(name) for name in names} for rid, row in rows.items()}


def export_files(repo: Repository, snap_id: str, store: MemoryObjectStore, out_dir: Path) -> dict:
    """Write the snapshot's data files and deletion vectors under ``out_dir``.

    Return {"clean": [path, ...], "dirty": [(path, deleted positions), ...]}.
    A file is dirty when its deletion vector is not empty.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    clean: list[str] = []
    dirty: list[tuple[str, list[int]]] = []
    for fid, meta in sorted(load_listing(store, repo.snapshots[snap_id].manifest_key).items()):
        if meta.row_count == 0:
            continue
        path = out_dir / f"{fid}.parquet"
        path.write_bytes(store.get(meta.object_key))
        deleted = DeletionVector.from_bytes(meta.dv_bytes)
        if len(deleted):
            dirty.append((path.as_posix(), list(deleted.bitmap)))
        else:
            clean.append(path.as_posix())
    return {"clean": clean, "dirty": dirty}
