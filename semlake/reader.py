"""Read a snapshot back from the object store, using only what was published.

``materialize`` follows the snapshot's manifest list, reads each data file and
applies its deletion vector. It never looks at the in-memory snapshot rows, so
it shows what another engine would see. ``export_files`` copies the files to a
local directory so DuckDB can scan them.
"""

from __future__ import annotations

from pathlib import Path

from semlake.metadata import FileMeta, Repository
from semlake.storage import DeletionVector, MemoryObjectStore, file_from_doc, load_json, read_parquet


def load_listing(store: MemoryObjectStore, key: str) -> dict[str, FileMeta]:
    """Load a file listing. A manifest list is read by following its manifests in order."""
    payload = load_json(store.get(key))
    children = payload.get("manifests") if isinstance(payload, dict) else None
    if isinstance(children, list):
        merged: dict[str, FileMeta] = {}
        for child in children:
            merged.update(load_listing(store, child))
        return merged
    return {fid: file_from_doc(fid, doc) for fid, doc in payload.items()}


def materialize(repo: Repository, snap_id: str, store: MemoryObjectStore | None = None) -> dict[int, dict]:
    """Return {row id: row} for the live rows of ``snap_id``, with every schema column present."""
    store = store or repo.store
    snap = repo.snapshots[snap_id]
    rows: dict[int, dict] = {}
    for fid, meta in sorted(load_listing(store, snap.manifest_key).items()):
        blob = store.get(meta.object_key)
        if blob[:4] != b"PAR1":
            continue
        hidden = DeletionVector.from_bytes(meta.dv_bytes)
        for position, record in enumerate(read_parquet(blob)):
            if position in hidden:
                continue
            rid = int(record.pop("__row_id"))
            if rid in rows:
                raise ValueError(f"row {rid} is live in two files, the second is {fid}")
            rows[rid] = record
    names = snap.schema.names()
    return {rid: {name: row.get(name) for name in names} for rid, row in rows.items()}


def export_files(repo: Repository, snap_id: str, store: MemoryObjectStore, out_dir: Path) -> dict:
    """Write the snapshot's data files under ``out_dir``.

    Return {"clean": [path, ...], "dirty": [(path, deleted positions), ...]}.
    A file is dirty when its deletion vector is not empty.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    clean: list[str] = []
    dirty: list[tuple[str, list[int]]] = []
    for fid, meta in sorted(load_listing(store, repo.snapshots[snap_id].manifest_key).items()):
        blob = store.get(meta.object_key)
        if blob[:4] != b"PAR1":
            continue
        path = out_dir / f"{fid}.parquet"
        path.write_bytes(blob)
        deleted = DeletionVector.from_bytes(meta.dv_bytes)
        if len(deleted):
            dirty.append((path.as_posix(), sorted(deleted.bitmap)))
        else:
            clean.append(path.as_posix())
    return {"clean": clean, "dirty": dirty}
