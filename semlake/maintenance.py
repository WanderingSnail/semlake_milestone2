"""Operations around the merge: choice resolution, pre-image pruning, crash recovery.

Commit protocol (what the recovery code relies on)
--------------------------------------------------
A commit or a merge writes, in this order,

1. data objects (delta Parquet files),
2. the commit summary,
3. the manifests (a delta manifest and a manifest list),

and only then moves a branch head with one compare-and-swap on the catalog
(``Repository.cas_head``). Every object key contains the new snapshot id, so no
write overwrites an object another snapshot uses. The invariants are:

I1  A branch head always names a snapshot whose summary, manifests and data
    objects all exist (``check_repository``).
I2  A crash before the compare-and-swap changes no branch head; it leaves only
    objects that no head can reach.
I3  Retrying a crashed commit or merge from the same parents gives a snapshot
    with the same rows.
I4  A group commit moves the head once for the whole batch, so either every
    merge of the batch is visible or none is.
I5  ``collect_garbage`` removes exactly the objects no head can reach.

The catalog (branch heads and the snapshot graph) is assumed to be a small
linearizable store with compare-and-swap, as an object store's conditional
write or a transactional catalog provides.
"""

from __future__ import annotations

from semlake.metadata import CrashError, Repository, Snapshot
from semlake.storage import MemoryObjectStore, dump_json, load_json


# ---------------------------------------------------------------------------
# Choice nodes


def unresolved(snap: Snapshot) -> dict[int, list[str]]:
    """Rows of ``snap`` that carry an open choice, with the columns in question.

    A cell with an open choice shows the value it had at the fork: no branch's
    write is applied until someone resolves it. ``"*"`` stands for the whole row
    (one branch deleted it, the other changed it).
    """
    found: dict[int, list[str]] = {}
    for choice in snap.open_choices:
        found.setdefault(choice["row_id"], []).append(choice.get("column", "*"))
    return found


def resolve_choices(
    repo: Repository,
    snap: Snapshot,
    picks: dict[tuple[int, str], object],
    branch: str | None = None,
    message: str = "resolve choices",
) -> Snapshot:
    """Close choices of ``snap`` in one ordinary commit and return it.

    ``picks`` maps ``(row id, column)`` to ``"A"`` or ``"B"`` (take that
    branch's option) or to ``("value", v)`` for a new value. For a whole-row
    choice (column ``"*"``) the pick must be ``"A"`` or ``"B"``. The result is a
    normal commit, so a later merge checks it like any other write.
    """
    updates: dict[int, dict] = {}
    deletes: list[int] = []
    by_key = {(choice["row_id"], choice.get("column", "*")): choice for choice in snap.open_choices}
    for key, pick in picks.items():
        if key not in by_key:
            raise KeyError(f"no open choice for row {key[0]} column {key[1]}")
        rid, column = key
        choice = by_key[key]
        if isinstance(pick, tuple) and pick and pick[0] == "value":
            if column == "*":
                raise ValueError("a whole-row choice takes one branch's operation, not a value")
            updates.setdefault(rid, {})[column] = pick[1]
            continue
        option = next((item for item in choice["options"] if item.get("branch") == pick), None)
        if option is None:
            raise ValueError(f"row {rid} column {column} has no option {pick!r}")
        if column == "*":
            if option.get("op") == "delete":
                deletes.append(rid)
            else:
                updates.setdefault(rid, {}).update(option.get("value") or {})
        else:
            updates.setdefault(rid, {})[column] = option["value"]
    for rid in deletes:
        updates.pop(rid, None)
    return repo.apply(snap, branch or snap.branch, updates=updates, deletes=deletes, message=message)


# ---------------------------------------------------------------------------
# Pre-image pruning


def prune_preimages(
    repo: Repository,
    snap_id: str,
    keep_columns: frozenset[str] | set[str] | tuple[str, ...] = (),
) -> dict:
    """Remove the recorded values of rows a commit deleted, from its summary.

    ``keep_columns`` are kept (the columns open branches still filter on); with
    none, the whole pre-image goes. The rows are listed under ``pruned`` in the
    summary, so a later merge knows the values are missing on purpose: a read
    clause it cannot decide on what is left keeps the conflict. Pruning can
    therefore only add blocks, never hide a conflict.

    Returns the summary size before and after, in bytes.
    """
    snap = repo.snapshots[snap_id]
    doc = load_json(repo.store.objects[snap.summary_key])
    before_bytes = len(repo.store.objects[snap.summary_key])
    keep = {str(col) for col in keep_columns}
    deleted = [rid for rid, item in doc["row_ops"].items() if item["op"] == "delete"]
    kept_before = dict(doc.get("before", {}))
    for rid in deleted:
        values = kept_before.pop(rid, None)
        if values is not None and keep:
            projected = {col: value for col, value in values.items() if col in keep}
            if projected:
                kept_before[rid] = projected
    doc["before"] = kept_before
    doc["pruned"] = sorted({*doc.get("pruned", []), *(int(rid) for rid in deleted)})
    blob = dump_json(doc)
    repo.store.objects[snap.summary_key] = blob
    for rid in deleted:
        if int(rid) in snap.row_before:
            values = snap.row_before.pop(int(rid))
            if keep:
                projected = {col: value for col, value in values.items() if col in keep}
                if projected:
                    snap.row_before[int(rid)] = projected
    return {"before_bytes": before_bytes, "after_bytes": len(blob), "rows": len(deleted)}


def prunable(repo: Repository, snap_id: str) -> bool:
    """Whether every open branch already contains ``snap_id``.

    Once it does, no later merge replays the commit against a concurrent
    reader, so its pre-images can go without costing any precision.
    """
    return all(snap_id in repo.graph.ancestors(head) for head in repo.heads.values())


# ---------------------------------------------------------------------------
# Crash recovery


class FaultyStore(MemoryObjectStore):
    """An object store that raises ``CrashError`` on its ``crash_at``-th PUT (1-based)."""

    def __init__(self, crash_at: int | None = None) -> None:
        super().__init__()
        self.crash_at = crash_at
        self.puts_seen = 0

    def put(self, key: str, data: bytes) -> None:
        self.puts_seen += 1
        if self.crash_at is not None and self.puts_seen == self.crash_at:
            raise CrashError(f"crash before PUT {self.puts_seen} ({key})")
        super().put(key, data)

    def clone(self) -> FaultyStore:
        copied = FaultyStore(self.crash_at)
        copied.objects.update(self.objects)
        return copied


def _manifest_keys(store: MemoryObjectStore, key: str, seen: set[str]) -> None:
    if key in seen or key not in store.objects:
        return
    seen.add(key)
    payload = load_json(store.objects[key])
    children = payload.get("manifests") if isinstance(payload, dict) else None
    if isinstance(children, list):
        for child in children:
            _manifest_keys(store, child, seen)


def reachable_objects(
    repo: Repository, store: MemoryObjectStore | None = None, roots: tuple[str, ...] = ()
) -> set[str]:
    """Every object key a branch head (or an extra root snapshot) reaches through the graph."""
    store = store or repo.store
    keys: set[str] = set()
    for head in (*repo.heads.values(), *roots):
        for snap_id in repo.graph.ancestors(head):
            snap = repo.snapshots[snap_id]
            if snap.summary_key:
                keys.add(snap.summary_key)
            if snap.manifest_key:
                _manifest_keys(store, snap.manifest_key, keys)
            for meta in snap.files.values():
                keys.add(meta.object_key)
    return keys


def check_repository(repo: Repository, store: MemoryObjectStore | None = None) -> list[str]:
    """Return the violations of invariant I1; an empty list means the repository is sound."""
    store = store or repo.store
    problems: list[str] = []
    for branch, head in repo.heads.items():
        if head not in repo.snapshots:
            problems.append(f"{branch}: head {head} is not a registered snapshot")
            continue
        for snap_id in repo.graph.ancestors(head):
            snap = repo.snapshots[snap_id]
            for label, key in (("summary", snap.summary_key), ("manifest", snap.manifest_key)):
                if not key or key not in store.objects:
                    problems.append(f"{branch}: {snap_id} lacks its {label} object {key!r}")
            seen: set[str] = set()
            if snap.manifest_key in store.objects:
                payload = load_json(store.objects[snap.manifest_key])
                for child in payload.get("manifests", []) if isinstance(payload, dict) else []:
                    if child not in store.objects:
                        problems.append(f"{branch}: {snap_id} lists a missing manifest {child!r}")
                    seen.add(child)
        for fid, meta in repo.snapshots[head].files.items():
            if meta.object_key not in store.objects:
                problems.append(f"{branch}: head file {fid} lacks its data object {meta.object_key!r}")
    return problems


def collect_garbage(
    repo: Repository, store: MemoryObjectStore | None = None, roots: tuple[str, ...] = ()
) -> list[str]:
    """Delete the objects no branch head (or extra root) can reach and return their keys."""
    store = store or repo.store
    keep = reachable_objects(repo, store, roots)
    orphans = sorted(key for key in store.objects if key not in keep)
    for key in orphans:
        del store.objects[key]
    return orphans
