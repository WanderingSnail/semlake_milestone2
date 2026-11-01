"""Merge branch pairs with a real lakeFS server, as a measured file-level baseline.

lakeFS versions objects and merges branches object by object: a merge fails
with a conflict when both branches changed the same object path since their
common commit, and otherwise takes each branch's objects. We give it the most
favourable table layout: every data file, every deletion vector and every
delta file is its own object, and each branch's table metadata goes to an
object with a unique name (as Iceberg metadata files do), so only data-level
overlap can conflict. A lakeFS conflict counts as a write-conflict abort; a
merge that succeeds commits whatever the branches did.

Needs a lakeFS server, e.g. ``docker run --network host treeverse/lakefs run --quickstart``.
Set ``LAKEFS_HOST`` (default http://127.0.0.1:8000), ``LAKEFS_ACCESS_KEY`` and
``LAKEFS_SECRET_KEY`` (default: the quickstart pair).
"""

from __future__ import annotations

import os
import uuid

from semlake.baselines import WriteConflictException
from semlake.merge import MergeResult

QUICKSTART_KEY = "AKIAIOSFOLQUICKSTART"
QUICKSTART_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
_CLIENT = None


def _client():
    global _CLIENT
    if _CLIENT is None:
        from lakefs.client import Client

        _CLIENT = Client(host=os.environ.get("LAKEFS_HOST", "http://127.0.0.1:8000"),
                         username=os.environ.get("LAKEFS_ACCESS_KEY", QUICKSTART_KEY),
                         password=os.environ.get("LAKEFS_SECRET_KEY", QUICKSTART_SECRET))
    return _CLIENT


def lakefs_available() -> bool:
    try:
        import lakefs  # noqa: F401

        _client().version  # noqa: B018
        return True
    except Exception:
        return False


def _objects(repo, snap) -> dict[str, bytes]:
    """The objects that hold a snapshot's table: data files and their deletion vectors."""
    out = {}
    for fid, meta in snap.files.items():
        out[f"data/{fid}.parquet"] = repo.store.get(meta.object_key) if meta.object_key in repo.store.objects \
            else bytes(meta.size_bytes)
        if meta.dv_bytes:
            out[f"data/{fid}.dv"] = bytes(meta.dv_bytes)
    return out


def _changed(base: dict[str, bytes], tip: dict[str, bytes]) -> dict[str, bytes]:
    return {key: blob for key, blob in tip.items() if base.get(key) != blob}


def lakefs_merge(repo, left_id: str, right_id: str, store=None) -> MergeResult:
    """Replay the two branches in a fresh lakeFS repository and merge both into main."""
    import lakefs
    from lakefs.exceptions import ConflictException

    base = repo.snapshots[repo.graph.lca(left_id, right_id)]
    base_objects = _objects(repo, base)
    name = f"sl{uuid.uuid4().hex[:12]}"
    lrepo = lakefs.Repository(name, client=_client()).create(
        storage_namespace=f"local://{name}", default_branch="main")
    try:
        main = lrepo.branch("main")
        with main.transact(commit_message="base") as tx:
            for key, blob in base_objects.items():
                tx.object(key).upload(data=blob)
        branches = []
        for label, tip_id in (("left", left_id), ("right", right_id)):
            branch = lrepo.branch(label).create(source_reference="main")
            changed = _changed(base_objects, _objects(repo, repo.snapshots[tip_id]))
            changed[f"_meta/{tip_id}.json"] = tip_id.encode()
            for key, blob in changed.items():
                branch.object(key).upload(data=blob)
            branch.commit(message=tip_id)
            branches.append(branch)
        try:
            for branch in branches:
                branch.merge_into(main)
        except ConflictException as exc:
            raise WriteConflictException({"lakeFS merge conflict"}) from exc
        return MergeResult(system="lakefs", status="clean", message="lakeFS object-level merge")
    finally:
        try:
            lrepo.delete()
        except Exception:
            pass
