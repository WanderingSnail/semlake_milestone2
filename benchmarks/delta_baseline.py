"""File-level commit against a real Delta table, when deltalake is installed.

Delta's optimistic concurrency aborts the second writer that started from the
same version. SemLake's B1 is that rule on our own snapshots. This module
checks the same rule on a Delta log so the comparison is not only an imitation.
"""

from __future__ import annotations

import tempfile
from pathlib import Path


def delta_available() -> bool:
    try:
        import importlib
        importlib.import_module("deltalake")
    except ImportError:
        return False
    return True


def second_writer_conflicts() -> bool:
    """Return whether a second commit from the same Delta version is rejected."""
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
    from deltalake.exceptions import CommitFailedError

    table = pa.table({"id": [1, 2], "value": [10, 20]})
    with tempfile.TemporaryDirectory(prefix="semlake-delta-") as tmp:
        path = str(Path(tmp))
        write_deltalake(path, table, mode="overwrite")
        first = DeltaTable(path)
        second = DeltaTable(path)
        first.update({"value": "11"}, predicate="id = 1")
        try:
            second.update({"value": "21"}, predicate="id = 2")
        except CommitFailedError:
            return True
    return False
