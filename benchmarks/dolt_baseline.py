"""Replay one labelled merge in a real Dolt repository.

Dolt records foreign-key and check violations after the merge, in
``dolt_constraint_violations``. It has no place to put a ``reads_where`` or
``rows_live`` predicate, so those cases merge as ordinary row changes.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from semlake.merge import MergeResult, load_delta
from semlake.metadata import Schema
from semlake.verification import Predicate

_SQL_TYPES = {
    "int32": "BIGINT",
    "int64": "BIGINT",
    "float": "DOUBLE",
    "double": "DOUBLE",
    "string": "VARCHAR(255)",
    "bool": "BOOLEAN",
}

_AUTHOR = ("semlake", "semlake@example.com")


def dolt_binary() -> str | None:
    """Return the dolt executable, or None when it is not installed."""
    found = shutil.which("dolt")
    if found:
        return found
    candidate = Path(r"C:\Program Files\Dolt\bin\dolt.exe")
    if candidate.is_file():
        return str(candidate)
    return None


def dolt_merge(repo, left_id: str, right_id: str, store) -> MergeResult:
    """Merge the two branches in a fresh Dolt repository and classify the result."""
    binary = dolt_binary()
    if binary is None:
        raise RuntimeError("dolt is not installed")
    status, detail = _replay(binary, repo, left_id, right_id, store)
    return MergeResult(system="dolt", status=status, message=detail)


def _replay(binary: str, repo, left_id: str, right_id: str, store) -> tuple[str, str]:
    left = repo.snapshots[left_id]
    right = repo.snapshots[right_id]
    if not left.parent_ids or left.parent_ids[0] != right.parent_ids[0]:
        raise ValueError("dolt replay expects both branches to share one parent")
    base = repo.snapshots[left.parent_ids[0]]
    left_delta = load_delta(repo, store, base.snapshot_id, left_id)
    right_delta = load_delta(repo, store, base.snapshot_id, right_id)

    work = Path(tempfile.mkdtemp(prefix="semlake-dolt-"))
    try:
        _run(binary, ["init", "--name", _AUTHOR[0], "--email", _AUTHOR[1]], work, required=True)
        _run(binary, ["config", "--local", "--add", "versioncheck.disabled", "true"], work)
        _run(binary, ["config", "--local", "--add", "metrics.disabled", "true"], work)
        _sql(binary, work, _create_users(base.schema))
        _sql(binary, work, _insert_users(base.schema, base.rows))
        _commit(binary, work, "base")
        _branch(binary, work, "left")
        rejected = _apply_delta(binary, work, base.schema, left_delta)
        _commit(binary, work, "left")
        _run(binary, ["checkout", "main"], work, required=True)
        _branch(binary, work, "right")
        rejected += _apply_delta(binary, work, base.schema, right_delta)
        _commit(binary, work, "right")
        _run(binary, ["checkout", "left"], work, required=True)
        code, out, err = _run(binary, ["merge", "right"], work)
        return _classify(binary, work, code, out, err, rejected)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _classify(binary: str, work: Path, code: int, out: str, err: str, rejected: list[str]) -> tuple[str, str]:
    combined = f"{out}\n{err}"
    summary = _violation_summary(binary, work)
    notes = list(rejected)
    if "CONSTRAINT VIOLATION" in combined or summary:
        status = "semantic_conflict"
        detail = summary or "constraint violation"
    elif code != 0:
        status = "data_conflict"
        detail = " ".join(combined.split())[:300]
    else:
        status = "clean"
        detail = "clean"
    if notes:
        extra = "; ".join(notes)
        detail = extra if detail == "clean" else f"{detail}; {extra}"
    return status, detail


def _apply_delta(binary: str, work: Path, base_schema: Schema, delta) -> list[str]:
    """Apply one branch. Return check constraints Dolt refused to install."""
    rejected: list[str] = []
    known = set(base_schema.names())
    for field in delta.schema.fields:
        if field.name in known:
            continue
        _sql(
            binary,
            work,
            f"ALTER TABLE users ADD COLUMN {_ident(field.name)} {_column_type(field.type_name)}",
        )
        known.add(field.name)
    for pred in delta.predicates:
        if pred.kind != "domain":
            continue
        reason = _try_check(binary, work, pred)
        if reason:
            rejected.append(reason)
    deletes = sorted(int(rid) for rid, (op, _values, _fid) in delta.row_ops.items() if op == "delete")
    if deletes:
        _sql(binary, work, f"DELETE FROM users WHERE id IN ({', '.join(map(str, deletes))})")
    for rid, (op, values, _fid) in sorted(delta.row_ops.items()):
        if op == "delete":
            continue
        if op == "update":
            assigns = ", ".join(
                f"{_ident(name)} = {_sql_literal(value)}" for name, value in values.items()
            )
            _sql(binary, work, f"UPDATE users SET {assigns} WHERE id = {int(rid)}")
        elif op == "delete":
            _sql(binary, work, f"DELETE FROM users WHERE id = {int(rid)}")
        elif op == "insert":
            columns = ", ".join(_ident(name) for name in values)
            literals = ", ".join(_sql_literal(value) for value in values.values())
            _sql(binary, work, f"INSERT INTO users ({columns}) VALUES ({literals})")
    for pred in delta.predicates:
        if pred.kind == "foreign_key":
            _apply_foreign_key(binary, work, pred, delta.row_ops)
    return rejected


def _try_check(binary: str, work: Path, pred: Predicate) -> str | None:
    if not pred.where or ";" in pred.where:
        return f"check_rejected: {pred.name}"
    sql = f"ALTER TABLE users ADD CONSTRAINT {_ident(pred.name)} CHECK ({pred.where})"
    code, out, err = _sql(binary, work, sql, required=False)
    if code == 0:
        return None
    message = " ".join(f"{out}\n{err}".split())
    return f"check_rejected: {pred.name}: {message[:180]}"


def _apply_foreign_key(binary: str, work: Path, pred: Predicate, row_ops: dict) -> None:
    name = _ident(pred.name)
    _sql(
        binary,
        work,
        (
            "CREATE TABLE orders ("
            "id BIGINT PRIMARY KEY, "
            "user_id BIGINT, "
            f"CONSTRAINT {name} FOREIGN KEY (user_id) REFERENCES users (id))"
        ),
    )
    insert_ids = [rid for rid, (op, _values, _fid) in row_ops.items() if op == "insert"]
    parents = sorted(pred.row_ids)
    if len(insert_ids) == 1:
        order_ids = [insert_ids[0] + i * 1000 for i in range(len(parents))]
    else:
        order_ids = [1_000_000 + i for i in range(len(parents))]
    for order_id, parent in zip(order_ids, parents):
        _sql(binary, work, f"INSERT INTO orders (id, user_id) VALUES ({int(order_id)}, {int(parent)})")


def _create_users(schema: Schema) -> str:
    columns = []
    for field in schema.fields:
        kind = _column_type(field.type_name)
        if field.name == "id":
            columns.append(f"{_ident(field.name)} {kind} PRIMARY KEY")
            continue
        nullability = "NULL" if field.nullable else "NOT NULL"
        columns.append(f"{_ident(field.name)} {kind} {nullability}")
    return f"CREATE TABLE users ({', '.join(columns)})"


def _insert_users(schema: Schema, rows: dict) -> str:
    columns = [_ident(field.name) for field in schema.fields]
    values = []
    for _rid, row in sorted(rows.items()):
        literals = ", ".join(_sql_literal(row.get(field.name)) for field in schema.fields)
        values.append(f"({literals})")
    return f"INSERT INTO users ({', '.join(columns)}) VALUES {', '.join(values)}"


def _column_type(type_name: str) -> str:
    try:
        return _SQL_TYPES[type_name]
    except KeyError as exc:
        raise ValueError(f"no Dolt type for {type_name}") from exc


def _ident(name: str) -> str:
    if not name or not name.replace("_", "a").isalnum() or name[0].isdigit():
        raise ValueError(f"unsafe SQL name: {name}")
    return name


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _violation_summary(binary: str, work: Path) -> str:
    code, out, _err = _sql(
        binary,
        work,
        "SELECT * FROM dolt_constraint_violations",
        result_format="json",
        required=False,
    )
    if code != 0 or not out.strip():
        return ""
    try:
        payload = json.loads(out)
    except json.JSONDecodeError:
        return ""
    rows = payload.get("rows", payload) if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or not rows:
        return ""
    parts = []
    for row in rows:
        table = row.get("table") or row.get("table_name") or "table"
        count = row.get("num_violations", 1)
        parts.append(f"{table}:{count}")
    return "constraint " + ", ".join(parts)


def _branch(binary: str, work: Path, name: str) -> None:
    _run(binary, ["checkout", "-b", name], work, required=True)


def _commit(binary: str, work: Path, message: str) -> None:
    code, out, err = _run(binary, ["commit", "-Am", message], work)
    if code == 0:
        return
    combined = f"{out}\n{err}"
    if "no changes added to commit" in combined:
        return
    raise RuntimeError(combined.strip())


def _sql(binary: str, work: Path, statement: str, result_format: str = "tabular", required: bool = True):
    if len(statement) > 50_000:
        # Long statements (a large base table) go through stdin, past the argument-length limit.
        return _run(binary, ["sql", "-r", result_format], work, required=required, stdin=statement + ";")
    return _run(
        binary,
        ["sql", "-r", result_format, "-q", statement],
        work,
        required=required,
    )


def _run(binary: str, args: list[str], work: Path, required: bool = False, stdin: str | None = None):
    proc = subprocess.run(
        [binary, *args],
        cwd=work,
        capture_output=True,
        text=True,
        check=False,
        timeout=600,
        input=stdin,
    )
    if required and proc.returncode != 0:
        raise RuntimeError(f"dolt {' '.join(args[:3])} failed: {proc.stdout}\n{proc.stderr}")
    return proc.returncode, proc.stdout, proc.stderr
