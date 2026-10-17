"""Pull a read or delete predicate out of one SQL statement.

The supported shape is a single DELETE or SELECT with a WHERE clause. The
clause is handed to the same checker as a writer-declared predicate. Joins,
UNION, and subqueries are kept as text and the checker treats them as a
possible overlap.
"""

from __future__ import annotations

import re

from semlake.verification import Predicate

_TAIL = re.compile(r"\b(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING)\b", re.IGNORECASE)


def predicates_from_sql(sql: str, name: str = "query") -> tuple[Predicate, ...]:
    """Return the WHERE predicate of ``sql``, or an empty tuple when it has none."""
    kind, where = _split_statement(sql)
    if not where:
        return ()
    predicate_kind = "deletes_where" if kind == "delete" else "reads_where"
    return (Predicate(name, predicate_kind, where=where),)


def _split_statement(sql: str) -> tuple[str, str]:
    parsed = _split_with_sqlglot(sql)
    if parsed is not None:
        return parsed
    return _split_simple(sql)


def _split_with_sqlglot(sql: str) -> tuple[str, str] | None:
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return None
    try:
        tree = sqlglot.parse_one(sql)
    except sqlglot.errors.ParseError:
        return None
    where = tree.args.get("where")
    text = where.this.sql(dialect="duckdb") if where is not None else ""
    kind = "delete" if isinstance(tree, exp.Delete) else "read"
    return kind, text


def _split_simple(sql: str) -> tuple[str, str]:
    text = " ".join(sql.strip().split())
    lowered = text.lower()
    if lowered.startswith("delete"):
        kind = "delete"
    elif lowered.startswith("select") or lowered.startswith("with"):
        kind = "read"
    else:
        raise ValueError(f"expected SELECT or DELETE, got: {sql[:40]}")
    match = re.search(r"\bWHERE\b", text, flags=re.IGNORECASE)
    if match is None:
        return kind, ""
    clause = text[match.end():].strip()
    tail = _TAIL.search(clause)
    if tail:
        clause = clause[: tail.start()].strip()
    return kind, clause
