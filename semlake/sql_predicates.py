"""Pull a read or delete predicate out of one SQL statement.

A single-table DELETE or SELECT yields its WHERE clause, handed to the same
checker as a writer-declared predicate.

A SELECT with joins or subqueries is *projected* onto the table being merged.
For every query block that reads the table, the parts of the block's WHERE (and
of its inner-join ON clauses) that mention only that table's columns are kept
and the rest are replaced by TRUE: a dropped conjunct disappears, and an OR
with a dropped branch disappears as a whole. Dropping can only widen the set of
rows the block may read, so the kept clause is a sound over-approximation of
the rows of the table the block depends on. This is the filter a scan of the
table would have pushed down. Each block yields its own read predicate, so a
row that moves from one block's range into another's still crosses a clause. A
block with an outer, semi or anti join, or with nothing left to keep, reads the
whole table.
"""

from __future__ import annotations

import re

from semlake.verification import Predicate

_TAIL = re.compile(r"\b(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING)\b", re.IGNORECASE)

# Read classes reported in the evaluation.
SINGLE = "single"
JOIN = "join"
SUBQUERY = "subquery"
TABLE_WIDE = "table-wide"


def predicates_from_sql(
    sql: str,
    name: str = "query",
    table: str | None = None,
    table_columns: frozenset[str] | set[str] | None = None,
    project: bool = True,
) -> tuple[Predicate, ...]:
    """Return the predicate ``sql`` declares on ``table``.

    A DELETE without a WHERE clause and a statement that does not read
    ``table`` yield nothing. A SELECT without a WHERE clause reads the whole
    table. ``project=False`` turns the projection off, so a join or subquery
    read keeps its full clause text and conflicts with every delete (ablation,
    and the behaviour of earlier versions).
    """
    kind, where = _split_statement(sql)
    if kind == "delete":
        return (Predicate(name, "deletes_where", where=where),) if where else ()
    shape = read_shape(sql)
    if shape == SINGLE:
        return (Predicate(name, "reads_where", detail=SINGLE, where=where, columns=read_columns(sql)),)
    if not project or table is None:
        # Kept as text: the checker cannot evaluate it on single-table rows.
        return (Predicate(name, "reads_where", detail=shape, where=where, columns=None),)
    projected = project_read(sql, table, table_columns)
    if projected is None:
        return ()
    return tuple(
        Predicate(name if index == 0 else f"{name}#{index + 1}", "reads_where", detail=shape, where=clause,
                  columns=columns)
        for index, (clause, columns) in enumerate(projected)
    )


def read_shape(sql: str) -> str:
    """Classify a SELECT as single-table, join, or subquery (a subquery wins over a join)."""
    try:
        import sqlglot
        from sqlglot import exp

        tree = sqlglot.parse_one(sql, read="duckdb")
    except Exception:  # noqa: BLE001 - unparsed text is treated as a single clause
        return SINGLE
    selects = list(tree.find_all(exp.Select))
    if len(selects) > 1 or tree.find(exp.Subquery) is not None:
        return SUBQUERY
    if isinstance(tree, exp.Select) and (tree.args.get("joins") or len(_from_tables(tree)) > 1):
        return JOIN
    return SINGLE


def project_read(
    sql: str, table: str, table_columns: frozenset[str] | set[str] | None = None
) -> list[tuple[str, frozenset[str] | None]] | None:
    """Project a join or subquery read onto ``table``.

    Returns one ``(where, columns)`` pair per distinct clause: a single-table
    clause that every row of ``table`` the block can depend on satisfies (""
    for the whole table), and the columns of ``table`` the query uses outside
    the kept clauses (``None`` for any column). Returns ``None`` when the query
    does not read ``table`` at all.
    """
    import sqlglot
    from sqlglot import exp

    table = table.lower()
    owned = {col.lower() for col in table_columns} if table_columns else None
    try:
        tree = sqlglot.parse_one(sql, read="duckdb")
    except sqlglot.errors.ParseError:
        return [("", None)]

    clauses: list[str] = []
    kept_nodes: set[int] = set()
    reads_table = False
    for block in tree.find_all(exp.Select):
        aliases = [alias for name, alias in _from_tables(block) if name == table]
        if not aliases:
            continue
        reads_table = True
        others = {alias for name, alias in _from_tables(block) if name != table}
        joins = block.args.get("joins") or []
        if any(_is_outer(join) for join in joins) or len(aliases) > 1 and owned is None:
            clauses.append("")
            continue
        conjuncts: list = []
        where = block.args.get("where")
        if where is not None:
            conjuncts.append(where.this)
        for join in joins:
            if join.args.get("on") is not None:
                conjuncts.append(join.args["on"])
        for alias in aliases:
            foreign = others | (set(aliases) - {alias})
            exact: list = []
            parts: list[str] = []
            for node in conjuncts:
                parts.extend(_project(node, alias, foreign, owned, len(aliases) == 1, exact) or [])
            if not parts:
                clauses.append("")
                continue
            kept_nodes.update(id(node) for node in exact)
            clauses.append(" AND ".join(f"({part})" for part in parts))

    if not reads_table:
        return None
    columns = _columns_outside(tree, kept_nodes, table, owned)
    return [(clause, columns) for clause in dict.fromkeys(clauses)]


def _project(
    node, alias: str, foreign: set[str], owned: set[str] | None, single: bool, exact: list
) -> list[str] | None:
    """The conjuncts of a boolean expression that mention only the one table occurrence.

    Returns SQL texts to be joined by AND, or ``None`` for TRUE (nothing could
    be kept). ``exact`` collects the nodes kept as they are under AND only: a
    row that satisfies such a node before and after an update did not change
    its truth value, so the node's columns need no separate check. A node kept
    under an OR that lost a part is not exact, because the row may move between
    the branches of the OR.
    """
    from sqlglot import exp

    if isinstance(node, exp.Paren):
        return _project(node.this, alias, foreign, owned, single, exact)
    if isinstance(node, exp.And):
        kept: list[str] = []
        for side in (node.left, node.right):
            kept.extend(_project(side, alias, foreign, owned, single, exact) or [])
        return kept or None
    if _only_mentions(node, alias, foreign, owned, single):
        exact.append(node)
        return [_unqualified(node)]
    if isinstance(node, exp.Or):
        loose: list = []  # nodes under a partly kept OR are never exact
        sides = [_project(side, alias, foreign, owned, single, loose) for side in (node.left, node.right)]
        if any(side is None for side in sides):
            return None
        texts = [side[0] if len(side) == 1 else " AND ".join(f"({part})" for part in side) for side in sides]
        return [" OR ".join(f"({text})" for text in texts)]
    return None


def _from_tables(block) -> list[tuple[str, str]]:
    """(table name, alias) of the base tables in a block's FROM and JOIN list."""
    from sqlglot import exp

    found = []
    sources = []
    if block.args.get("from") is not None:
        sources.append(block.args["from"].this)
    elif block.args.get("from_") is not None:
        sources.append(block.args["from_"].this)
    for join in block.args.get("joins") or []:
        sources.append(join.this)
    for source in sources:
        if isinstance(source, exp.Table):
            found.append((source.name.lower(), (source.alias or source.name).lower()))
    return found


def _is_outer(join) -> bool:
    side = (join.args.get("side") or "").upper()
    kind = (join.args.get("kind") or "").upper()
    return side in ("LEFT", "RIGHT", "FULL") or kind in ("SEMI", "ANTI", "OUTER")


def _only_mentions(node, alias: str, other_aliases: set[str], owned: set[str] | None, single: bool) -> bool:
    """Whether a conjunct refers to columns of the one table occurrence and nothing else."""
    from sqlglot import exp

    if node.find(exp.Subquery) is not None or node.find(exp.Select) is not None:
        return False
    columns = list(node.find_all(exp.Column))
    if not columns:
        return False
    for col in columns:
        qualifier = (col.table or "").lower()
        if qualifier:
            if qualifier != alias:
                return False
        elif owned is not None:
            if col.name.lower() not in owned:
                return False
        elif other_aliases or not single:
            # Unqualified, and nothing says which table owns it.
            return False
    return True


def _unqualified(node) -> str:
    from sqlglot import exp

    node = node.copy()
    for col in node.find_all(exp.Column):
        col.set("table", None)
    return node.sql(dialect="duckdb")


def _columns_outside(tree, kept_nodes: set[int], table: str, owned: set[str] | None) -> frozenset[str] | None:
    """Columns of ``table`` the query uses outside the kept conjuncts; None for any column."""
    from sqlglot import exp

    for star in tree.find_all(exp.Star):
        if isinstance(star.parent, exp.Count):
            continue
        block = star.find_ancestor(exp.Select)
        if block is not None and isinstance(block.parent, exp.Exists):
            continue  # EXISTS (SELECT * ...) uses no column of the row
        return None
    if owned is None:
        return None
    used: set[str] = set()
    for col in tree.find_all(exp.Column):
        name = col.name.lower()
        if name not in owned:
            continue
        node = col
        inside = False
        while node is not None:
            if id(node) in kept_nodes:
                inside = True
                break
            node = node.parent
        if not inside:
            used.add(name)
    return frozenset(used)


def read_columns(sql: str) -> frozenset[str] | None:
    """Columns a SELECT's result depends on outside its WHERE clause.

    A row that stays inside the clause changes the result only through these
    columns; moving rows across the clause is checked separately. Returns
    ``None`` (any column) for ``*``, a join, or text that does not parse as a SELECT.
    ``count(*)`` depends only on which rows are in range, so its star is no
    column at all.
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return None
    try:
        tree = sqlglot.parse_one(sql)
    except sqlglot.errors.ParseError:
        return None
    if not isinstance(tree, exp.Select) or tree.args.get("joins"):
        return None
    tree = tree.copy()
    tree.set("where", None)
    if any(not isinstance(star.parent, exp.Count) for star in tree.find_all(exp.Star)):
        return None
    return frozenset(col.name.lower() for col in tree.find_all(exp.Column))


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
