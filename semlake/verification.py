"""Semantic predicates checked before a merge is allowed to commit."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Predicate:
    """A fact a commit relies on, or the scope of a delete it performed.

    Kinds:

    - ``rows_live``: the writer lists row ids an aggregate or foreign key
      depends on. Deleting any of them on the other branch is a conflict.
    - ``reads_where``: the commit read rows matching a SQL WHERE clause,
      for example the filter of an aggregate query.
    - ``deletes_where``: the commit deleted rows matching a SQL WHERE clause.

    A ``deletes_where`` on one branch conflicts with a ``reads_where`` on the
    other when both clauses can hold for the same row. The test is static and
    looks only at the two clauses, never at table data.
    """

    name: str
    kind: str
    row_ids: frozenset[int] = frozenset()
    detail: str = ""
    where: str = ""

    def to_doc(self) -> dict:
        return {
            "name": self.name,
            "kind": self.kind,
            "row_ids": sorted(self.row_ids),
            "detail": self.detail,
            "where": self.where,
        }

    @classmethod
    def from_doc(cls, doc: dict) -> Predicate:
        return cls(
            name=doc["name"],
            kind=doc["kind"],
            row_ids=frozenset(int(rid) for rid in doc.get("row_ids", [])),
            detail=doc.get("detail", ""),
            where=doc.get("where", ""),
        )


@dataclass(frozen=True)
class Violation:
    predicate: str
    detail: str
    sample_rows: tuple[int, ...]

    def to_doc(self) -> dict:
        return {
            "predicate": self.predicate,
            "detail": self.detail,
            "sample_rows": list(self.sample_rows),
        }


def deleted_ids(row_ops: dict[int, tuple]) -> set[int]:
    return {rid for rid, op in row_ops.items() if op[0] == "delete"}


def check_invariants(
    left_deletes: set[int],
    left_predicates: tuple[Predicate, ...],
    right_deletes: set[int],
    right_predicates: tuple[Predicate, ...],
) -> list[Violation]:
    """Return a violation for every delete on one branch that breaks a predicate on the other."""
    found = _find_violations("A", left_deletes, left_predicates, right_predicates)
    found += _find_violations("B", right_deletes, right_predicates, left_predicates)
    return found


def _find_violations(
    branch: str,
    deleted: set[int],
    own_predicates: tuple[Predicate, ...],
    other_predicates: tuple[Predicate, ...],
) -> list[Violation]:
    found: list[Violation] = []
    for pred in other_predicates:
        if pred.kind != "rows_live" or not deleted:
            continue
        hit = pred.row_ids & deleted
        if not hit:
            continue
        found.append(
            Violation(
                predicate=pred.name,
                detail=f"branch {branch} deleted {len(hit)} row(s) required by {pred.name}",
                sample_rows=tuple(sorted(hit)[:12]),
            )
        )
    for scope in own_predicates:
        if scope.kind != "deletes_where":
            continue
        for pred in other_predicates:
            if pred.kind != "reads_where":
                continue
            overlap, reason = where_overlap(scope.where, pred.where)
            if not overlap:
                continue
            found.append(
                Violation(
                    predicate=pred.name,
                    detail=(
                        f"branch {branch} delete [{scope.where}] can match rows read by "
                        f"{pred.name} [{pred.where}] ({reason})"
                    ),
                    sample_rows=(),
                )
            )
    return found


# The WHERE-clause analyzer accepts conjunctions of these terms:
#   column (=|!=|<>|<|<=|>|>=) literal
#   column IN (literal, ...)
#   column BETWEEN literal AND literal
# Literals are numbers or single-quoted strings. Anything else (OR, NOT,
# functions, column-to-column comparisons) counts as a possible overlap, so
# syntax the analyzer does not understand can only add conflicts.

_LITERAL = r"'[^']*'|-?\d+(?:\.\d+)?"
_TERM = re.compile(
    rf"""^\s*(?P<col>[A-Za-z_][A-Za-z0-9_]*)\s*
        (?:
            (?P<op>=|!=|<>|<=|>=|<|>)\s*(?P<value>{_LITERAL})
          | (?P<in>IN)\s*\((?P<items>[^)]*)\)
          | (?P<between>BETWEEN)\s+(?P<low>{_LITERAL})\s+AND\s+(?P<high>{_LITERAL})
        )\s*$""",
    re.IGNORECASE | re.VERBOSE,
)


class _UnsupportedClause(Exception):
    pass


def _parse_literal(text: str):
    text = text.strip()
    if text.startswith("'"):
        return text[1:-1]
    return float(text) if "." in text else int(text)


def _split_and(clause: str) -> list[str]:
    # Hide string literals and the AND inside BETWEEN behind control
    # characters, split on the remaining ANDs, then put them back.
    quoted: list[str] = []

    def hide(match: re.Match) -> str:
        quoted.append(match.group(0))
        return f"\x00{len(quoted) - 1}\x00"

    masked = re.sub(r"'[^']*'", hide, clause)
    if re.search(r"\b(OR|NOT)\b", masked, flags=re.IGNORECASE):
        raise _UnsupportedClause(clause)
    masked = re.sub(
        r"(\bBETWEEN\s+\S+)\s+AND\s+",
        lambda match: match.group(1) + " \x01 ",
        masked,
        flags=re.IGNORECASE,
    )
    parts = []
    for part in re.split(r"\s+AND\s+", masked, flags=re.IGNORECASE):
        part = part.replace("\x01", "AND")
        part = re.sub(r"\x00(\d+)\x00", lambda match: quoted[int(match.group(1))], part)
        if part.strip():
            parts.append(part.strip())
    return parts


def _parse_where(clause: str) -> list[tuple[str, str, object]]:
    """Return (column, operator, value) terms. BETWEEN becomes two range terms."""
    terms = []
    for part in _split_and(clause):
        match = _TERM.match(part)
        if not match:
            raise _UnsupportedClause(part)
        col = match.group("col").lower()
        if match.group("op"):
            op = "!=" if match.group("op") == "<>" else match.group("op")
            terms.append((col, op, _parse_literal(match.group("value"))))
        elif match.group("in"):
            items = re.findall(_LITERAL, match.group("items"))
            terms.append((col, "in", {_parse_literal(item) for item in items}))
        else:
            terms.append((col, ">=", _parse_literal(match.group("low"))))
            terms.append((col, "<=", _parse_literal(match.group("high"))))
    return terms


def _is_satisfiable(terms: list[tuple[str, object]]) -> bool:
    """Return whether one column value can meet every (operator, value) term."""
    allowed = None
    excluded = set()
    low, low_inclusive = None, True
    high, high_inclusive = None, True
    try:
        for op, value in terms:
            if op == "=":
                allowed = {value} if allowed is None else allowed & {value}
            elif op == "in":
                allowed = set(value) if allowed is None else allowed & set(value)
            elif op == "!=":
                excluded.add(value)
            elif op in (">", ">="):
                if low is None or value > low or (value == low and op == ">"):
                    low, low_inclusive = value, op == ">="
            elif op in ("<", "<="):
                if high is None or value < high or (value == high and op == "<"):
                    high, high_inclusive = value, op == "<="

        def in_range(candidate) -> bool:
            if candidate in excluded:
                return False
            if low is not None and (candidate < low or (candidate == low and not low_inclusive)):
                return False
            if high is not None and (candidate > high or (candidate == high and not high_inclusive)):
                return False
            return True

        if allowed is not None:
            return any(in_range(candidate) for candidate in allowed)
        if low is not None and high is not None:
            if low > high or (low == high and not (low_inclusive and high_inclusive)):
                return False
            if low == high:
                return low not in excluded
        return True
    except TypeError:
        # One column compared with both strings and numbers. Count it as an overlap.
        return True


def where_overlap(left: str, right: str) -> tuple[bool, str]:
    """Return whether some row can satisfy both WHERE clauses, and why."""
    if not left.strip() or not right.strip():
        return True, "an empty clause matches every row"
    try:
        terms = _parse_where(left) + _parse_where(right)
    except _UnsupportedClause as exc:
        return True, f"clause not analyzed: {exc}"
    by_column: dict[str, list[tuple[str, object]]] = {}
    for col, op, value in terms:
        by_column.setdefault(col, []).append((op, value))
    for col, column_terms in by_column.items():
        if not _is_satisfiable(column_terms):
            return False, f"column {col} has no common value"
    return True, "every column has a common value"
