"""Deterministic SQL grader that compares agent output to expected results."""

import sqlite3
from typing import Any, Dict, List, Tuple


def grade_sql(
    conn: sqlite3.Connection,
    agent_sql: str,
    expected_sql: str,
    expected_columns: List[str],
    order_matters: bool = True,
) -> Tuple[float, str]:
    """
    Grade an agent's SQL query against the expected result.

    Returns (score, explanation) where score is in (0.0, 1.0) exclusive:
        0.01 - SQL syntax error or runtime error
        0.1  - SQL executes but returns empty result when non-empty expected
        0.3  - Wrong number of columns returned
        0.5  - Correct column count but wrong values
        0.7  - Most values correct but some rows missing or extra
        0.9  - All values correct but wrong row ordering (when order matters)
        0.99 - Exact match
    """
    # Execute expected SQL to get ground truth
    try:
        expected_rows, expected_cols = _execute_query(conn, expected_sql)
    except Exception as e:
        return 0.01, f"Internal error executing expected SQL: {e}"

    # Execute agent SQL
    try:
        agent_rows, agent_cols = _execute_query(conn, agent_sql)
    except sqlite3.OperationalError as e:
        error_msg = str(e).lower()
        if "no such table" in error_msg or "no such column" in error_msg:
            return 0.01, f"SQL references invalid table or column: {e}"
        return 0.01, f"SQL execution error: {e}"
    except Exception as e:
        return 0.01, f"SQL error: {e}"

    # Check empty result
    if not agent_rows and expected_rows:
        return 0.1, "Query returned no rows but expected results exist"

    if not expected_rows and not agent_rows:
        return 0.99, "Both queries correctly return empty results"

    # Check column count
    if len(agent_cols) != len(expected_cols):
        return 0.3, (
            f"Wrong number of columns: got {len(agent_cols)} ({agent_cols}), "
            f"expected {len(expected_cols)} ({expected_columns})"
        )

    # Normalize both result sets for comparison
    agent_normalized = _normalize_rows(agent_rows)
    expected_normalized = _normalize_rows(expected_rows)

    # Check exact match (with order)
    if agent_normalized == expected_normalized:
        return 0.99, "Exact match"

    # Check match ignoring order
    agent_sorted = sorted(agent_normalized)
    expected_sorted = sorted(expected_normalized)

    if agent_sorted == expected_sorted:
        if order_matters:
            return 0.9, "Correct values but wrong row ordering"
        return 0.99, "Exact match (order not required)"

    # Check for partial matches - how many expected rows appear in agent result
    agent_set = set(agent_normalized)
    expected_set = set(expected_normalized)

    matching_rows = agent_set & expected_set
    match_ratio = len(matching_rows) / len(expected_set) if expected_set else 0

    if match_ratio >= 0.8:
        return 0.7, (
            f"Most rows correct ({len(matching_rows)}/{len(expected_set)}), "
            f"but some rows missing or extra"
        )

    if match_ratio >= 0.3:
        return 0.5, (
            f"Some correct rows ({len(matching_rows)}/{len(expected_set)}), "
            f"significant differences in results"
        )

    # Check if at least the row count is similar
    if abs(len(agent_rows) - len(expected_rows)) <= 1:
        agent_values = set()
        for row in agent_normalized:
            agent_values.update(row)
        expected_values = set()
        for row in expected_normalized:
            expected_values.update(row)

        value_overlap = len(agent_values & expected_values) / len(expected_values) if expected_values else 0
        if value_overlap >= 0.5:
            return 0.5, "Correct row count with some matching values but different structure"

    return 0.3, (
        f"Query returned {len(agent_rows)} rows with {len(agent_cols)} columns "
        f"but results don't match (expected {len(expected_rows)} rows)"
    )


def _execute_query(
    conn: sqlite3.Connection, sql: str
) -> Tuple[List[Tuple], List[str]]:
    """Execute SQL and return (rows, column_names)."""
    cursor = conn.execute(sql)
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    return rows, col_names


def _normalize_rows(rows: List[Tuple]) -> List[Tuple]:
    """Normalize values for comparison (handle float precision)."""
    normalized = []
    for row in rows:
        norm_row = tuple(_normalize_value(v) for v in row)
        normalized.append(norm_row)
    return normalized


def _normalize_value(value: Any) -> Any:
    """Normalize a single value for comparison."""
    if value is None:
        return None
    if isinstance(value, float):
        return round(value, 2)
    if isinstance(value, str):
        return value.strip()
    return value
