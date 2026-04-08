"""SQLite setup and query helpers for the SQL optimization environment."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

from .seed_data import SeedConfig, populate_schema

SCHEMA_DIR = Path(__file__).resolve().parent / "data" / "schemas"
SUPPORTED_SCHEMAS = ("ecommerce", "analytics", "hr")


@dataclass(frozen=True)
class QueryBenchmark:
    """Benchmark result for a single SQL query."""

    sql: str
    runs: int
    median_seconds: float
    timings_seconds: list[float]
    row_count: int


def create_connection(database_path: str = ":memory:") -> sqlite3.Connection:
    """Create a SQLite connection with rows exposed as mappings."""
    conn = sqlite3.connect(database_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def load_schema_sql(schema_name: str) -> str:
    """Load the SQL DDL for a named schema."""
    if schema_name not in SUPPORTED_SCHEMAS:
        raise ValueError(f"Unsupported schema '{schema_name}'. Expected one of {SUPPORTED_SCHEMAS}.")
    schema_path = SCHEMA_DIR / f"{schema_name}.sql"
    return schema_path.read_text(encoding="utf-8")


def initialize_database(
    schema_name: str = "ecommerce",
    database_path: str = ":memory:",
    seed: bool = True,
    seed_config: SeedConfig | None = None,
) -> sqlite3.Connection:
    """Create, initialize, and optionally seed a database for one schema."""
    conn = create_connection(database_path)
    conn.executescript(load_schema_sql(schema_name))
    if seed:
        populate_schema(conn, schema_name=schema_name, config=seed_config)
    conn.commit()
    return conn


def execute_query(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """Execute a read query and return JSON-serializable rows."""
    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def execute_script(conn: sqlite3.Connection, sql: str) -> None:
    """Run a SQL script, committing after success."""
    conn.executescript(sql)
    conn.commit()


def explain_query_plan(conn: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    """Return SQLite EXPLAIN QUERY PLAN output as dictionaries."""
    cursor = conn.execute(f"EXPLAIN QUERY PLAN {sql}")
    columns = ["id", "parent", "notused", "detail"]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def count_full_table_scans(plan_rows: list[dict[str, Any]]) -> int:
    """Count scan steps that likely indicate full table scans."""
    count = 0
    for row in plan_rows:
        detail = str(row.get("detail", "")).upper()
        if "SCAN " in detail and "USING INDEX" not in detail and "USING COVERING INDEX" not in detail:
            count += 1
    return count


def benchmark_query(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] = (),
    runs: int = 5,
    warmup_runs: int = 1,
) -> QueryBenchmark:
    """Benchmark a query with warmup runs and a median timing."""
    if runs < 1:
        raise ValueError("runs must be at least 1")

    for _ in range(warmup_runs):
        conn.execute(sql, params).fetchall()

    timings: list[float] = []
    row_count = 0
    for _ in range(runs):
        start = time.perf_counter()
        rows = conn.execute(sql, params).fetchall()
        timings.append(time.perf_counter() - start)
        row_count = len(rows)

    return QueryBenchmark(
        sql=sql,
        runs=runs,
        median_seconds=median(timings),
        timings_seconds=timings,
        row_count=row_count,
    )


def compare_query_results(
    conn: sqlite3.Connection,
    reference_sql: str,
    candidate_sql: str,
    params: tuple[Any, ...] = (),
) -> bool:
    """Compare two result sets as sorted tuples."""
    left = _normalize_result(conn.execute(reference_sql, params).fetchall())
    right = _normalize_result(conn.execute(candidate_sql, params).fetchall())
    return left == right


def get_table_row_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Return row counts for all user tables."""
    table_names = [
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]
    return {table_name: conn.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()["count"] for table_name in table_names}


def get_schema_summary(conn: sqlite3.Connection, sample_rows: int = 3) -> list[dict[str, Any]]:
    """Describe tables, columns, row counts, and a few sample rows."""
    summary: list[dict[str, Any]] = []
    for table_name in sorted(get_table_row_counts(conn)):
        columns = [
            {
                "cid": row["cid"],
                "name": row["name"],
                "type": row["type"],
                "notnull": bool(row["notnull"]),
                "default": row["dflt_value"],
                "primary_key": bool(row["pk"]),
            }
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        ]
        sample = [
            dict(row)
            for row in conn.execute(f"SELECT * FROM {table_name} LIMIT {sample_rows}").fetchall()
        ]
        summary.append(
            {
                "table_name": table_name,
                "row_count": conn.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()["count"],
                "columns": columns,
                "sample_rows": sample,
            }
        )
    return summary


def _normalize_result(rows: list[sqlite3.Row]) -> list[tuple[Any, ...]]:
    normalized = [tuple(row) for row in rows]
    normalized.sort()
    return normalized

