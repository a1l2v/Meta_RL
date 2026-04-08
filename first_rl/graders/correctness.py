"""Correctness grading based on result-set comparison."""

from __future__ import annotations

import sqlite3
from typing import Any

from .base_grader import BaseGrader, GraderResult
from ..database import compare_query_results, execute_query


class CorrectnessGrader(BaseGrader):
    """Checks whether the candidate query returns the same results as the reference."""

    name = "correctness"
    max_score = 0.40

    def grade(
        self,
        conn: sqlite3.Connection,
        *,
        reference_sql: str,
        candidate_sql: str,
        context: dict[str, Any] | None = None,
    ) -> GraderResult:
        params = tuple((context or {}).get("params", ()))

        try:
            reference_rows = execute_query(conn, reference_sql, params)
        except sqlite3.Error as exc:
            return GraderResult(
                score=0.0,
                passed=False,
                feedback=["Reference query failed to execute."],
                metadata={"error": str(exc)},
            )

        try:
            candidate_rows = execute_query(conn, candidate_sql, params)
        except sqlite3.Error as exc:
            return GraderResult(
                score=0.0,
                passed=False,
                feedback=["Candidate query failed to execute."],
                metadata={"error": str(exc)},
            )

        matches = compare_query_results(conn, reference_sql, candidate_sql, params)
        feedback = (
            ["Candidate query matches the reference result set."]
            if matches
            else ["Candidate query does not match the reference result set."]
        )
        return GraderResult(
            score=self.max_score if matches else 0.0,
            passed=matches,
            feedback=feedback,
            metadata={
                "reference_row_count": len(reference_rows),
                "candidate_row_count": len(candidate_rows),
            },
        )

