"""Performance grading using timing ratios and EXPLAIN QUERY PLAN analysis."""

from __future__ import annotations

import sqlite3
from typing import Any

from .base_grader import BaseGrader, GraderResult
from ..database import benchmark_query, count_full_table_scans, explain_query_plan


class PerformanceGrader(BaseGrader):
    """Grades speed improvement and plan quality over the baseline query."""

    name = "performance"
    max_score = 0.30

    def __init__(self, runs: int = 5, target_ratio: float = 1.25):
        self.runs = runs
        self.target_ratio = target_ratio

    def grade(
        self,
        conn: sqlite3.Connection,
        *,
        reference_sql: str,
        candidate_sql: str,
        context: dict[str, Any] | None = None,
    ) -> GraderResult:
        baseline_sql = (context or {}).get("baseline_sql", reference_sql)
        params = tuple((context or {}).get("params", ()))

        try:
            baseline_benchmark = benchmark_query(conn, baseline_sql, params=params, runs=self.runs)
            candidate_benchmark = benchmark_query(conn, candidate_sql, params=params, runs=self.runs)
            baseline_plan = explain_query_plan(conn, baseline_sql)
            candidate_plan = explain_query_plan(conn, candidate_sql)
        except sqlite3.Error as exc:
            return GraderResult(
                score=0.0,
                passed=False,
                feedback=["Performance grading could not run because one of the queries failed."],
                metadata={"error": str(exc)},
            )

        baseline_time = max(baseline_benchmark.median_seconds, 1e-9)
        candidate_time = max(candidate_benchmark.median_seconds, 1e-9)
        speedup_ratio = baseline_time / candidate_time

        baseline_scans = count_full_table_scans(baseline_plan)
        candidate_scans = count_full_table_scans(candidate_plan)
        scan_improvement = max(0, baseline_scans - candidate_scans)

        ratio_component = min(speedup_ratio / self.target_ratio, 1.0) * 0.25
        scan_component = 0.05 if candidate_scans < baseline_scans else 0.0
        score = self._clamp_score(ratio_component + scan_component)
        passed = speedup_ratio >= self.target_ratio or candidate_scans < baseline_scans

        feedback: list[str] = [
            f"Baseline median: {baseline_time:.6f}s, candidate median: {candidate_time:.6f}s.",
            f"Measured speedup ratio: {speedup_ratio:.2f}x.",
        ]
        if scan_improvement > 0:
            feedback.append(
                f"Candidate reduced likely full-table scans from {baseline_scans} to {candidate_scans}."
            )
        elif candidate_scans > baseline_scans:
            feedback.append(
                f"Candidate increased likely full-table scans from {baseline_scans} to {candidate_scans}."
            )

        if speedup_ratio < 1.0:
            feedback.append("Candidate query is slower than the baseline.")

        return GraderResult(
            score=score,
            passed=passed,
            feedback=feedback,
            metadata={
                "baseline_sql": baseline_sql,
                "baseline_seconds": baseline_time,
                "candidate_seconds": candidate_time,
                "speedup_ratio": speedup_ratio,
                "baseline_full_scans": baseline_scans,
                "candidate_full_scans": candidate_scans,
                "baseline_plan": baseline_plan,
                "candidate_plan": candidate_plan,
            },
        )

