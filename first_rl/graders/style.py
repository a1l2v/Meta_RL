"""Style grader using SQL AST inspection for common anti-patterns."""

from __future__ import annotations

import sqlite3
from typing import Any

from .base_grader import BaseGrader, GraderResult


class StyleGrader(BaseGrader):
    """Grades whether the candidate removed anti-patterns from the query."""

    name = "style"
    max_score = 0.20

    def grade(
        self,
        conn: sqlite3.Connection,
        *,
        reference_sql: str,
        candidate_sql: str,
        context: dict[str, Any] | None = None,
    ) -> GraderResult:
        del conn, reference_sql  # Style grading is AST-based and does not need the database connection.
        sqlglot = _require_sqlglot()
        exp = sqlglot.exp

        try:
            candidate_ast = sqlglot.parse_one(candidate_sql, read="sqlite")
        except Exception as exc:
            return GraderResult(
                score=0.0,
                passed=False,
                feedback=["Candidate query could not be parsed for style analysis."],
                metadata={"error": str(exc)},
            )

        context = context or {}
        baseline_ast = None
        baseline_sql = context.get("baseline_sql")
        if baseline_sql:
            try:
                baseline_ast = sqlglot.parse_one(baseline_sql, read="sqlite")
            except Exception:
                baseline_ast = None

        candidate_flags = _detect_antipatterns(candidate_ast, exp)
        baseline_flags = _detect_antipatterns(baseline_ast, exp) if baseline_ast is not None else {}

        feedback: list[str] = []
        score = 0.0

        if not candidate_flags["select_star"]:
            score += 0.10
            feedback.append("Candidate avoids SELECT *.")
        else:
            feedback.append("Candidate still uses SELECT *.")

        if not candidate_flags["distinct_on_primary_key"]:
            score += 0.03
        else:
            feedback.append("Candidate still uses DISTINCT, which may be redundant.")

        if not candidate_flags["correlated_subquery"]:
            score += 0.04
        elif baseline_flags.get("correlated_subquery"):
            feedback.append("Candidate still contains a correlated subquery.")

        if not candidate_flags["in_subquery"]:
            score += 0.03
        elif baseline_flags.get("in_subquery"):
            feedback.append("Candidate still uses IN (SELECT ...).")

        score = self._clamp_score(score)
        passed = score >= 0.10

        return GraderResult(
            score=score,
            passed=passed,
            feedback=feedback or ["Candidate query avoids the style anti-patterns checked by this grader."],
            metadata={"candidate_flags": candidate_flags, "baseline_flags": baseline_flags},
        )


def _require_sqlglot():
    try:
        import sqlglot
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "sqlglot is required for StyleGrader. Install project dependencies to enable AST-based grading."
        ) from exc
    return sqlglot


def _detect_antipatterns(ast: Any, exp: Any) -> dict[str, bool]:
    if ast is None:
        return {
            "select_star": False,
            "correlated_subquery": False,
            "in_subquery": False,
            "distinct_on_primary_key": False,
        }

    select_star = any(isinstance(node, exp.Star) for node in ast.find_all(exp.Star))

    correlated_subquery = False
    for subquery in ast.find_all(exp.Subquery):
        inner_tables = {table.alias_or_name for table in subquery.find_all(exp.Table)}
        for column in subquery.find_all(exp.Column):
            if column.table and column.table not in inner_tables:
                correlated_subquery = True
                break
        if correlated_subquery:
            break

    in_subquery = any(
        isinstance(node, exp.In) and isinstance(getattr(node, "expression", None), exp.Subquery)
        for node in ast.walk()
    )

    distinct_on_primary_key = False
    for select in ast.find_all(exp.Select):
        if select.args.get("distinct") and len(select.expressions) == 1:
            expr = select.expressions[0]
            if isinstance(expr, exp.Column) and expr.name.lower().endswith("_id"):
                distinct_on_primary_key = True
                break

    return {
        "select_star": select_star,
        "correlated_subquery": correlated_subquery,
        "in_subquery": in_subquery,
        "distinct_on_primary_key": distinct_on_primary_key,
    }

