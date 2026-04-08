"""Abstract base types and helpers for SQL optimization tasks."""

from __future__ import annotations

import random
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal

from ..database import count_full_table_scans, explain_query_plan, get_schema_summary
from ..models import ExecutionPlan, FirstRlObservation, TableColumn, TableSchema


PromptStrategy = Literal["zero_shot", "cot", "few_shot_json"]
TaskType = Literal["basic", "join_opt", "complex"]
Difficulty = Literal["easy", "medium", "hard"]


@dataclass(frozen=True)
class TaskCase:
    """One query-optimization challenge item."""

    case_id: str
    slow_query: str
    reference_query: str
    optimization_hints: list[str]
    max_steps: int = 1
    time_limit_seconds: float = 30.0


class BaseTask(ABC):
    """Shared interface for all SQL optimization task families."""

    task_type: TaskType
    difficulty: Difficulty
    schema_name: str
    prompting_strategy: PromptStrategy
    expected_baseline_score: float

    @property
    @abstractmethod
    def cases(self) -> list[TaskCase]:
        """Return all task cases for this task family."""

    def sample_case(self, rng: random.Random | None = None) -> TaskCase:
        """Sample one case from the task bank."""
        task_rng = rng or random.Random(7)
        return task_rng.choice(self.cases)

    def build_observation(
        self,
        conn: sqlite3.Connection,
        case: TaskCase,
        *,
        step_number: int = 0,
        feedback: str | None = None,
        last_submission_valid: bool | None = None,
    ) -> FirstRlObservation:
        """Construct an observation payload from the task case and live DB state."""
        plan_rows = explain_query_plan(conn, case.slow_query)
        execution_plan = ExecutionPlan(
            raw_plan=plan_rows,
            full_scan_count=count_full_table_scans(plan_rows),
        )
        schema_info = _build_schema_models(get_schema_summary(conn))

        return FirstRlObservation(
            task_id=case.case_id,
            task_type=self.task_type,
            difficulty=self.difficulty,
            slow_query=case.slow_query,
            schema=schema_info,
            execution_plan=execution_plan,
            optimization_hints=case.optimization_hints,
            step_number=step_number,
            max_steps=case.max_steps,
            time_limit_seconds=case.time_limit_seconds,
            last_submission_valid=last_submission_valid,
            feedback=feedback,
            metadata={
                "schema_name": self.schema_name,
                "prompting_strategy": self.prompting_strategy,
                "expected_baseline_score": self.expected_baseline_score,
                "reference_query": case.reference_query,
            },
        )

    def as_dict(self) -> dict[str, Any]:
        """Return a compact summary for introspection or debugging."""
        return {
            "task_type": self.task_type,
            "difficulty": self.difficulty,
            "schema_name": self.schema_name,
            "prompting_strategy": self.prompting_strategy,
            "expected_baseline_score": self.expected_baseline_score,
            "num_cases": len(self.cases),
        }


def _build_schema_models(raw_schema: list[dict[str, Any]]) -> list[TableSchema]:
    out: list[TableSchema] = []
    for table in raw_schema:
        columns = [
            TableColumn(
                name=col["name"],
                type=col.get("type", ""),
                primary_key=bool(col.get("primary_key", False)),
                nullable=not bool(col.get("notnull", False)),
                default=col.get("default"),
            )
            for col in table.get("columns", [])
        ]
        out.append(
            TableSchema(
                table_name=table["table_name"],
                row_count=int(table.get("row_count", 0)),
                columns=columns,
                sample_rows=table.get("sample_rows", []),
            )
        )
    return out

