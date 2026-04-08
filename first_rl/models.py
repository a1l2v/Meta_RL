# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Typed models for the SQL query optimization environment."""

from __future__ import annotations

from typing import Any, Literal

from openenv.core.env_server.types import Action, Observation
from pydantic import BaseModel, ConfigDict, Field


class TableColumn(BaseModel):
    """Describes one column in a table schema."""

    name: str = Field(..., description="Column name")
    type: str = Field(..., description="SQLite type for the column")
    primary_key: bool = Field(default=False, description="Whether this column is part of the primary key")
    nullable: bool = Field(default=True, description="Whether this column allows NULL values")
    default: Any | None = Field(default=None, description="Default value for the column, if any")


class TableSchema(BaseModel):
    """Summarized schema information for a single table."""

    table_name: str = Field(..., description="Table name")
    row_count: int = Field(default=0, description="Approximate or exact row count")
    columns: list[TableColumn] = Field(default_factory=list, description="Columns defined on the table")
    sample_rows: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Small sample of rows to help the agent understand the data",
    )


class ExecutionPlan(BaseModel):
    """Lightweight EXPLAIN QUERY PLAN summary."""

    raw_plan: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Raw EXPLAIN QUERY PLAN rows",
    )
    estimated_cost: float | None = Field(
        default=None,
        description="Optional derived cost estimate if one is available",
    )
    full_scan_count: int = Field(
        default=0,
        description="Number of likely full-table scans in the plan",
    )


class RewardBreakdown(BaseModel):
    """Reward breakdown from the architecture document."""

    correctness: float = Field(default=0.0, ge=0.0, le=0.40)
    performance: float = Field(default=0.0, ge=-1.0, le=0.30)
    style: float = Field(default=0.0, ge=0.0, le=0.20)
    explanation: float = Field(default=0.0, ge=0.0, le=0.10)
    total: float = Field(default=0.0, ge=-1.0, le=1.0)


class FirstRlAction(Action):
    """Action submitted by the agent for SQL optimization tasks."""

    optimized_query: str = Field(
        default="",
        description="The rewritten SQL query the agent wants to submit",
    )
    index_suggestions: list[str] = Field(
        default_factory=list,
        description="Optional CREATE INDEX statements proposed by the agent",
    )
    explanation: str | None = Field(
        default=None,
        description="Optional reasoning for the rewrite, mainly used for hard tasks",
    )


class FirstRlObservation(Observation):
    """Observation presented to the agent for SQL optimization tasks."""

    model_config = ConfigDict(populate_by_name=True)

    task_id: str = Field(..., description="Stable identifier for the current task")
    task_type: Literal["basic", "join_opt", "complex"] = Field(
        ...,
        description="Task family from the architecture plan",
    )
    difficulty: Literal["easy", "medium", "hard"] = Field(
        ...,
        description="Difficulty level of the current task",
    )
    slow_query: str = Field(..., description="The intentionally slow or poor SQL query to optimize")
    schema_info: list[TableSchema] = Field(
        default_factory=list,
        alias="schema",
        serialization_alias="schema",
        description="Database schema summary with columns, row counts, and samples",
    )
    execution_plan: ExecutionPlan | None = Field(
        default=None,
        description="Optional EXPLAIN QUERY PLAN summary for the slow query",
    )
    optimization_hints: list[str] = Field(
        default_factory=list,
        description="Optional hints such as candidate indexes or anti-pattern reminders",
    )
    step_number: int = Field(default=0, ge=0, description="Current step within the episode")
    max_steps: int = Field(default=1, ge=1, description="Maximum steps allowed in the episode")
    time_limit_seconds: float = Field(
        default=30.0,
        gt=0.0,
        description="Per-step time budget for the task",
    )
    reward_breakdown: RewardBreakdown | None = Field(
        default=None,
        description="Optional detailed reward from the previous evaluation",
    )
    last_submission_valid: bool | None = Field(
        default=None,
        description="Whether the last submitted SQL passed basic validation",
    )
    feedback: str | None = Field(
        default=None,
        description="Optional grader or environment feedback for the agent",
    )
