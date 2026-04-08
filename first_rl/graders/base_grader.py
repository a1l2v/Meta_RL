"""Abstract grader types shared by all SQL environment graders."""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field


class GraderResult(BaseModel):
    """Normalized result returned by each grader."""

    score: float = Field(..., description="Score contribution from this grader")
    passed: bool = Field(..., description="Whether the submission met the grader's main criterion")
    feedback: list[str] = Field(default_factory=list, description="Human-readable grading notes")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Structured grader details")


class BaseGrader(ABC):
    """Abstract base class for SQL query graders."""

    name: str = "base"
    max_score: float = 0.0

    @abstractmethod
    def grade(
        self,
        conn: sqlite3.Connection,
        *,
        reference_sql: str,
        candidate_sql: str,
        context: dict[str, Any] | None = None,
    ) -> GraderResult:
        """Grade a candidate query against a reference query."""

    def _clamp_score(self, score: float) -> float:
        return max(0.0, min(self.max_score, score))

