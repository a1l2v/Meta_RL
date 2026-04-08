"""Core single-step SQL optimization environment with reset/step/state orchestration."""

from __future__ import annotations

import random
import re
import sqlite3
from typing import Any, Literal
from uuid import uuid4

from openenv.core.env_server.interfaces import Environment
from openenv.core.env_server.types import State

from .database import initialize_database
from .graders import CorrectnessGrader, GraderResult, PerformanceGrader, StyleGrader
from .models import FirstRlAction, FirstRlObservation, RewardBreakdown
from .tasks import BaseTask, Task1Basic, Task2Joins, Task3Complex

TaskType = Literal["basic", "join_opt", "complex"]


class SqlEnv(Environment[FirstRlAction, FirstRlObservation, State]):
    """Orchestration layer for SQL optimization tasks."""

    SUPPORTS_CONCURRENT_SESSIONS: bool = True

    def __init__(self):
        super().__init__()
        self._rng = random.Random(7)
        self._tasks: dict[TaskType, BaseTask] = {
            "basic": Task1Basic(),
            "join_opt": Task2Joins(),
            "complex": Task3Complex(),
        }
        self._correctness = CorrectnessGrader()
        self._performance = PerformanceGrader()
        self._style = StyleGrader()

        self._current_task: BaseTask | None = None
        self._current_observation: FirstRlObservation | None = None
        self._db: sqlite3.Connection | None = None
        self._done: bool = False
        self._state = State(episode_id=None, step_count=0)

    def reset(
        self,
        seed: int | None = None,
        episode_id: str | None = None,
        **kwargs: Any,
    ) -> FirstRlObservation:
        """Reset the environment with a fresh SQLite DB and sampled task case."""
        if seed is not None:
            self._rng.seed(seed)

        if self._db is not None:
            self._db.close()
            self._db = None

        task_name = kwargs.get("task_type")
        task = self._choose_task(task_name)
        case = task.sample_case(self._rng)
        self._db = initialize_database(task.schema_name)
        observation = task.build_observation(self._db, case, step_number=0)
        observation.done = False
        observation.reward = 0.0

        self._current_task = task
        self._current_observation = observation
        self._done = False
        self._state = State(
            episode_id=episode_id or str(uuid4()),
            step_count=0,
            done=self._done,
            task_id=observation.task_id,
            task_type=observation.task_type,
            difficulty=observation.difficulty,
            current_observation=observation.model_dump(by_alias=True),
        )
        return observation

    def step(
        self,
        action: FirstRlAction,
        timeout_s: float | None = None,
        **kwargs: Any,
    ) -> FirstRlObservation:
        """Apply one action and run grading pipeline with short-circuit rules."""
        del timeout_s, kwargs
        if self._current_task is None or self._current_observation is None or self._db is None:
            raise RuntimeError("Environment must be reset before step().")

        if self._done:
            return self._current_observation.model_copy(
                update={
                    "done": True,
                    "feedback": "Episode already finished. Call reset() to start a new task.",
                }
            )

        reference_sql = str(self._current_observation.metadata.get("reference_query", ""))
        candidate_sql = action.optimized_query.strip()

        if not candidate_sql:
            return self._finish_episode(
                reward=0.0,
                breakdown=RewardBreakdown(total=0.0),
                feedback="Empty optimized_query. Please provide a SQL SELECT statement.",
                last_submission_valid=False,
                action=action,
            )

        # 1) Syntax check -> fail? return immediately
        syntax_error = self._check_syntax(candidate_sql)
        if syntax_error is not None:
            return self._finish_episode(
                reward=0.0,
                breakdown=RewardBreakdown(total=0.0),
                feedback=f"Syntax check failed: {syntax_error}",
                last_submission_valid=False,
                action=action,
            )

        # 2) Safety check -> fail? return immediately
        safety_issue = self._check_safety(candidate_sql)
        if safety_issue is not None:
            return self._finish_episode(
                reward=-0.50,
                breakdown=RewardBreakdown(total=-0.50),
                feedback=f"Safety check failed: {safety_issue}",
                last_submission_valid=False,
                action=action,
            )

        # 3) correctness -> always run
        correctness = self._correctness.grade(
            self._db,
            reference_sql=reference_sql,
            candidate_sql=candidate_sql,
            context={"baseline_sql": self._current_observation.slow_query},
        )

        # 4) performance -> only if correctness > 0
        performance = GraderResult(score=0.0, passed=False, feedback=["Performance skipped (correctness=0)."])
        if correctness.score > 0.0:
            performance = self._performance.grade(
                self._db,
                reference_sql=reference_sql,
                candidate_sql=candidate_sql,
                context={"baseline_sql": self._current_observation.slow_query},
            )

        # 5) style -> always run (independent of correctness)
        style = self._safe_style_grade(reference_sql, candidate_sql)

        # 6) explanation -> only for complex task
        explanation_score, explanation_feedback = self._grade_explanation(
            action.explanation, task_type=self._current_observation.task_type
        )

        breakdown = RewardBreakdown(
            correctness=correctness.score,
            performance=performance.score,
            style=style.score,
            explanation=explanation_score,
            total=correctness.score + performance.score + style.score + explanation_score,
        )

        feedback_lines = []
        feedback_lines.extend(correctness.feedback)
        feedback_lines.extend(performance.feedback)
        feedback_lines.extend(style.feedback)
        if explanation_feedback:
            feedback_lines.append(explanation_feedback)

        return self._finish_episode(
            reward=breakdown.total,
            breakdown=breakdown,
            feedback=" | ".join(feedback_lines),
            last_submission_valid=True,
            action=action,
            grader_metadata={
                "correctness": correctness.metadata,
                "performance": performance.metadata,
                "style": style.metadata,
            },
        )

    @property
    def state(self) -> State:
        """Return current episode state for the `/state` endpoint."""
        return self._state

    def _choose_task(self, task_name: str | None) -> BaseTask:
        if task_name is None:
            return self._rng.choice(list(self._tasks.values()))
        if task_name not in self._tasks:
            allowed = ", ".join(sorted(self._tasks))
            raise ValueError(f"Unknown task_type '{task_name}'. Expected one of: {allowed}")
        return self._tasks[task_name]

    def _check_syntax(self, sql: str) -> str | None:
        try:
            assert self._db is not None
            self._db.execute(f"EXPLAIN QUERY PLAN {sql}").fetchall()
            return None
        except sqlite3.Error as exc:
            return str(exc)

    def _check_safety(self, sql: str) -> str | None:
        pattern = re.compile(
            r"\b(DELETE|UPDATE|INSERT|DROP|ALTER|CREATE|REINDEX|VACUUM|ATTACH|DETACH|PRAGMA)\b",
            flags=re.IGNORECASE,
        )
        if pattern.search(sql):
            return "write or DDL operation detected"
        if ";" in sql:
            return "multiple statements are not allowed"
        return None

    def _safe_style_grade(self, reference_sql: str, candidate_sql: str) -> GraderResult:
        try:
            assert self._db is not None
            return self._style.grade(
                self._db,
                reference_sql=reference_sql,
                candidate_sql=candidate_sql,
                context={"baseline_sql": self._current_observation.slow_query if self._current_observation else None},
            )
        except Exception as exc:
            return GraderResult(
                score=0.0,
                passed=False,
                feedback=[f"Style grading unavailable: {exc}"],
                metadata={"error": str(exc)},
            )

    def _grade_explanation(self, explanation: str | None, *, task_type: str) -> tuple[float, str]:
        if task_type != "complex":
            return 0.0, ""
        if not explanation:
            return 0.0, "Explanation missing for complex task."

        text = explanation.strip().lower()
        score = 0.0
        if len(text) >= 40:
            score += 0.04
        keywords = ["join", "index", "filter", "group", "subquery", "scan", "cost", "where"]
        covered = sum(1 for key in keywords if key in text)
        score += min(0.06, covered * 0.015)
        score = min(0.10, score)
        return score, f"Explanation quality score: {score:.2f}/0.10."

    def _finish_episode(
        self,
        *,
        reward: float,
        breakdown: RewardBreakdown,
        feedback: str,
        last_submission_valid: bool,
        action: FirstRlAction,
        grader_metadata: dict[str, Any] | None = None,
    ) -> FirstRlObservation:
        assert self._current_observation is not None
        observation = self._current_observation.model_copy(
            update={
                "done": True,
                "reward": reward,
                "reward_breakdown": breakdown,
                "step_number": 1,
                "last_submission_valid": last_submission_valid,
                "feedback": feedback,
                "metadata": {
                    **self._current_observation.metadata,
                    "submitted_query": action.optimized_query,
                    "index_suggestions": action.index_suggestions,
                    "explanation": action.explanation,
                    "grader_metadata": grader_metadata or {},
                },
            }
        )
        self._current_observation = observation
        self._done = True
        self._state.step_count = 1
        self._state.done = True
        self._state.last_reward = reward
        self._state.last_submission_valid = last_submission_valid
        self._state.current_observation = observation.model_dump(by_alias=True)
        return observation
