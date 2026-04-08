"""Easy task bank: query correction and basic optimization."""

from __future__ import annotations

from .base_task import BaseTask, TaskCase


class Task1Basic(BaseTask):
    """Task 1 from the architecture: easy SQL cleanup patterns."""

    task_type = "basic"
    difficulty = "easy"
    schema_name = "ecommerce"
    prompting_strategy = "zero_shot"
    expected_baseline_score = 0.75

    @property
    def cases(self) -> list[TaskCase]:
        return [
            TaskCase(
                case_id="task1-ecommerce-01",
                slow_query="""
                    SELECT *
                    FROM orders
                    WHERE status = 'delivered'
                """.strip(),
                reference_query="""
                    SELECT order_id, user_id, created_at, total_cents
                    FROM orders
                    WHERE status = 'delivered'
                """.strip(),
                optimization_hints=[
                    "Avoid SELECT * and return only required columns.",
                    "Preserve the same rows and ordering semantics.",
                ],
                max_steps=1,
                time_limit_seconds=20.0,
            ),
            TaskCase(
                case_id="task1-ecommerce-02",
                slow_query="""
                    SELECT DISTINCT user_id
                    FROM users
                """.strip(),
                reference_query="""
                    SELECT user_id
                    FROM users
                """.strip(),
                optimization_hints=[
                    "Check whether DISTINCT is redundant on primary keys.",
                    "Return the same result set with less unnecessary work.",
                ],
                max_steps=1,
                time_limit_seconds=20.0,
            ),
            TaskCase(
                case_id="task1-ecommerce-03",
                slow_query="""
                    SELECT *
                    FROM order_items
                    LIMIT 2000
                """.strip(),
                reference_query="""
                    SELECT order_item_id, order_id, product_id, quantity, unit_price_cents
                    FROM order_items
                    LIMIT 2000
                """.strip(),
                optimization_hints=[
                    "Use explicit projection instead of SELECT *.",
                    "Keep LIMIT behavior unchanged.",
                ],
                max_steps=1,
                time_limit_seconds=20.0,
            ),
        ]

