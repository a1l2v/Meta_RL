"""Medium task bank: join optimization and subquery rewrites."""

from __future__ import annotations

from .base_task import BaseTask, TaskCase


class Task2Joins(BaseTask):
    """Task 2 from the architecture: join order and subquery patterns."""

    task_type = "join_opt"
    difficulty = "medium"
    schema_name = "ecommerce"
    prompting_strategy = "cot"
    expected_baseline_score = 0.50

    @property
    def cases(self) -> list[TaskCase]:
        return [
            TaskCase(
                case_id="task2-ecommerce-01",
                slow_query="""
                    SELECT
                        o.order_id,
                        o.user_id,
                        o.total_cents
                    FROM orders o
                    WHERE o.order_id IN (
                        SELECT oi.order_id
                        FROM order_items oi
                        WHERE oi.quantity >= 3
                    )
                """.strip(),
                reference_query="""
                    SELECT
                        DISTINCT o.order_id,
                        o.user_id,
                        o.total_cents
                    FROM orders o
                    JOIN order_items oi ON oi.order_id = o.order_id
                    WHERE oi.quantity >= 3
                """.strip(),
                optimization_hints=[
                    "Consider replacing IN (SELECT ...) with a JOIN or EXISTS.",
                    "Avoid changing result semantics while reducing repeated scans.",
                ],
                max_steps=2,
                time_limit_seconds=30.0,
            ),
            TaskCase(
                case_id="task2-ecommerce-02",
                slow_query="""
                    SELECT
                        u.user_id,
                        u.full_name,
                        (
                            SELECT COUNT(*)
                            FROM orders o
                            WHERE o.user_id = u.user_id AND o.status = 'delivered'
                        ) AS delivered_orders
                    FROM users u
                    WHERE u.country = 'US'
                """.strip(),
                reference_query="""
                    SELECT
                        u.user_id,
                        u.full_name,
                        COUNT(o.order_id) AS delivered_orders
                    FROM users u
                    LEFT JOIN orders o
                        ON o.user_id = u.user_id AND o.status = 'delivered'
                    WHERE u.country = 'US'
                    GROUP BY u.user_id, u.full_name
                """.strip(),
                optimization_hints=[
                    "Correlated subqueries can be expensive at scale.",
                    "Try using a grouped JOIN instead of row-by-row counting.",
                ],
                max_steps=2,
                time_limit_seconds=30.0,
            ),
            TaskCase(
                case_id="task2-ecommerce-03",
                slow_query="""
                    SELECT
                        p.product_id,
                        p.product_name,
                        (
                            SELECT SUM(oi.quantity)
                            FROM order_items oi
                            WHERE oi.product_id = p.product_id
                        ) AS units_sold
                    FROM products p
                    WHERE p.active = 1
                """.strip(),
                reference_query="""
                    SELECT
                        p.product_id,
                        p.product_name,
                        COALESCE(SUM(oi.quantity), 0) AS units_sold
                    FROM products p
                    LEFT JOIN order_items oi ON oi.product_id = p.product_id
                    WHERE p.active = 1
                    GROUP BY p.product_id, p.product_name
                """.strip(),
                optimization_hints=[
                    "Replace repeated correlated aggregates with JOIN + GROUP BY.",
                    "Ensure products with zero sales are still returned.",
                ],
                max_steps=2,
                time_limit_seconds=30.0,
            ),
        ]

