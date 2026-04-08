"""Hard task bank: full query restructuring."""

from __future__ import annotations

from .base_task import BaseTask, TaskCase


class Task3Complex(BaseTask):
    """Task 3 from the architecture: full rewrite with multi-pattern cleanup."""

    task_type = "complex"
    difficulty = "hard"
    schema_name = "ecommerce"
    prompting_strategy = "few_shot_json"
    expected_baseline_score = 0.25

    @property
    def cases(self) -> list[TaskCase]:
        return [
            TaskCase(
                case_id="task3-ecommerce-01",
                slow_query="""
                    SELECT DISTINCT
                        u.user_id,
                        u.full_name,
                        (
                            SELECT COUNT(*)
                            FROM orders o2
                            WHERE o2.user_id = u.user_id
                        ) AS total_orders,
                        (
                            SELECT SUM(o3.total_cents)
                            FROM orders o3
                            WHERE o3.user_id = u.user_id
                        ) AS total_spent_cents
                    FROM users u
                    JOIN orders o ON o.user_id = u.user_id
                    WHERE u.user_id IN (
                        SELECT o4.user_id
                        FROM orders o4
                        WHERE o4.status = 'delivered'
                    )
                """.strip(),
                reference_query="""
                    WITH user_order_stats AS (
                        SELECT
                            o.user_id,
                            COUNT(*) AS total_orders,
                            SUM(o.total_cents) AS total_spent_cents,
                            SUM(CASE WHEN o.status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders
                        FROM orders o
                        GROUP BY o.user_id
                    )
                    SELECT
                        u.user_id,
                        u.full_name,
                        s.total_orders,
                        s.total_spent_cents
                    FROM users u
                    JOIN user_order_stats s ON s.user_id = u.user_id
                    WHERE s.delivered_orders > 0
                """.strip(),
                optimization_hints=[
                    "Eliminate redundant DISTINCT and repeated correlated subqueries.",
                    "Prefer one aggregated pass over orders rather than multiple scans.",
                    "Keep business semantics intact.",
                ],
                max_steps=3,
                time_limit_seconds=45.0,
            ),
            TaskCase(
                case_id="task3-ecommerce-02",
                slow_query="""
                    SELECT
                        p.product_id,
                        p.product_name,
                        (
                            SELECT COUNT(*)
                            FROM order_items oi
                            WHERE oi.product_id = p.product_id
                        ) AS line_count,
                        (
                            SELECT SUM(oi2.quantity * oi2.unit_price_cents)
                            FROM order_items oi2
                            WHERE oi2.product_id = p.product_id
                        ) AS gross_revenue_cents
                    FROM products p
                    WHERE p.product_id IN (
                        SELECT oi3.product_id
                        FROM order_items oi3
                    )
                    ORDER BY gross_revenue_cents DESC
                    LIMIT 100
                """.strip(),
                reference_query="""
                    SELECT
                        p.product_id,
                        p.product_name,
                        COUNT(oi.order_item_id) AS line_count,
                        SUM(oi.quantity * oi.unit_price_cents) AS gross_revenue_cents
                    FROM products p
                    JOIN order_items oi ON oi.product_id = p.product_id
                    GROUP BY p.product_id, p.product_name
                    ORDER BY gross_revenue_cents DESC
                    LIMIT 100
                """.strip(),
                optimization_hints=[
                    "Collapse repeated per-product subqueries into one aggregate query.",
                    "Use JOIN + GROUP BY for ranking top products.",
                ],
                max_steps=3,
                time_limit_seconds=45.0,
            ),
        ]

