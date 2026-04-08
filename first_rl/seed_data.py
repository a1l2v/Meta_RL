"""Deterministic seed helpers for SQLite schemas used by the SQL environment."""

from __future__ import annotations

import random
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class SeedConfig:
    """Configures deterministic dataset sizes."""

    seed: int = 7
    users: int = 1000
    products: int = 250
    orders: int = 5000
    max_items_per_order: int = 5
    accounts: int = 120
    sessions: int = 4000
    max_page_views_per_session: int = 8
    employees: int = 600


def populate_schema(conn: sqlite3.Connection, schema_name: str, config: SeedConfig | None = None) -> None:
    """Populate the selected schema with deterministic sample data."""
    cfg = config or SeedConfig()
    rng = random.Random(cfg.seed)

    if schema_name == "ecommerce":
        _seed_ecommerce(conn, cfg, rng)
        return
    if schema_name == "analytics":
        _seed_analytics(conn, cfg, rng)
        return
    if schema_name == "hr":
        _seed_hr(conn, cfg, rng)
        return
    raise ValueError(f"Unsupported schema '{schema_name}'")


def _seed_ecommerce(conn: sqlite3.Connection, cfg: SeedConfig, rng: random.Random) -> None:
    categories = [
        "Books",
        "Electronics",
        "Home",
        "Fitness",
        "Office",
        "Garden",
        "Toys",
        "Apparel",
    ]
    conn.executemany(
        "INSERT INTO categories(category_id, name) VALUES (?, ?)",
        [(idx + 1, name) for idx, name in enumerate(categories)],
    )

    products = []
    for product_id in range(1, cfg.products + 1):
        category_id = (product_id % len(categories)) + 1
        price_cents = rng.randint(899, 25999)
        products.append(
            (
                product_id,
                category_id,
                f"SKU-{product_id:05d}",
                f"{categories[category_id - 1]} Item {product_id}",
                price_cents,
                1 if product_id % 17 else 0,
            )
        )
    conn.executemany(
        """
        INSERT INTO products(product_id, category_id, sku, product_name, price_cents, active)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        products,
    )

    countries = ["US", "IN", "DE", "BR", "GB", "CA", "AU"]
    segments = ["consumer", "business", "enterprise"]
    user_rows = []
    base_signup = datetime(2021, 1, 1)
    for user_id in range(1, cfg.users + 1):
        signup_date = base_signup + timedelta(days=rng.randint(0, 900))
        user_rows.append(
            (
                user_id,
                f"user{user_id}@example.com",
                f"User {user_id}",
                rng.choice(countries),
                signup_date.date().isoformat(),
                segments[user_id % len(segments)],
            )
        )
    conn.executemany(
        """
        INSERT INTO users(user_id, email, full_name, country, signup_date, segment)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        user_rows,
    )

    order_rows = []
    order_item_rows = []
    base_created = datetime(2023, 1, 1, 8, 0, 0)
    order_item_id = 1
    statuses = ["pending", "paid", "shipped", "delivered", "cancelled"]
    product_price = {row[0]: row[4] for row in products}
    for order_id in range(1, cfg.orders + 1):
        user_id = rng.randint(1, cfg.users)
        created_at = base_created + timedelta(minutes=order_id * 13)
        status = rng.choices(statuses, weights=[1, 2, 3, 5, 1])[0]
        item_count = rng.randint(1, cfg.max_items_per_order)
        total_cents = 0
        for _ in range(item_count):
            product_id = rng.randint(1, cfg.products)
            quantity = rng.randint(1, 4)
            unit_price = product_price[product_id]
            total_cents += quantity * unit_price
            order_item_rows.append(
                (
                    order_item_id,
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                )
            )
            order_item_id += 1
        shipped_at = None
        if status in {"shipped", "delivered"}:
            shipped_at = (created_at + timedelta(days=rng.randint(1, 5))).isoformat(sep=" ")
        order_rows.append(
            (
                order_id,
                user_id,
                status,
                created_at.isoformat(sep=" "),
                shipped_at,
                total_cents,
            )
        )

    conn.executemany(
        """
        INSERT INTO orders(order_id, user_id, status, created_at, shipped_at, total_cents)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        order_rows,
    )
    conn.executemany(
        """
        INSERT INTO order_items(order_item_id, order_id, product_id, quantity, unit_price_cents)
        VALUES (?, ?, ?, ?, ?)
        """,
        order_item_rows,
    )


def _seed_analytics(conn: sqlite3.Connection, cfg: SeedConfig, rng: random.Random) -> None:
    plan_tiers = ["free", "pro", "enterprise"]
    accounts = []
    base_created = datetime(2022, 1, 1)
    for account_id in range(1, cfg.accounts + 1):
        accounts.append(
            (
                account_id,
                f"Account {account_id}",
                plan_tiers[account_id % len(plan_tiers)],
                (base_created + timedelta(days=account_id)).date().isoformat(),
            )
        )
    conn.executemany(
        "INSERT INTO accounts(account_id, account_name, plan_tier, created_at) VALUES (?, ?, ?, ?)",
        accounts,
    )

    countries = ["US", "IN", "DE", "FR", "GB", "AU"]
    device_types = ["desktop", "mobile", "tablet"]
    page_paths = ["/", "/pricing", "/docs", "/blog", "/checkout", "/features"]
    referrers = ["google", "newsletter", "twitter", "direct", "partner"]
    sessions = []
    page_views = []
    conversions = []
    base_start = datetime(2024, 1, 1, 0, 0, 0)
    page_view_id = 1
    conversion_id = 1
    for session_id in range(1, cfg.sessions + 1):
        account_id = rng.randint(1, cfg.accounts)
        started_at = base_start + timedelta(minutes=session_id * 7)
        duration_minutes = rng.randint(1, 90)
        ended_at = started_at + timedelta(minutes=duration_minutes)
        sessions.append(
            (
                session_id,
                account_id,
                f"user-{account_id}-{session_id}",
                started_at.isoformat(sep=" "),
                ended_at.isoformat(sep=" "),
                rng.choice(device_types),
                rng.choice(countries),
            )
        )

        view_count = rng.randint(1, cfg.max_page_views_per_session)
        for offset in range(view_count):
            viewed_at = started_at + timedelta(seconds=offset * rng.randint(12, 90))
            page_views.append(
                (
                    page_view_id,
                    session_id,
                    rng.choice(page_paths),
                    rng.choice(referrers),
                    viewed_at.isoformat(sep=" "),
                    rng.randint(500, 180000),
                )
            )
            page_view_id += 1

        if session_id % 9 == 0:
            conversions.append(
                (
                    conversion_id,
                    session_id,
                    "purchase" if session_id % 18 == 0 else "trial_signup",
                    0 if session_id % 18 else rng.randint(2500, 45000),
                    ended_at.isoformat(sep=" "),
                )
            )
            conversion_id += 1

    conn.executemany(
        """
        INSERT INTO sessions(session_id, account_id, user_identifier, started_at, ended_at, device_type, country)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        sessions,
    )
    conn.executemany(
        """
        INSERT INTO page_views(page_view_id, session_id, page_path, referrer, viewed_at, duration_ms)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        page_views,
    )
    conn.executemany(
        """
        INSERT INTO conversions(conversion_id, session_id, conversion_type, revenue_cents, converted_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        conversions,
    )


def _seed_hr(conn: sqlite3.Connection, cfg: SeedConfig, rng: random.Random) -> None:
    departments = [
        ("Engineering", "CC100"),
        ("Finance", "CC200"),
        ("Operations", "CC300"),
        ("People", "CC400"),
        ("Sales", "CC500"),
        ("Support", "CC600"),
    ]
    conn.executemany(
        "INSERT INTO departments(department_id, department_name, cost_center) VALUES (?, ?, ?)",
        [(idx + 1, name, cost_center) for idx, (name, cost_center) in enumerate(departments)],
    )

    titles = ["Analyst", "Manager", "Engineer", "Director", "Specialist", "Coordinator"]
    countries = ["US", "IN", "DE", "GB", "CA"]
    status_cycle = ["active", "active", "active", "leave", "terminated"]
    employees = []
    salaries = []
    reviews = []
    base_hire = datetime(2018, 1, 1)
    for employee_id in range(1, cfg.employees + 1):
        department_id = ((employee_id - 1) % len(departments)) + 1
        manager_id = None if employee_id <= len(departments) else max(1, employee_id // 8)
        employees.append(
            (
                employee_id,
                department_id,
                manager_id,
                f"First{employee_id}",
                f"Last{employee_id}",
                titles[employee_id % len(titles)],
                (base_hire + timedelta(days=employee_id * 5)).date().isoformat(),
                countries[employee_id % len(countries)],
                status_cycle[employee_id % len(status_cycle)],
            )
        )

        annual_salary = 6500000 + (department_id * 250000) + (employee_id % 25) * 180000
        salaries.append(
            (
                employee_id,
                employee_id,
                "2024-01-01",
                annual_salary,
                round(0.05 + (employee_id % 6) * 0.02, 2),
            )
        )

        if employee_id > len(departments):
            reviewer_id = manager_id or 1
            reviews.append(
                (
                    employee_id,
                    employee_id,
                    "2024-H1",
                    reviewer_id,
                    2 + (employee_id % 4),
                    f"Performance review for employee {employee_id}",
                )
            )

    conn.executemany(
        """
        INSERT INTO employees(
            employee_id, department_id, manager_id, first_name, last_name, title, hire_date, country, employment_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        employees,
    )
    conn.executemany(
        """
        INSERT INTO salaries(salary_id, employee_id, effective_date, annual_salary_cents, bonus_target)
        VALUES (?, ?, ?, ?, ?)
        """,
        salaries,
    )
    conn.executemany(
        """
        INSERT INTO performance_reviews(review_id, employee_id, review_period, reviewer_id, rating, review_summary)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        reviews,
    )

