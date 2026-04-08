"""Task definitions for the SQL query optimization environment."""

from .base_task import BaseTask, TaskCase
from .task1_basic import Task1Basic
from .task2_joins import Task2Joins
from .task3_complex import Task3Complex

__all__ = [
    "BaseTask",
    "Task1Basic",
    "Task2Joins",
    "Task3Complex",
    "TaskCase",
]

