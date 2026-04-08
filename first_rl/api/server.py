"""Thin FastAPI wrapper around SqlEnv with serialized access."""

from __future__ import annotations

import asyncio
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from first_rl.models import FirstRlAction, FirstRlObservation
from first_rl.sql_env import SqlEnv


APP_VERSION = "0.1.0"


class ResetRequest(BaseModel):
    """Request body for environment reset."""

    task_type: Literal["basic", "join_opt", "complex"] | None = None


app = FastAPI(title="SQL Optimizer API", version=APP_VERSION)
env = SqlEnv()
lock = asyncio.Lock()


@app.post("/reset", response_model=FirstRlObservation)
async def reset_endpoint(req: ResetRequest) -> FirstRlObservation:
    """Reset environment and return a fresh observation."""
    async with lock:
        try:
            return env.reset(task_type=req.task_type)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/step", response_model=FirstRlObservation)
async def step_endpoint(action: FirstRlAction) -> FirstRlObservation:
    """Evaluate one action through the SQL grading pipeline."""
    async with lock:
        try:
            return env.step(action)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/state", response_model=FirstRlObservation)
async def state_endpoint() -> FirstRlObservation:
    """Return the current observation without stepping the environment."""
    async with lock:
        state = env.state
        payload = getattr(state, "current_observation", None)
        if payload is None:
            raise HTTPException(status_code=409, detail="Environment has not been reset yet.")
        return FirstRlObservation.model_validate(payload)


@app.get("/health")
async def health_endpoint() -> dict[str, str]:
    """Basic health check endpoint."""
    return {"status": "ok", "version": APP_VERSION}


def main(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Run the FastAPI server directly."""
    import uvicorn

    uvicorn.run(app, host=host, port=port)

