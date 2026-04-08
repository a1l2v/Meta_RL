import os
from typing import List, Optional

from openai import OpenAI

from first_rl import FirstRlAction, FirstRlEnv

API_BASE_URL = os.getenv("API_BASE_URL", "https://api.openai.com/v1")
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4.1-mini")

API_KEY = os.getenv("OPENAI_API_KEY") or os.getenv("API_KEY") or os.getenv("HF_TOKEN")
IMAGE_NAME = os.getenv("IMAGE_NAME")
ENV_BASE_URL = os.getenv("ENV_BASE_URL")
TASK_NAME = os.getenv("TASK_NAME")
BENCHMARK = os.getenv("BENCHMARK") or "first_rl_sql_optimizer"
MAX_STEPS = 3
TEMPERATURE = 0.2
MAX_TOKENS = 300
SUCCESS_SCORE_THRESHOLD = 0.1


def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def _one_line(value: str) -> str:
    return value.replace("\n", " ").replace("\r", " ").strip()


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    action_val = _one_line(action)
    error_val = _one_line(error) if error else "null"
    done_val = str(done).lower()
    print(
        f"[STEP] step={step} action={action_val} reward={reward:.2f} done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(f"[END] success={str(success).lower()} steps={steps} score={score:.2f} rewards={rewards_str}", flush=True)


def _build_prompt(task_name: str, observation) -> str:
    schema_lines = []
    for table in observation.schema_info[:6]:
        col_names = ", ".join(col.name for col in table.columns[:8])
        schema_lines.append(f"- {table.table_name} ({table.row_count} rows): {col_names}")

    hints = observation.optimization_hints or []
    hint_text = "; ".join(hints[:4]) if hints else "None"
    plan_scans = observation.execution_plan.full_scan_count if observation.execution_plan else 0
    strategy = observation.metadata.get("prompting_strategy", "")

    return (
        "You are optimizing SQLite SQL.\n"
        f"Task type: {task_name}\n"
        f"Difficulty: {observation.difficulty}\n"
        f"Prompting strategy hint: {strategy}\n"
        f"Current slow query:\n{observation.slow_query}\n\n"
        f"Schema summary:\n" + "\n".join(schema_lines) + "\n\n"
        f"Plan full-scan count: {plan_scans}\n"
        f"Optimization hints: {hint_text}\n\n"
        "Return JSON with keys: optimized_query, index_suggestions, explanation.\n"
        "optimized_query must be valid SQLite SELECT-only SQL."
    )


def _model_action(client: OpenAI, task_name: str, observation) -> FirstRlAction:
    prompt = _build_prompt(task_name, observation)
    completion = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": "You are an expert SQL optimizer."},
            {"role": "user", "content": prompt},
        ],
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
        response_format={"type": "json_object"},
    )
    content = (completion.choices[0].message.content or "").strip()
    # Minimal defensive parse without extra dependencies.
    import json

    data = json.loads(content)
    return FirstRlAction(
        optimized_query=str(data.get("optimized_query", "SELECT 1")),
        index_suggestions=[str(x) for x in data.get("index_suggestions", []) if isinstance(x, str)],
        explanation=(str(data["explanation"]) if data.get("explanation") is not None else None),
    )


def _make_env() -> FirstRlEnv:
    if IMAGE_NAME:
        return FirstRlEnv.from_docker_image(IMAGE_NAME)
    if ENV_BASE_URL:
        return FirstRlEnv(base_url=ENV_BASE_URL)
    raise RuntimeError("Set IMAGE_NAME or ENV_BASE_URL.")


def _extract_error(observation) -> Optional[str]:
    metadata = observation.metadata or {}
    value = metadata.get("last_action_error")
    if value is None:
        return None
    return str(value)


def main() -> None:
    client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)
    env = _make_env()

    rewards: List[float] = []
    steps_taken = 0
    score = 0.0
    success = False
    task_name = TASK_NAME or "unknown"

    log_start(task=task_name, env=BENCHMARK, model=MODEL_NAME)

    try:
        reset_kwargs = {}
        if TASK_NAME:
            reset_kwargs["task_type"] = TASK_NAME
        result = env.reset(**reset_kwargs)
        task_name = result.observation.task_type

        for step in range(1, MAX_STEPS + 1):
            if result.done:
                break

            action_obj = _model_action(client, task_name, result.observation)
            result = env.step(action_obj)

            reward = float(result.reward or 0.0)
            rewards.append(reward)
            steps_taken = step
            error = _extract_error(result.observation)
            log_step(
                step=step,
                action=action_obj.optimized_query,
                reward=reward,
                done=bool(result.done),
                error=error,
            )

            if result.done:
                break

        raw_score = rewards[-1] if rewards else 0.0
        score = min(max(raw_score, 0.0), 1.0)
        success = score >= SUCCESS_SCORE_THRESHOLD
    finally:
        try:
            env.close()
        finally:
            log_end(success=success, steps=steps_taken, score=score, rewards=rewards)


if __name__ == "__main__":
    main()
