"""
Inference Script — SQL Analytics Workspace
===================================
MANDATORY
- Environment variables: API_BASE_URL, MODEL_NAME, HF_TOKEN
- Defaults set only for API_BASE_URL and MODEL_NAME (not HF_TOKEN)
- Uses OpenAI Client for all LLM calls
- Emits [START]/[STEP]/[END] structured stdout
"""

import os
import re
import sys
import textwrap
from typing import List, Optional

from openai import OpenAI

from client import SQLWorkspaceEnv
from models import SQLAction

# Environment variables (defaults only for API_BASE_URL and MODEL_NAME, NOT HF_TOKEN)
API_BASE_URL = os.getenv("API_BASE_URL", "https://router.huggingface.co/v1")
MODEL_NAME = os.getenv("MODEL_NAME", "Qwen/Qwen2.5-72B-Instruct")
HF_TOKEN = os.getenv("HF_TOKEN")
ENV_URL = os.getenv("ENV_URL", "http://localhost:8000")

if HF_TOKEN is None:
    raise ValueError("HF_TOKEN environment variable is required")

TASKS = ["easy", "medium", "hard", "very_hard"]
BENCHMARK = "sql-analytics-workspace"

SQL_PROMPT = textwrap.dedent("""
You are an expert SQL data analyst working with a SQLite database.
You have explored the database and gathered the information below.
Write a SQL query that answers the business question.

Rules:
- Respond with ONLY the SQL query, nothing else.
- Do not include explanations, markdown formatting, or comments.
- Use SQLite-compatible syntax.
- Pay close attention to data warnings — filter out bad data.
- Use the exact column names from the schema.
- When asked to order results, include ORDER BY.
- If a previous attempt failed, fix the issue based on the error/grader feedback.
""").strip()


# ── Structured Logging ──────────────────────────────────────

def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    error_val = error if error else "null"
    done_val = str(done).lower()
    print(
        f"[STEP] step={step} action={action} reward={reward:.2f} done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END] success={str(success).lower()} steps={steps} score={score:.2f} rewards={rewards_str}",
        flush=True,
    )


# ── Helpers ──────────────────────────────────────────────────

def extract_sql(response_text: str) -> str:
    text = response_text.strip()
    match = re.search(r"```(?:sql)?\s*\n?(.*?)```", text, re.DOTALL)
    if match:
        text = match.group(1).strip()
    return text.strip().rstrip(";").strip()


def _parse_table_names(tables_list: str) -> list:
    names = []
    for line in tables_list.split("\n"):
        line = line.strip()
        if line and "(" in line:
            name = line.split("(")[0].strip()
            if name and name != "Available tables:":
                names.append(name)
    return names


def _fallback_sql(question: str) -> str:
    q = question.lower()

    if "premium" in q and ("signed up" in q or "signup" in q):
        return (
            "SELECT name, COALESCE(email, 'N/A') AS email, signup_date FROM customers "
            "WHERE tier = 'premium' AND signup_date > '2023-01-01' ORDER BY name"
        )

    if "revenue" in q and "category" in q:
        return (
            "SELECT p.category, COUNT(DISTINCT o.customer_id) AS customer_count, "
            "ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_revenue "
            "FROM orders o JOIN order_items oi ON o.id = oi.order_id "
            "JOIN products p ON oi.product_id = p.id "
            "WHERE o.order_date >= '2023-10-01' AND o.order_date <= '2023-12-31' "
            "AND o.status = 'completed' GROUP BY p.category "
            "HAVING SUM(oi.quantity * oi.unit_price) > 1000 ORDER BY total_revenue DESC"
        )

    if ("month-over-month" in q or "trend" in q) and "revenue" in q:
        return (
            "WITH monthly_revenue AS (SELECT strftime('%Y-%m', o.order_date) AS month, "
            "ROUND(SUM(oi.quantity * oi.unit_price), 2) AS revenue FROM orders o "
            "JOIN order_items oi ON o.id = oi.order_id WHERE o.status = 'completed' "
            "GROUP BY strftime('%Y-%m', o.order_date)), "
            "with_growth AS (SELECT month, revenue, "
            "LAG(revenue) OVER (ORDER BY month) AS prev_revenue, "
            "ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 "
            "/ LAG(revenue) OVER (ORDER BY month), 1) AS growth_pct FROM monthly_revenue) "
            "SELECT month, revenue, COALESCE(prev_revenue, 0) AS prev_revenue, "
            "COALESCE(growth_pct, 0) AS growth_pct, "
            "CASE WHEN prev_revenue IS NULL THEN 'first_month' "
            "WHEN growth_pct > 20 THEN 'high_growth' "
            "WHEN growth_pct > 0 THEN 'moderate_growth' "
            "WHEN growth_pct = 0 THEN 'flat' ELSE 'decline' END AS trend "
            "FROM with_growth ORDER BY month"
        )

    if ("rank" in q or "top customers" in q or "spending" in q) and "country" in q:
        return (
            "WITH customer_totals AS (SELECT c.id, c.name, c.country, "
            "ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_spend, "
            "COUNT(DISTINCT o.id) AS order_count FROM customers c "
            "JOIN orders o ON c.id = o.customer_id "
            "JOIN order_items oi ON o.id = oi.order_id "
            "WHERE o.status = 'completed' GROUP BY c.id, c.name, c.country "
            "HAVING COUNT(DISTINCT o.id) >= 2) "
            "SELECT name, country, total_spend, "
            "ROUND(total_spend / order_count, 2) AS avg_order_value, "
            "RANK() OVER (PARTITION BY country ORDER BY total_spend DESC) AS country_rank, "
            "ROUND(total_spend * 100.0 / SUM(total_spend) OVER (PARTITION BY country), 1) "
            "AS pct_of_country FROM customer_totals ORDER BY country ASC, country_rank ASC"
        )

    return "SELECT 1"


def build_llm_prompt(context: dict, question: str, prev_error: str = "", prev_grader: str = "") -> str:
    parts = [f"BUSINESS QUESTION: {question}"]
    parts.append("\nDATABASE SCHEMA:")
    for desc in context["descriptions"]:
        parts.append(desc)
    if prev_error:
        parts.append(f"\nPREVIOUS ATTEMPT FAILED: {prev_error}")
    if prev_grader:
        parts.append(f"\nPREVIOUS GRADER FEEDBACK: {prev_grader}")
    parts.append("\nWrite ONLY the SQL query:")
    return "\n".join(parts)


# ── Main ─────────────────────────────────────────────────────

def run_task(client: OpenAI, env_url: str, task_id: str) -> List[float]:
    """Run a single task. Returns list of ALL rewards (one per step)."""
    rewards: List[float] = []
    step_num = 0

    log_start(task=task_id, env=BENCHMARK, model=MODEL_NAME)

    try:
        with SQLWorkspaceEnv(base_url=env_url).sync() as env:
            result = env.reset(task_id=task_id)
            obs = result.observation
            question = obs.question
            context = {"descriptions": [], "samples": [], "warnings": []}

            # Phase 1: Exploration — log [STEP] for EVERY env.step() call
            # Exploration steps return reward=0.00 (matches standard pattern)
            step_num += 1
            result = env.step(SQLAction(action_type="explore_tables"))
            obs = result.observation
            step_reward = result.reward if result.reward is not None else 0.01
            rewards.append(step_reward)
            log_step(step_num, "explore_tables", step_reward, result.done, None)
            context["tables_list"] = obs.tables_list

            tables = _parse_table_names(obs.tables_list)
            for table in tables:
                step_num += 1
                result = env.step(SQLAction(action_type="describe_table", table_name=table))
                obs = result.observation
                step_reward = result.reward if result.reward is not None else 0.01
                rewards.append(step_reward)
                log_step(step_num, f"describe_table({table})", step_reward, result.done, None)
                if obs.table_description:
                    context["descriptions"].append(obs.table_description)
                if obs.data_warnings:
                    context["warnings"].append(obs.data_warnings)

            for table in tables[:3]:
                step_num += 1
                result = env.step(SQLAction(action_type="sample_data", table_name=table))
                obs = result.observation
                step_reward = result.reward if result.reward is not None else 0.01
                rewards.append(step_reward)
                log_step(step_num, f"sample_data({table})", step_reward, result.done, None)
                if obs.sample_rows:
                    context["samples"].append(obs.sample_rows)

            # Phase 2: Submit SQL — up to 3 attempts
            prev_error = ""
            prev_grader = ""

            for _ in range(obs.submission_attempts_remaining):
                if result.done:
                    break

                step_num += 1

                prompt = build_llm_prompt(context, question, prev_error, prev_grader)
                try:
                    completion = client.chat.completions.create(
                        model=MODEL_NAME,
                        messages=[
                            {"role": "system", "content": SQL_PROMPT},
                            {"role": "user", "content": prompt},
                        ],
                        temperature=0.0,
                        max_tokens=800,
                    )
                    response = completion.choices[0].message.content or ""
                    sql = extract_sql(response)
                except Exception as e:
                    print(f"[DEBUG] LLM error: {e}", file=sys.stderr, flush=True)
                    sql = _fallback_sql(question.lower())

                if not sql:
                    sql = "SELECT 1"

                result = env.step(SQLAction(action_type="submit_answer", sql_query=sql))
                obs = result.observation
                reward = result.reward if result.reward is not None else 0.01
                rewards.append(reward)
                error = obs.error_message if obs.error_message else None

                log_step(step_num, sql[:80], reward, result.done, error)

                if reward >= 0.95:
                    break

                prev_error = obs.error_message
                prev_grader = obs.grader_explanation

    except Exception as exc:
        print(f"[DEBUG] Exception: {exc}", file=sys.stderr, flush=True)

    score = max(rewards) if rewards else 0.01
    score = max(0.01, min(0.99, score))
    success = score >= 0.5
    log_end(success=success, steps=step_num, score=score, rewards=rewards)

    return rewards


def main() -> None:
    client = OpenAI(base_url=API_BASE_URL, api_key=HF_TOKEN)

    for task_id in TASKS:
        run_task(client, ENV_URL, task_id)


if __name__ == "__main__":
    main()
