"""SQL Analytics Workspace — multi-step data exploration environment."""

import sqlite3
from typing import Any, Optional, Set

from openenv.core.env_server import Environment

from models import SQLAction, SQLObservation, SQLState
from server.database import create_database
from server.grader import grade_sql
from server.tasks import ALL_TASKS

# Maximum exploration reward budget (out of 1.0)
MAX_EXPLORATION_REWARD = 0.15
# Scaling factor for submission grading (1.0 - MAX_EXPLORATION_REWARD)
SUBMISSION_SCALE = 1.0 - MAX_EXPLORATION_REWARD

# Blocked SQL prefixes for run_query (maintain determinism)
BLOCKED_PREFIXES = ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "ATTACH", "DETACH")


class SQLAnalyticsEnvironment(
    Environment[SQLAction, SQLObservation, SQLState]
):
    """
    Multi-step environment where an AI agent acts as a data analyst.
    The agent must explore the database schema and data before writing
    SQL queries to answer business questions.
    """

    SUPPORTS_CONCURRENT_SESSIONS = True

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._conn: Optional[sqlite3.Connection] = None
        self._task: Optional[dict] = None
        self._step_count: int = 0
        self._done: bool = False
        self._episode_id: Optional[str] = None

        # Exploration tracking
        self._tables_explored: Set[str] = set()
        self._tables_sampled: Set[str] = set()
        self._tables_available: list = []
        self._relevant_tables: Set[str] = set()
        self._exploration_reward: float = 0.0
        self._explored_tables_flag: bool = False
        self._successful_queries: int = 0

        # Submission tracking
        self._submission_attempts: int = 0
        self._max_submissions: int = 3
        self._best_submission_score: float = 0.0
        self._last_action_type: str = ""

        # Data warnings discovered
        self._data_warnings: list = []

    def reset(
        self,
        seed: Optional[int] = None,
        episode_id: Optional[str] = None,
        **kwargs: Any,
    ) -> SQLObservation:
        """Start a new episode — agent sees only the question, must explore."""
        task_id = kwargs.get("task_id", "easy")
        if task_id not in ALL_TASKS:
            task_id = "easy"

        self._task = ALL_TASKS[task_id]
        self._episode_id = episode_id
        self._step_count = 0
        self._done = False

        # Reset exploration state
        self._tables_explored = set()
        self._tables_sampled = set()
        self._exploration_reward = 0.0
        self._explored_tables_flag = False
        self._successful_queries = 0
        self._data_warnings = []

        # Reset submission state
        self._submission_attempts = 0
        self._max_submissions = self._task.get("max_submissions", 3)
        self._best_submission_score = 0.0
        self._last_action_type = ""

        # Create fresh database
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = create_database()

        # Discover available tables
        cursor = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        self._tables_available = [row[0] for row in cursor.fetchall()]
        self._relevant_tables = set(self._task.get("relevant_tables", []))

        max_steps = self._task.get("max_steps", 8)

        return SQLObservation(
            done=False,
            reward=0.01,
            question=self._task["question"],
            task_id=self._task["id"],
            task_description=self._task["description"],
            phase="exploration",
            step_number=0,
            steps_remaining=max_steps,
            submission_attempts_remaining=self._max_submissions,
        )

    def step(
        self,
        action: SQLAction,
        timeout_s: Optional[float] = None,
        **kwargs: Any,
    ) -> SQLObservation:
        """Dispatch based on action_type."""
        # Handle case where step is called without reset (HTTP stateless mode)
        if self._task is None:
            self.reset(**kwargs)

        if self._done:
            return self._make_obs(
                error_message="Episode already finished.",
                reward=self._total_reward(),
                done=True,
            )

        self._step_count += 1
        self._last_action_type = action.action_type
        max_steps = self._task.get("max_steps", 8)

        # Check step budget
        if self._step_count > max_steps:
            self._done = True
            return self._make_obs(
                error_message="Step budget exhausted.",
                reward=self._total_reward(),
                done=True,
            )

        action_type = action.action_type.strip().lower()

        if action_type == "explore_tables":
            return self._handle_explore_tables()
        elif action_type == "describe_table":
            return self._handle_describe_table(action.table_name.strip())
        elif action_type == "sample_data":
            return self._handle_sample_data(
                action.table_name.strip(), min(max(action.num_rows, 1), 10)
            )
        elif action_type == "run_query":
            return self._handle_run_query(action.sql_query.strip())
        elif action_type == "submit_answer":
            return self._handle_submit_answer(action.sql_query.strip())
        else:
            return self._make_obs(
                error_message=f"Unknown action_type: '{action.action_type}'. "
                "Use: explore_tables, describe_table, sample_data, run_query, submit_answer",
            )

    # ── Action Handlers ──────────────────────────────────────────

    def _handle_explore_tables(self) -> SQLObservation:
        """List all tables with row counts."""
        lines = []
        for table in self._tables_available:
            cursor = self._conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            lines.append(f"  {table} ({count} rows)")

        tables_list = "Available tables:\n" + "\n".join(lines)

        # Milestone: agent listed the tables
        if not self._explored_tables_flag:
            self._explored_tables_flag = True
            self._add_exploration_reward(0.05)

        return self._make_obs(tables_list=tables_list)

    def _handle_describe_table(self, table_name: str) -> SQLObservation:
        """Show column details for a table."""
        if not table_name:
            return self._make_obs(
                error_message="describe_table requires a table_name."
            )
        if table_name not in self._tables_available:
            return self._make_obs(
                error_message=f"Table '{table_name}' does not exist. "
                f"Available: {', '.join(self._tables_available)}"
            )

        cursor = self._conn.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()

        lines = [f"TABLE: {table_name}"]
        for col in columns:
            pk = " PRIMARY KEY" if col[5] else ""
            nullable = " (nullable)" if not col[3] else ""
            lines.append(f"  {col[1]} {col[2]}{pk}{nullable}")

        # Foreign keys
        cursor = self._conn.execute(f"PRAGMA foreign_key_list({table_name})")
        fks = cursor.fetchall()
        if fks:
            lines.append("  Foreign keys:")
            for fk in fks:
                lines.append(f"    {fk[3]} → {fk[2]}.{fk[4]}")

        description = "\n".join(lines)

        # Check for data quality issues and warn
        warnings = self._check_data_warnings(table_name)

        # Milestone: first describe of ANY table = +0.05
        if not self._tables_explored:
            self._add_exploration_reward(0.05)
        self._tables_explored.add(table_name)

        return self._make_obs(
            table_description=description,
            data_warnings=warnings,
        )

    def _handle_sample_data(self, table_name: str, num_rows: int) -> SQLObservation:
        """Show sample rows from a table."""
        if not table_name:
            return self._make_obs(
                error_message="sample_data requires a table_name."
            )
        if table_name not in self._tables_available:
            return self._make_obs(
                error_message=f"Table '{table_name}' does not exist."
            )

        cursor = self._conn.execute(f"SELECT * FROM {table_name} LIMIT {num_rows}")
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        header = " | ".join(col_names)
        separator = "-" * len(header)
        data_lines = [" | ".join(str(v) for v in row) for row in rows]
        sample = f"Sample from {table_name} ({num_rows} rows):\n{header}\n{separator}\n"
        sample += "\n".join(data_lines)

        # Milestone: first sample of ANY table = +0.05
        if not self._tables_sampled:
            self._add_exploration_reward(0.05)
        self._tables_sampled.add(table_name)

        return self._make_obs(sample_rows=sample)

    def _handle_run_query(self, sql_query: str) -> SQLObservation:
        """Execute an exploratory query (not graded)."""
        if not sql_query:
            return self._make_obs(error_message="run_query requires a sql_query.")

        # Block DDL/DML
        first_word = sql_query.strip().split()[0].upper() if sql_query.strip() else ""
        if first_word in BLOCKED_PREFIXES:
            return self._make_obs(
                error_message=f"Write operations ({first_word}) are not allowed. "
                "This environment is read-only."
            )

        try:
            cursor = self._conn.execute(sql_query)
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            result = _format_result_table(col_names, rows)

            # Small reward for first 2 successful exploratory queries
            if self._successful_queries < 2:
                self._successful_queries += 1
                self._add_exploration_reward(0.02)

            return self._make_obs(query_result=result)
        except Exception as e:
            return self._make_obs(error_message=f"Query error: {e}")

    def _handle_submit_answer(self, sql_query: str) -> SQLObservation:
        """Submit final SQL answer for grading."""
        if not sql_query:
            return self._make_obs(
                error_message="submit_answer requires a sql_query."
            )

        self._submission_attempts += 1

        # Grade it
        score, explanation = grade_sql(
            conn=self._conn,
            agent_sql=sql_query,
            expected_sql=self._task["expected_sql"],
            expected_columns=self._task["expected_columns"],
            order_matters=self._task["order_matters"],
        )

        self._best_submission_score = max(self._best_submission_score, score)

        # Try to get result text for feedback
        result_text = ""
        if score > 0.0:
            try:
                cursor = self._conn.execute(sql_query)
                rows = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description]
                result_text = _format_result_table(col_names, rows)
            except Exception:
                pass

        # Check if done
        is_done = (
            score >= 0.99
            or self._submission_attempts >= self._max_submissions
        )
        self._done = is_done

        raw_reward = self._exploration_reward + (score * SUBMISSION_SCALE)
        # Clamp to strictly within (0, 1) — never exactly 0.0 or 1.0
        total_reward = max(0.01, min(0.99, raw_reward))

        return self._make_obs(
            query_result=result_text,
            grader_explanation=explanation,
            reward=total_reward,
            done=is_done,
            phase="submission",
        )

    # ── Helpers ───────────────────────────────────────────────────

    def _check_data_warnings(self, table_name: str) -> str:
        """Detect data quality issues and return warnings."""
        warnings = []

        # Check for NULLs in key columns
        cursor = self._conn.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        for col in columns:
            col_name = col[1]
            not_null = col[3]
            if not not_null:  # nullable column
                cursor2 = self._conn.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL"
                )
                null_count = cursor2.fetchone()[0]
                if null_count > 0:
                    warnings.append(
                        f"Warning: {table_name}.{col_name} has {null_count} NULL values"
                    )

        # Check for non-completed orders (trap hint)
        if table_name == "orders":
            cursor = self._conn.execute(
                "SELECT status, COUNT(*) FROM orders GROUP BY status"
            )
            statuses = cursor.fetchall()
            if len(statuses) > 1:
                status_str = ", ".join(f"{s[0]}={s[1]}" for s in statuses)
                warnings.append(
                    f"Warning: orders table has mixed statuses: {status_str}"
                )

        if warnings:
            self._data_warnings.extend(warnings)
            return "\n".join(warnings)
        return ""

    def _add_exploration_reward(self, amount: float) -> None:
        """Add exploration reward, capped at MAX_EXPLORATION_REWARD."""
        self._exploration_reward = min(
            self._exploration_reward + amount, MAX_EXPLORATION_REWARD
        )

    def _total_reward(self) -> float:
        """Total reward = exploration + best submission (scaled)."""
        if self._best_submission_score == 0 and self._exploration_reward == 0:
            return 0.01  # minimum when episode ends without any meaningful action
        raw = self._exploration_reward + (
            self._best_submission_score * SUBMISSION_SCALE
        )
        return max(0.01, min(0.99, raw))

    def _make_obs(self, **overrides: Any) -> SQLObservation:
        """Build an observation with defaults + overrides."""
        max_steps = self._task.get("max_steps", 8) if self._task else 8
        # All steps return reward strictly in (0, 1) exclusive
        # Non-submission steps return 0.01; submit_answer returns a graded reward
        raw_reward = overrides.pop("reward", None)
        if raw_reward is None:
            reward = 0.01  # exploration/non-terminal steps — must be in (0, 1) exclusive
        else:
            reward = max(0.01, min(0.99, float(raw_reward)))
        defaults = dict(
            done=overrides.pop("done", False),
            reward=reward,
            question=self._task["question"] if self._task else "",
            task_id=self._task["id"] if self._task else "",
            task_description=self._task["description"] if self._task else "",
            phase="exploration",
            step_number=self._step_count,
            steps_remaining=max(0, max_steps - self._step_count),
            submission_attempts_remaining=max(
                0, self._max_submissions - self._submission_attempts
            ),
            tables_list="",
            table_description="",
            sample_rows="",
            query_result="",
            error_message="",
            grader_explanation="",
            data_warnings="",
        )
        defaults.update(overrides)
        return SQLObservation(**defaults)

    @property
    def state(self) -> SQLState:
        """Return current episode metadata."""
        return SQLState(
            episode_id=self._episode_id,
            step_count=self._step_count,
            task_id=self._task["id"] if self._task else "",
            current_reward=self._total_reward(),
            exploration_reward=self._exploration_reward,
            submission_reward=self._best_submission_score,
            tables_explored=sorted(self._tables_explored),
            submission_attempts=self._submission_attempts,
            max_submissions=self._max_submissions,
            last_action_type=self._last_action_type,
            phase="submission" if self._submission_attempts > 0 else "exploration",
        )

    def close(self) -> None:
        """Clean up database connection."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        super().close()


def _format_result_table(columns: list, rows: list, max_rows: int = 20) -> str:
    """Format SQL results as a readable text table."""
    if not rows:
        return "(empty result set)"

    display_rows = rows[:max_rows]
    header = " | ".join(str(c) for c in columns)
    separator = "-" * len(header)
    lines = [header, separator]
    for row in display_rows:
        lines.append(" | ".join(str(v) for v in row))

    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more rows)")

    return "\n".join(lines)
