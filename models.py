"""Typed Pydantic models for the SQL Analytics Workspace environment."""

from typing import Any, Dict, List, Optional

from pydantic import Field

from openenv.core.env_server import Action, Observation, State


class SQLAction(Action):
    """Multi-step action: explore the database before submitting a query."""

    action_type: str = Field(
        default="submit_answer",
        description=(
            "One of: explore_tables, describe_table, sample_data, "
            "run_query, submit_answer"
        ),
    )
    table_name: str = Field(
        default="", description="Target table (for describe_table / sample_data)"
    )
    sql_query: str = Field(
        default="", description="SQL query (for run_query / submit_answer)"
    )
    num_rows: int = Field(
        default=5, description="Number of sample rows for sample_data (max 10)"
    )


class SQLObservation(Observation):
    """What the agent sees after each step."""

    question: str = Field(default="", description="Natural language business question")
    task_id: str = Field(default="", description="Current task identifier")
    task_description: str = Field(default="", description="Difficulty description")

    # Exploration results (populated on demand by agent actions)
    tables_list: str = Field(default="", description="Available tables and row counts")
    table_description: str = Field(
        default="", description="Column details for a described table"
    )
    sample_rows: str = Field(default="", description="Sample data rows from a table")
    query_result: str = Field(
        default="", description="Result of run_query or submit_answer"
    )
    error_message: str = Field(default="", description="Error from previous action")
    grader_explanation: str = Field(
        default="", description="Grader feedback on submitted answer"
    )

    # Phase and budget
    phase: str = Field(
        default="exploration", description="Current phase: exploration or submission"
    )
    step_number: int = Field(default=0, description="Current step number")
    steps_remaining: int = Field(default=0, description="Total steps remaining")
    submission_attempts_remaining: int = Field(
        default=3, description="submit_answer calls remaining"
    )

    # Hints about dirty data (process supervision signal)
    data_warnings: str = Field(
        default="", description="Warnings about data quality issues the agent discovered"
    )


class SQLState(State):
    """Current episode metadata."""

    task_id: str = Field(default="", description="Current task identifier")
    current_reward: float = Field(default=0.0, description="Total reward so far")
    exploration_reward: float = Field(
        default=0.0, description="Reward earned from exploration"
    )
    submission_reward: float = Field(
        default=0.0, description="Best reward from submissions"
    )
    tables_explored: List[str] = Field(
        default_factory=list, description="Tables the agent has explored"
    )
    submission_attempts: int = Field(default=0, description="submit_answer calls made")
    max_submissions: int = Field(default=3, description="Max submit_answer calls")
    last_action_type: str = Field(default="", description="Last action type taken")
    phase: str = Field(default="exploration", description="Current phase")
