"""Client for the SQL Analytics Workspace environment."""

from typing import Any, Dict

from openenv.core.env_client import EnvClient
from openenv.core.client_types import StepResult

from models import SQLAction, SQLObservation, SQLState


class SQLWorkspaceEnv(EnvClient[SQLAction, SQLObservation, SQLState]):
    """Client for interacting with the SQL Analytics Workspace environment."""

    def _step_payload(self, action: SQLAction) -> Dict[str, Any]:
        """Convert action to JSON payload."""
        return action.model_dump()

    def _parse_result(self, payload: Dict[str, Any]) -> StepResult[SQLObservation]:
        """Parse server response into typed StepResult."""
        # Framework serializes as: {"observation": {...}, "reward": ..., "done": ...}
        obs_data = payload.get("observation", payload)
        reward = payload.get("reward")
        done = payload.get("done", False)
        observation = SQLObservation(**obs_data, reward=reward, done=done)
        return StepResult(
            observation=observation,
            reward=reward,
            done=done,
        )

    def _parse_state(self, payload: Dict[str, Any]) -> SQLState:
        """Parse server state response."""
        return SQLState(**payload)
