"""Task definitions for the SQL Analytics Workspace."""

from .easy import EASY_TASK
from .medium import MEDIUM_TASK
from .hard import HARD_TASK
from .very_hard import VERY_HARD_TASK

ALL_TASKS = {
    "easy": EASY_TASK,
    "medium": MEDIUM_TASK,
    "hard": HARD_TASK,
    "very_hard": VERY_HARD_TASK,
}
