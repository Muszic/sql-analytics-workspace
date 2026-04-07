"""FastAPI application for the SQL Analytics Workspace environment."""

import sys
import os

# Add parent directory to path so models can be imported
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openenv.core.env_server import create_fastapi_app

from models import SQLAction, SQLObservation
from server.environment import SQLAnalyticsEnvironment

app = create_fastapi_app(
    env=SQLAnalyticsEnvironment,
    action_cls=SQLAction,
    observation_cls=SQLObservation,
)


from fastapi.responses import RedirectResponse


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


def main():
    import uvicorn
    uvicorn.run("server.app:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
