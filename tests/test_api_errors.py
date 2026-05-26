from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from api.main import app
from core.exceptions import InsufficientUserProfileDataError


@app.get("/__test__/insufficient-user-profile-data")
def insufficient_user_profile_data() -> None:
    raise InsufficientUserProfileDataError(
        "User profile has no available vectors",
        details={"user_id": 7},
    )


def test_app_error_handler_returns_structured_insufficient_profile_error() -> None:
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/__test__/insufficient-user-profile-data")

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "insufficient_user_profile_data",
            "message": "User profile has no available vectors",
            "details": {"user_id": 7},
        }
    }
