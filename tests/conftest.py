from typing import Any

import pytest


@pytest.fixture
def log_config() -> dict[str, Any]:
    return {"log": {"class": "cg.log.BasicConfigLogFactory"}}
