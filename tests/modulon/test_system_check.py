from typing import Any

import pytest
from pydantic import ValidationError

from modulon import Graph
from modulon.core.error import SystemChecksFailedError
from modulon.system_check import AbstractSystemCheckComponent, SystemCheckResult

# Dotted-path prefix used to reference the check classes below from graph config.
# `__name__` matches however pytest imported this module, so the re-import performed
# by `import_object` resolves to these exact classes.
PREFIX = __name__


class PassingCheck(AbstractSystemCheckComponent):
    async def evaluate(self) -> SystemCheckResult:
        return SystemCheckResult(name=self.name, passing=True, msg="ok")


class FailingCheck(AbstractSystemCheckComponent):
    async def evaluate(self) -> SystemCheckResult:
        return SystemCheckResult(name=self.name, passing=False, msg="nope")


class ThresholdCheck(AbstractSystemCheckComponent):
    value: int
    minimum: int

    async def evaluate(self) -> SystemCheckResult:
        passing = self.value >= self.minimum
        return SystemCheckResult(
            name=self.name,
            passing=passing,
            msg=f"{self.value} >= {self.minimum}",
        )


class NotACheck:
    """A plain class that is deliberately *not* an AbstractSystemCheckComponent."""


def test_system_check_result_bool_status_render() -> None:
    passing = SystemCheckResult(name="x", passing=True, msg="fine")
    failing = SystemCheckResult(name="y", passing=False, msg="broken")

    assert bool(passing) is True
    assert bool(failing) is False
    assert passing.status == "PASS"
    assert failing.status == "FAIL"
    assert passing.render() == "System check 'x' is PASS: fine"
    assert failing.render() == "System check 'y' is FAIL: broken"


@pytest.mark.asyncio
async def test_all_checks_passing_builds_graph(log_config: dict[str, Any]) -> None:
    config = log_config | {
        "system_check": {
            "class": "modulon.system_check.SystemCheckFactory",
            "config": {
                "a": {"class": f"{PREFIX}.PassingCheck"},
                "b": {"class": f"{PREFIX}.PassingCheck"},
            },
        },
    }
    graph = await Graph.from_config(config)

    assert graph.system_check is not None


@pytest.mark.asyncio
async def test_failing_check_aborts_graph_build(log_config: dict[str, Any]) -> None:
    config = log_config | {
        "system_check": {
            "class": "modulon.system_check.SystemCheckFactory",
            "config": {
                "ok": {"class": f"{PREFIX}.PassingCheck"},
                "bad": {"class": f"{PREFIX}.FailingCheck"},
            },
        },
    }
    with pytest.raises(BaseExceptionGroup) as excinfo:
        await Graph.from_config(config)

    assert excinfo.value.subgroup(SystemChecksFailedError) is not None


@pytest.mark.asyncio
async def test_check_config_fields_are_validated(log_config: dict[str, Any]) -> None:
    config = log_config | {
        "system_check": {
            "class": "modulon.system_check.SystemCheckFactory",
            "config": {
                "above": {"class": f"{PREFIX}.ThresholdCheck", "value": 10, "minimum": 5},
            },
        },
    }
    graph = await Graph.from_config(config)

    assert graph.system_check is not None


@pytest.mark.asyncio
async def test_failing_threshold_aborts_graph_build(log_config: dict[str, Any]) -> None:
    config = log_config | {
        "system_check": {
            "class": "modulon.system_check.SystemCheckFactory",
            "config": {
                "below": {"class": f"{PREFIX}.ThresholdCheck", "value": 1, "minimum": 5},
            },
        },
    }
    with pytest.raises(BaseExceptionGroup) as excinfo:
        await Graph.from_config(config)

    assert excinfo.value.subgroup(SystemChecksFailedError) is not None


@pytest.mark.asyncio
async def test_no_checks_configured_raises_error(log_config: dict[str, Any]) -> None:
    config = log_config | {
        "system_check": {"class": "modulon.system_check.SystemCheckFactory"},
    }

    with pytest.raises(ValidationError):
        await Graph.from_config(config)


@pytest.mark.asyncio
async def test_non_check_class_raises_type_error(log_config: dict[str, Any]) -> None:
    config = log_config | {
        "system_check": {
            "class": "modulon.system_check.SystemCheckFactory",
            "config": {"bogus": {"class": f"{PREFIX}.NotACheck"}},
        },
    }
    with pytest.raises(BaseExceptionGroup) as excinfo:
        await Graph.from_config(config)

    assert excinfo.value.subgroup(TypeError) is not None
