from typing import Any

import pytest
from pydantic import ValidationError

from modulon import Graph
from modulon.core.error import SystemChecksFailedError
from modulon.system_check import (
    AbstractSystemCheckComponent,
    PackageDependencyCheck,
    SystemCheckResult,
)

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
                "above": {
                    "class": f"{PREFIX}.ThresholdCheck",
                    "value": 10,
                    "minimum": 5,
                },
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
                "below": {
                    "class": f"{PREFIX}.ThresholdCheck",
                    "value": 1,
                    "minimum": 5,
                },
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


# A package that is guaranteed absent from any environment running these tests.
MISSING_PACKAGE = "modulon-nonexistent-dependency-xyz"


@pytest.mark.asyncio
async def test_package_dependency_check_all_satisfied() -> None:
    check = PackageDependencyCheck(
        name="deps", requirements=["pydantic>=2.9", "packaging"]
    )
    result = await check.evaluate()

    assert result.passing is True
    assert result.msg == "2 dependency requirement(s) satisfied"


@pytest.mark.asyncio
async def test_package_dependency_check_missing_package() -> None:
    check = PackageDependencyCheck(name="deps", requirements=[MISSING_PACKAGE])
    result = await check.evaluate()

    assert result.passing is False
    assert f"{MISSING_PACKAGE} is not installed" in result.msg


@pytest.mark.asyncio
async def test_package_dependency_check_version_mismatch() -> None:
    check = PackageDependencyCheck(name="deps", requirements=["pydantic<2"])
    result = await check.evaluate()

    assert result.passing is False
    assert "does not satisfy" in result.msg


@pytest.mark.asyncio
async def test_package_dependency_check_inapplicable_marker_is_skipped() -> None:
    # The marker is false under any Python 3 interpreter, so the (missing) package
    # requirement is skipped rather than failing the check.
    check = PackageDependencyCheck(
        name="deps", requirements=[f"{MISSING_PACKAGE}; python_version < '3.0'"]
    )
    result = await check.evaluate()

    assert result.passing is True


def test_package_dependency_check_rejects_invalid_requirement() -> None:
    with pytest.raises(ValidationError):
        PackageDependencyCheck(name="deps", requirements=["=="])


@pytest.mark.asyncio
async def test_package_dependency_check_via_graph_passes(
    log_config: dict[str, Any],
) -> None:
    config = log_config | {
        "system_check": {
            "class": "modulon.system_check.SystemCheckFactory",
            "config": {
                "deps": {
                    "class": "modulon.system_check.PackageDependencyCheck",
                    "requirements": ["pydantic>=2.9"],
                },
            },
        },
    }
    graph = await Graph.from_config(config)

    assert graph.system_check is not None


@pytest.mark.asyncio
async def test_package_dependency_check_via_graph_aborts_on_missing(
    log_config: dict[str, Any],
) -> None:
    config = log_config | {
        "system_check": {
            "class": "modulon.system_check.SystemCheckFactory",
            "config": {
                "deps": {
                    "class": "modulon.system_check.PackageDependencyCheck",
                    "requirements": [MISSING_PACKAGE],
                },
            },
        },
    }
    with pytest.raises(BaseExceptionGroup) as excinfo:
        await Graph.from_config(config)

    assert excinfo.value.subgroup(SystemChecksFailedError) is not None
