import asyncio
from abc import abstractmethod
from collections.abc import Callable
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from packaging.requirements import InvalidRequirement, Requirement
from pydantic import AwareDatetime, Field, field_validator

from modulon.core.error import SystemChecksFailedError
from modulon.graph import AbstractComponent, AbstractFactory
from modulon.models import ImmutableModel
from modulon.utils import est_now, import_object


class SystemCheckResult(ImmutableModel):
    name: str
    passing: bool
    msg: str
    as_of: AwareDatetime = Field(default_factory=est_now)

    def __bool__(self) -> bool:
        return self.passing

    @property
    def status(self) -> str:
        return "PASS" if self.passing else "FAIL"

    def render(self) -> str:
        return f"System check '{self.name}' is {self.status}: {self.msg}"


class AbstractSystemCheckComponent(AbstractComponent, skip_setup=True, skip_run=True):
    """
    A component that performs a single system check.

    Subclasses carry a `name` (supplied by the factory from the config key) and
    implement the async `.evaluate()` method, returning a `SystemCheckResult` that
    describes whether the check passed. Checks are created and executed concurrently
    by the `SystemCheckFactory` registered at the ``system_check`` namespace.

    Dependencies (a subset of the factory's) are reachable through `.dep` and a
    pre-configured logger through `.log`. `_setup`/`_run` are skipped by default;
    override `_setup` if a check needs async initialization before it runs.
    """

    name: str

    @abstractmethod
    async def evaluate(self) -> SystemCheckResult: ...


class PackageDependencyCheck(AbstractSystemCheckComponent):
    """
    A system check that validates config-declared Python package dependencies against
    the packages installed in the current environment.

    `requirements` is a list of PEP 508 requirement strings (e.g. ``"pydantic>=2.9"``,
    ``"pandas>=2.0,<3"``, ``"requests"``). Each is checked against the installed
    distribution metadata: the check fails if a required package is missing or its
    installed version does not satisfy the requirement's version specifier.
    Requirements whose environment markers do not apply to the current environment
    (e.g. a different ``python_version``) are skipped.
    """

    requirements: list[str]

    @field_validator("requirements")
    @classmethod
    def _require_valid_pep508(cls, value: list[str]) -> list[str]:
        for requirement in value:
            try:
                Requirement(requirement)
            except InvalidRequirement as err:
                raise ValueError(f"invalid requirement '{requirement}': {err}") from err
        return value

    async def evaluate(self) -> SystemCheckResult:
        unmet: list[str] = []
        for requirement in map(Requirement, self.requirements):
            if requirement.marker and not requirement.marker.evaluate():
                continue  # requirement does not apply to this environment

            try:
                installed = version(requirement.name)
            except PackageNotFoundError:
                unmet.append(f"{requirement.name} is not installed")
                continue

            if not requirement.specifier.contains(installed, prereleases=True):
                unmet.append(
                    f"{requirement.name} {installed} does not satisfy "
                    f"'{requirement.specifier}'"
                )

        passing = not unmet
        msg = (
            f"{len(self.requirements)} dependency requirement(s) satisfied"
            if passing
            else "; ".join(unmet)
        )
        return SystemCheckResult(name=self.name, passing=passing, msg=msg)


class SystemCheckFactory(
    AbstractFactory,
    node_namespace="system_check",
    skip_run=True,
):
    """
    A generic, config-driven factory that creates and runs system checks.

    Each entry in `config` maps a unique name to a component spec of the form
    ``{"class": "importable.path.To.CheckComponent", ...}`` where the remaining keys
    are validated into the component as pydantic fields (the `name` is injected from
    the config key) -- mirroring the top-level graph config schema. Every configured
    check is instantiated via `_create_component` and its `.evaluate()` is awaited
    concurrently during `_setup()`. Each result is logged; if any check fails, setup
    raises `ValueError`, aborting graph construction.
    """

    config: dict[str, dict[str, Any]]

    _components: dict[str, AbstractSystemCheckComponent] = {}
    _results: dict[str, SystemCheckResult] = {}

    async def _setup(self) -> None:
        for name, spec in self.config.items():
            spec = dict(spec)  # copy so the stored config `class` key is left intact
            klass_location = spec.pop("class")
            klass = import_object(klass_location)
            if not (
                isinstance(klass, type)
                and issubclass(klass, AbstractSystemCheckComponent)
            ):
                raise TypeError(
                    f"System check '{name}' class {klass_location} does not subclass "
                    "AbstractSystemCheckComponent"
                )
            component = await self._create_component(klass, name=name, **spec)
            self._components[name] = component

        def get_callback(name: str) -> Callable[[asyncio.Task[SystemCheckResult]], Any]:
            def callback(task: asyncio.Task[SystemCheckResult]) -> None:
                self._results[name] = result = task.result()
                self.log.info(result.render())

            return callback

        async with asyncio.TaskGroup() as tg:
            for name, component in self._components.items():
                coro = component.evaluate()
                task = tg.create_task(coro, name=name)
                task.add_done_callback(get_callback(name))

        failing_checks = [n for n, r in self._results.items() if not r.passing]
        if any(failing_checks):
            msg = f"System checks failed: {failing_checks}"
            self.log.error(msg)
            raise SystemChecksFailedError(msg)

        self.log.info("System checks passed")
