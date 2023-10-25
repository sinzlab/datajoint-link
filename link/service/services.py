"""Contains all the services."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto

from link.domain import commands, events
from link.domain.custom_types import Identifier
from link.domain.state import Operations, states

from .uow import UnitOfWork


class Request:
    """Base class for all request models."""


class Response:
    """Base class for all response models."""


@dataclass(frozen=True)
class PullRequest(Request):
    """Request model for the pull use-case."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class PullResponse(Response):
    """Response model for the pull use-case."""

    requested: frozenset[Identifier]
    errors: frozenset[events.InvalidOperationRequested]


def pull(
    request: PullRequest,
    *,
    process_to_completion_service: Callable[[commands.FullyProcessLink], ProcessToCompletionResponse],
    start_pull_process_service: Callable[[commands.StartPullProcess], events.LinkStateChanged],
    output_port: Callable[[PullResponse], None],
) -> None:
    """Pull entities across the link."""
    process_to_completion_service(commands.FullyProcessLink(request.requested))
    response = start_pull_process_service(commands.StartPullProcess(request.requested))
    errors = (error for error in response.errors if error.state is states.Deprecated)
    process_to_completion_service(commands.FullyProcessLink(request.requested))
    output_port(PullResponse(request.requested, errors=frozenset(errors)))


@dataclass(frozen=True)
class DeleteRequest(Request):
    """Request model for the delete use-case."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class DeleteResponse(Response):
    """Response model for the delete use-case."""

    requested: frozenset[Identifier]


def delete(
    request: DeleteRequest,
    *,
    process_to_completion_service: Callable[[commands.FullyProcessLink], ProcessToCompletionResponse],
    start_delete_process_service: Callable[[commands.StartDeleteProcess], events.LinkStateChanged],
    output_port: Callable[[DeleteResponse], None],
) -> None:
    """Delete pulled entities."""
    process_to_completion_service(commands.FullyProcessLink(request.requested))
    start_delete_process_service(commands.StartDeleteProcess(request.requested))
    process_to_completion_service(commands.FullyProcessLink(request.requested))
    output_port(DeleteResponse(request.requested))


@dataclass(frozen=True)
class ProcessToCompletionResponse(Response):
    """Response model for the process to completion use-case."""

    requested: frozenset[Identifier]


def process_to_completion(
    command: commands.FullyProcessLink,
    *,
    process_service: Callable[[commands.ProcessLink], events.LinkStateChanged],
    output_port: Callable[[ProcessToCompletionResponse], None],
) -> None:
    """Process entities until their processes are complete."""
    while process_service(commands.ProcessLink(command.requested)).updates:
        pass
    output_port(ProcessToCompletionResponse(command.requested))


def start_pull_process(
    command: commands.StartPullProcess,
    *,
    uow: UnitOfWork,
    output_port: Callable[[events.LinkStateChanged], None],
) -> None:
    """Start the pull process for the requested entities."""
    with uow:
        result = uow.link.apply(Operations.START_PULL, requested=command.requested).events[0]
        uow.commit()
    assert isinstance(result, events.LinkStateChanged)
    output_port(result)


def start_delete_process(
    command: commands.StartDeleteProcess,
    *,
    uow: UnitOfWork,
    output_port: Callable[[events.LinkStateChanged], None],
) -> None:
    """Start the delete process for the requested entities."""
    with uow:
        result = uow.link.apply(Operations.START_DELETE, requested=command.requested).events[0]
        uow.commit()
    assert isinstance(result, events.LinkStateChanged)
    output_port(result)


def process(
    command: commands.ProcessLink, *, uow: UnitOfWork, output_port: Callable[[events.LinkStateChanged], None]
) -> None:
    """Process entities."""
    with uow:
        result = uow.link.apply(Operations.PROCESS, requested=command.requested).events[0]
        uow.commit()
    assert isinstance(result, events.LinkStateChanged)
    output_port(result)


def list_idle_entities(
    command: commands.ListIdleEntities,
    *,
    uow: UnitOfWork,
    output_port: Callable[[events.IdleEntitiesListed], None],
) -> None:
    """List all idle entities."""
    with uow:
        uow.link.list_idle_entities()
        event = uow.link.events[-1]
        assert isinstance(event, events.IdleEntitiesListed)
        output_port(event)


class Services(Enum):
    """Names for all available services."""

    PULL = auto()
    DELETE = auto()
    PROCESS = auto()
    LIST_IDLE_ENTITIES = auto()
