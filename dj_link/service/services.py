"""Contains all the services."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto

from dj_link.domain.custom_types import Identifier
from dj_link.domain.link import process as process_domain_service
from dj_link.domain.link import start_delete as delete_domain_service
from dj_link.domain.link import start_pull as pull_domain_service
from dj_link.domain.state import InvalidOperation, Operations, Update, states

from .gateway import LinkGateway


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


def pull(
    request: PullRequest,
    *,
    link_gateway: LinkGateway,
    process_to_completion_service: Callable[[ProcessToCompletionRequest], ProcessToCompletionResponse],
    output_port: Callable[[PullResponse], None],
) -> None:
    """Pull entities across the link."""
    process_to_completion_service(ProcessToCompletionRequest(request.requested))
    result = pull_domain_service(link_gateway.create_link(), requested=request.requested)
    link_gateway.apply(result.updates)
    process_to_completion_service(ProcessToCompletionRequest(request.requested))
    output_port(PullResponse(request.requested))


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
    link_gateway: LinkGateway,
    process_to_completion_service: Callable[[ProcessToCompletionRequest], ProcessToCompletionResponse],
    output_port: Callable[[DeleteResponse], None],
) -> None:
    """Delete pulled entities."""
    process_to_completion_service(ProcessToCompletionRequest(request.requested))
    result = delete_domain_service(link_gateway.create_link(), requested=request.requested)
    link_gateway.apply(result.updates)
    process_to_completion_service(ProcessToCompletionRequest(request.requested))
    output_port(DeleteResponse(request.requested))


@dataclass(frozen=True)
class ProcessToCompletionRequest(Request):
    """Request model for the process to completion use-case."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class ProcessToCompletionResponse(Response):
    """Response model for the process to completion use-case."""

    requested: frozenset[Identifier]


def process_to_completion(
    request: ProcessToCompletionRequest,
    *,
    process_service: Callable[[ProcessRequest], OperationResponse],
    output_port: Callable[[ProcessToCompletionResponse], None],
) -> None:
    """Process entities until their processes are complete."""
    while process_service(ProcessRequest(request.requested)).updates:
        pass
    output_port(ProcessToCompletionResponse(request.requested))


@dataclass(frozen=True)
class ProcessRequest(Request):
    """Request model for the process use-case."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class OperationResponse(Response):
    """Response model for all use-cases that operate on entities."""

    operation: Operations
    requested: frozenset[Identifier]
    updates: frozenset[Update]
    errors: frozenset[InvalidOperation]


def process(
    request: ProcessRequest, *, link_gateway: LinkGateway, output_port: Callable[[OperationResponse], None]
) -> None:
    """Process entities."""
    result = process_domain_service(link_gateway.create_link(), requested=request.requested)
    link_gateway.apply(result.updates)
    output_port(OperationResponse(result.operation, request.requested, result.updates, result.errors))


@dataclass(frozen=True)
class ListIdleEntitiesRequest(Request):
    """Request model for the use-case that lists idle entities."""


@dataclass(frozen=True)
class ListIdleEntitiesResponse(Response):
    """Response model for the use-case that lists idle entities."""

    identifiers: frozenset[Identifier]


def list_idle_entities(
    request: ListIdleEntitiesRequest,
    *,
    link_gateway: LinkGateway,
    output_port: Callable[[ListIdleEntitiesResponse], None],
) -> None:
    """List all idle entities."""
    output_port(
        ListIdleEntitiesResponse(
            frozenset(entity.identifier for entity in link_gateway.create_link() if entity.state is states.Idle)
        )
    )


class UseCases(Enum):
    """Names for all available use-cases."""

    PULL = auto()
    DELETE = auto()
    PROCESS = auto()
    LISTIDLEENTITIES = auto()
