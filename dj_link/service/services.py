"""Contains all the services."""
from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Generic, TypeVar

from dj_link.domain.custom_types import Identifier
from dj_link.domain.link import process as process_domain_service
from dj_link.domain.link import start_delete as delete_domain_service
from dj_link.domain.link import start_pull as pull_domain_service
from dj_link.domain.state import InvalidOperation, Operations, Update, states

from .gateway import LinkGateway


class RequestModel:
    """Base class for all request models."""


class ResponseModel:
    """Base class for all response models."""


@dataclass(frozen=True)
class PullRequestModel(RequestModel):
    """Request model for the pull use-case."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class OperationResponse(ResponseModel):
    """Response model for all use-cases that operate on entities."""

    operation: Operations
    requested: frozenset[Identifier]
    updates: frozenset[Update]
    errors: frozenset[InvalidOperation]


@dataclass(frozen=True)
class PullResponse(ResponseModel):
    """Response model for the pull use-case."""

    requested: frozenset[Identifier]


def pull(
    request: PullRequestModel,
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
class DeleteRequestModel(RequestModel):
    """Request model for the delete use-case."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class DeleteResponse(ResponseModel):
    """Response model for the delete use-case."""

    requested: frozenset[Identifier]


def delete(
    request: DeleteRequestModel,
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
class ProcessRequestModel(RequestModel):
    """Request model for the process use-case."""

    requested: frozenset[Identifier]


def process(
    request: ProcessRequestModel, *, link_gateway: LinkGateway, output_port: Callable[[OperationResponse], None]
) -> None:
    """Process entities."""
    result = process_domain_service(link_gateway.create_link(), requested=request.requested)
    link_gateway.apply(result.updates)
    output_port(OperationResponse(result.operation, request.requested, result.updates, result.errors))


@dataclass(frozen=True)
class ProcessToCompletionRequest(RequestModel):
    """Request model for the process to completion use-case."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class ProcessToCompletionResponse(ResponseModel):
    """Response model for the process to completion use-case."""

    requested: frozenset[Identifier]


_T = TypeVar("_T", bound=ResponseModel)


class ResponseRelay(Generic[_T]):
    """A relay that makes the response of one service available to another."""

    def __init__(self) -> None:
        """Initialize the relay."""
        self._response: _T | None = None

    @property
    def response(self) -> _T:
        """Return the response of the relayed service."""
        assert self._response is not None
        return self._response

    def get_response(self) -> _T:
        """Return the response of the relayed service."""
        assert self._response is not None
        return self._response

    def __call__(self, response: _T) -> None:
        """Store the response of the relayed service."""
        self._response = response


_V = TypeVar("_V", bound=RequestModel)


def create_returning_service(service: Callable[[_V], None], get_response: Callable[[], _T]) -> Callable[[_V], _T]:
    """Create a version of the provided service that returns its response when executed."""

    def execute(request: _V) -> _T:
        service(request)
        return get_response()

    return execute


def create_response_forwarder(recipients: Iterable[Callable[[_T], None]]) -> Callable[[_T], None]:
    """Create an object that forwards the response it gets called with to multiple recipients."""
    recipients = list(recipients)

    def duplicate_response(response: _T) -> None:
        for recipient in recipients:
            recipient(response)

    return duplicate_response


def process_to_completion(
    request: ProcessToCompletionRequest,
    *,
    process_service: Callable[[ProcessRequestModel], OperationResponse],
    output_port: Callable[[ProcessToCompletionResponse], None],
) -> None:
    """Process entities until their processes are complete."""
    while process_service(ProcessRequestModel(request.requested)).updates:
        pass
    output_port(ProcessToCompletionResponse(request.requested))


@dataclass(frozen=True)
class ListIdleEntitiesRequestModel(RequestModel):
    """Request model for the use-case that lists idle entities."""


@dataclass(frozen=True)
class ListIdleEntitiesResponseModel(ResponseModel):
    """Response model for the use-case that lists idle entities."""

    identifiers: frozenset[Identifier]


def list_idle_entities(
    request: ListIdleEntitiesRequestModel,
    *,
    link_gateway: LinkGateway,
    output_port: Callable[[ListIdleEntitiesResponseModel], None],
) -> None:
    """List all idle entities."""
    output_port(
        ListIdleEntitiesResponseModel(
            frozenset(entity.identifier for entity in link_gateway.create_link() if entity.state is states.Idle)
        )
    )


class UseCases(Enum):
    """Names for all available use-cases."""

    PULL = auto()
    DELETE = auto()
    PROCESS = auto()
    LISTIDLEENTITIES = auto()
