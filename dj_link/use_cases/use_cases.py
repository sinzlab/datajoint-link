"""Contains all the use-cases."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto

from dj_link.entities.custom_types import Identifier
from dj_link.entities.link import delete as delete_domain_service
from dj_link.entities.link import process as process_domain_service
from dj_link.entities.link import pull as pull_domain_service
from dj_link.entities.state import states

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
class PullResponseModel(ResponseModel):
    """Response model for the pull use-case."""


def pull(
    request: PullRequestModel, *, link_gateway: LinkGateway, output_port: Callable[[PullResponseModel], None]
) -> None:
    """Pull entities across the link."""
    result = pull_domain_service(link_gateway.create_link(), requested=request.requested)
    link_gateway.apply(result.updates)
    output_port(PullResponseModel())


@dataclass(frozen=True)
class DeleteRequestModel(RequestModel):
    """Request model for the delete use-case."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class DeleteResponseModel(ResponseModel):
    """Response model for the delete use-case."""


def delete(
    request: DeleteRequestModel, *, link_gateway: LinkGateway, output_port: Callable[[DeleteResponseModel], None]
) -> None:
    """Delete pulled entities."""
    result = delete_domain_service(link_gateway.create_link(), requested=request.requested)
    link_gateway.apply(result.updates)
    output_port(DeleteResponseModel())


@dataclass(frozen=True)
class ProcessRequestModel(RequestModel):
    """Request model for the process use-case."""

    requested: frozenset[Identifier]


@dataclass(frozen=True)
class ProcessResponseModel(ResponseModel):
    """Response model for the process use-case."""


def process(
    request: ProcessRequestModel, *, link_gateway: LinkGateway, output_port: Callable[[ProcessResponseModel], None]
) -> None:
    """Process entities."""
    result = process_domain_service(link_gateway.create_link(), requested=request.requested)
    link_gateway.apply(result.updates)
    output_port(ProcessResponseModel())


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
