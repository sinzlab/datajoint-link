"""Contains all the use-cases."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from dj_link.entities.custom_types import Identifier
from dj_link.entities.link import delete as delete_domain_service
from dj_link.entities.link import process
from dj_link.entities.link import pull as pull_domain_service

from .gateway import LinkGateway


class ResponseModel:  # pylint: disable=too-few-public-methods
    """Base class for all response models."""


@dataclass(frozen=True)
class PullRequestModel:
    """Request model for the pull use-case."""

    identifiers: frozenset[Identifier]


@dataclass(frozen=True)
class PullResponseModel(ResponseModel):
    """Response model for the pull use-case."""


def pull(
    request: PullRequestModel, *, link_gateway: LinkGateway, output_port: Callable[[PullResponseModel], None]
) -> None:
    """Pull entities across the link."""
    result = pull_domain_service(link_gateway.create_link(), requested=request.identifiers)
    while result.updates:
        link_gateway.apply(result.updates)
        result = process(link_gateway.create_link())
    output_port(PullResponseModel())


@dataclass(frozen=True)
class DeleteRequestModel:
    """Request model for the delete use-case."""

    identifiers: frozenset[Identifier]


@dataclass(frozen=True)
class DeleteResponseModel(ResponseModel):
    """Response model for the delete use-case."""


def delete(
    request: DeleteRequestModel, *, link_gateway: LinkGateway, output_port: Callable[[DeleteResponseModel], None]
) -> None:
    """Delete pulled entities."""
    result = delete_domain_service(link_gateway.create_link(), requested=request.identifiers)
    while result.updates:
        link_gateway.apply(result.updates)
        result = process(link_gateway.create_link())
    output_port(DeleteResponseModel())
