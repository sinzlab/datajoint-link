"""Contains all the use-cases."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Iterable

from dj_link.entities.custom_types import Identifier
from dj_link.entities.link import delete as delete_domain_service
from dj_link.entities.link import process as process_domain_service
from dj_link.entities.link import pull as pull_domain_service

from .gateway import LinkGateway


class ResponseModel:
    """Base class for all response models."""


@dataclass(frozen=True)
class PullResponseModel(ResponseModel):
    """Response model for the pull use-case."""


def pull(
    requested: Iterable[Identifier], *, link_gateway: LinkGateway, output_port: Callable[[PullResponseModel], None]
) -> None:
    """Pull entities across the link."""
    result = pull_domain_service(link_gateway.create_link(), requested=requested)
    link_gateway.apply(result.updates)
    output_port(PullResponseModel())


@dataclass(frozen=True)
class DeleteResponseModel(ResponseModel):
    """Response model for the delete use-case."""


def delete(
    requested: Iterable[Identifier], *, link_gateway: LinkGateway, output_port: Callable[[DeleteResponseModel], None]
) -> None:
    """Delete pulled entities."""
    result = delete_domain_service(link_gateway.create_link(), requested=requested)
    link_gateway.apply(result.updates)
    output_port(DeleteResponseModel())


@dataclass(frozen=True)
class ProcessResponseModel(ResponseModel):
    """Response model for the process use-case."""


def process(
    requested: Iterable[Identifier], *, link_gateway: LinkGateway, output_port: Callable[[ProcessResponseModel], None]
) -> None:
    """Process entities."""
    result = process_domain_service(link_gateway.create_link(), requested=requested)
    link_gateway.apply(result.updates)
    output_port(ProcessResponseModel())


class UseCases(Enum):
    """Names for all available use-cases."""

    PULL = auto()
    DELETE = auto()
    PROCESS = auto()
