"""Contains the presenter class and related classes/functions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

from dj_link.use_cases.use_cases import (
    ListIdleEntitiesResponseModel,
    OperationResponse,
)

from .custom_types import PrimaryKey
from .identification import IdentificationTranslator


@dataclass(frozen=True)
class OperationRecord:
    """Record of a finished operation."""

    requests: list[Request]
    successes: list[Sucess]
    failures: list[Failure]


@dataclass(frozen=True)
class Request:
    """Record of a request to perform a certain operation on a particular entity."""

    primary_key: PrimaryKey
    operation: str


@dataclass(frozen=True)
class Sucess:
    """Record of a successful operation on a particular entity."""

    primary_key: PrimaryKey
    operation: str
    transition: Transition


@dataclass(frozen=True)
class Transition:
    """Record of a transition between two states."""

    old: str
    new: str


@dataclass(frozen=True)
class Failure:
    """Record of a failed operation on a particular entity."""

    primary_key: PrimaryKey
    operation: str


class DJPresenter:
    """DataJoint-specific presenter."""

    def __init__(
        self,
        translator: IdentificationTranslator,
        *,
        update_idle_entities_list: Callable[[Iterable[PrimaryKey]], None],
    ) -> None:
        """Initialize the presenter."""
        self._translator = translator
        self._update_idle_entities_list = update_idle_entities_list

    def update_idle_entities_list(self, response: ListIdleEntitiesResponseModel) -> None:
        """Update the list of idle entities."""
        self._update_idle_entities_list(
            self._translator.to_primary_key(identifier) for identifier in response.identifiers
        )


def create_operation_response_presenter(
    translator: IdentificationTranslator, show: Callable[[OperationRecord], None]
) -> Callable[[OperationResponse], None]:
    """Create a callable that when called presents information about a finished operation."""

    def present_operation_response(response: OperationResponse) -> None:
        show(
            OperationRecord(
                [
                    Request(translator.to_primary_key(identifier), response.operation.name)
                    for identifier in response.requested
                ],
                [
                    Sucess(
                        translator.to_primary_key(update.identifier),
                        operation=response.operation.name,
                        transition=Transition(
                            update.transition.current.__name__.upper(), update.transition.new.__name__.upper()
                        ),
                    )
                    for update in response.updates
                ],
                [
                    Failure(translator.to_primary_key(operation.identifier), operation.operation.name)
                    for operation in response.errors
                ],
            )
        )

    return present_operation_response
