"""Logic associated with presenting information about finished use-cases."""
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
    state: str


def create_operation_response_presenter(
    translator: IdentificationTranslator, show: Callable[[OperationRecord], None]
) -> Callable[[OperationResponse], None]:
    """Create a callable that when called presents information about a finished operation."""

    def get_class_name(obj: type) -> str:
        return obj.__name__

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
                            get_class_name(update.transition.current).upper(),
                            get_class_name(update.transition.new).upper(),
                        ),
                    )
                    for update in response.updates
                ],
                [
                    Failure(
                        translator.to_primary_key(error.identifier),
                        error.operation.name,
                        get_class_name(error.state).upper(),
                    )
                    for error in response.errors
                ],
            )
        )

    return present_operation_response


def create_idle_entities_updater(
    translator: IdentificationTranslator, update: Callable[[Iterable[PrimaryKey]], None]
) -> Callable[[ListIdleEntitiesResponseModel], None]:
    """Create a callable that when called updates the list of idle entities."""

    def update_idle_entities(response: ListIdleEntitiesResponseModel) -> None:
        update(translator.to_primary_key(identifier) for identifier in response.identifiers)

    return update_idle_entities
