from __future__ import annotations

from functools import partial
from typing import Callable, Generic, TypedDict, TypeVar, cast

import pytest

from link.domain import commands, events
from link.domain.state import Components, Processes, State, states
from link.service.handlers import delete, delete_entity, pull, pull_entity
from link.service.messagebus import CommandHandlers, EventHandlers, MessageBus
from link.service.uow import UnitOfWork
from tests.assignments import create_assignments, create_identifier, create_identifiers

from .gateway import FakeLinkGateway

T = TypeVar("T", bound=events.Event)


class FakeOutputPort(Generic[T]):
    def __init__(self) -> None:
        self._response: T | None = None

    @property
    def response(self) -> T:
        assert self._response is not None
        return self._response

    def __call__(self, response: T) -> None:
        self._response = response


def create_uow(state: type[State], process: Processes | None = None, is_tainted: bool = False) -> UnitOfWork:
    if state in (states.Activated, states.Received):
        assert process is not None
    else:
        assert process is None
    if state in (states.Tainted, states.Deprecated):
        assert is_tainted
    elif state in (states.Unshared, states.Shared):
        assert not is_tainted

    if is_tainted:
        tainted_identifiers = create_identifiers("1")
    else:
        tainted_identifiers = set()
    if process is not None:
        processes = {process: create_identifiers("1")}
    else:
        processes = {}
    assignments = {Components.SOURCE: {"1"}}
    if state is states.Unshared:
        return UnitOfWork(
            FakeLinkGateway(
                create_assignments(assignments), tainted_identifiers=tainted_identifiers, processes=processes
            )
        )
    assignments[Components.OUTBOUND] = {"1"}
    if state in (states.Deprecated, states.Activated):
        return UnitOfWork(
            FakeLinkGateway(
                create_assignments(assignments), tainted_identifiers=tainted_identifiers, processes=processes
            )
        )
    assignments[Components.LOCAL] = {"1"}
    return UnitOfWork(
        FakeLinkGateway(create_assignments(assignments), tainted_identifiers=tainted_identifiers, processes=processes)
    )


_Event_co = TypeVar("_Event_co", bound=events.Event, covariant=True)

_Command_contra = TypeVar("_Command_contra", bound=commands.Command, contravariant=True)


def create_pull_service(uow: UnitOfWork) -> Callable[[commands.PullEntities], None]:
    command_handlers = cast(CommandHandlers, {})
    event_handlers = cast(EventHandlers, {})
    bus = MessageBus(uow, command_handlers, event_handlers)
    command_handlers[commands.PullEntity] = partial(pull_entity, uow=uow, message_bus=bus)
    event_handlers[events.InvalidOperationRequested] = [lambda event: None]
    event_handlers[events.StateChanged] = [lambda event: None]
    event_handlers[events.ProcessStarted] = [lambda event: None]
    event_handlers[events.ProcessFinished] = [lambda event: None]
    event_handlers[events.BatchProcessingStarted] = [lambda event: None]
    event_handlers[events.BatchProcessingFinished] = [lambda event: None]
    return partial(pull, message_bus=bus)


def create_delete_service(uow: UnitOfWork) -> Callable[[commands.DeleteEntities], None]:
    command_handlers = cast(CommandHandlers, {})
    event_handlers = cast(EventHandlers, {})
    bus = MessageBus(uow, command_handlers, event_handlers)
    command_handlers[commands.DeleteEntity] = partial(delete_entity, uow=uow, message_bus=bus)
    event_handlers[events.InvalidOperationRequested] = [lambda event: None]
    event_handlers[events.StateChanged] = [lambda event: None]
    event_handlers[events.ProcessStarted] = [lambda event: None]
    event_handlers[events.ProcessFinished] = [lambda event: None]
    event_handlers[events.BatchProcessingStarted] = [lambda event: None]
    event_handlers[events.BatchProcessingFinished] = [lambda event: None]
    return partial(delete, message_bus=bus)


class EntityConfig(TypedDict):
    state: type[State]
    is_tainted: bool
    process: Processes | None


STATES: list[EntityConfig] = [
    {"state": states.Unshared, "is_tainted": False, "process": None},
    {"state": states.Activated, "is_tainted": False, "process": Processes.PULL},
    {"state": states.Activated, "is_tainted": False, "process": Processes.DELETE},
    {"state": states.Activated, "is_tainted": True, "process": Processes.PULL},
    {"state": states.Activated, "is_tainted": True, "process": Processes.DELETE},
    {"state": states.Received, "is_tainted": False, "process": Processes.PULL},
    {"state": states.Received, "is_tainted": False, "process": Processes.DELETE},
    {"state": states.Received, "is_tainted": True, "process": Processes.PULL},
    {"state": states.Received, "is_tainted": True, "process": Processes.DELETE},
    {"state": states.Shared, "is_tainted": False, "process": None},
    {"state": states.Tainted, "is_tainted": True, "process": None},
    {"state": states.Deprecated, "is_tainted": True, "process": None},
]


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (STATES[0], states.Unshared),
        (STATES[1], states.Unshared),
        (STATES[2], states.Unshared),
        (STATES[3], states.Deprecated),
        (STATES[4], states.Deprecated),
        (STATES[5], states.Unshared),
        (STATES[6], states.Unshared),
        (STATES[7], states.Deprecated),
        (STATES[8], states.Deprecated),
        (STATES[9], states.Unshared),
        (STATES[10], states.Deprecated),
        (STATES[11], states.Deprecated),
    ],
)
def test_deleted_entity_ends_in_correct_state(state: EntityConfig, expected: type[State]) -> None:
    uow = create_uow(**state)
    delete_service = create_delete_service(uow)
    delete_service(commands.DeleteEntities(frozenset(create_identifiers("1"))))
    with uow:
        assert uow.entities.create_entity(create_identifier("1")).state is expected


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (STATES[0], states.Shared),
        (STATES[1], states.Shared),
        (STATES[2], states.Shared),
        (STATES[3], states.Deprecated),
        (STATES[4], states.Deprecated),
        (STATES[5], states.Shared),
        (STATES[6], states.Shared),
        (STATES[7], states.Tainted),
        (STATES[8], states.Deprecated),
        (STATES[9], states.Shared),
        (STATES[10], states.Tainted),
        (STATES[11], states.Deprecated),
    ],
)
def test_pulled_entity_ends_in_correct_state(state: EntityConfig, expected: type[State]) -> None:
    uow = create_uow(**state)
    pull_service = create_pull_service(uow)
    pull_service(commands.PullEntities(frozenset(create_identifiers("1"))))
    with uow:
        assert uow.entities.create_entity(create_identifier("1")).state is expected
