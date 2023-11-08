from __future__ import annotations

from functools import partial
from typing import Callable, Generic, TypedDict, TypeVar

import pytest

from link.domain import commands, events
from link.domain.state import Components, Processes, State, states
from link.service.handlers import delete, delete_entity, list_idle_entities, pull, pull_entity
from link.service.messagebus import CommandHandlers, EventHandlers, MessageBus
from link.service.uow import UnitOfWork
from tests.assignments import create_assignments, create_identifiers

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
    elif state in (states.Idle, states.Pulled):
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
    if state is states.Idle:
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
    command_handlers: CommandHandlers = {}
    event_handlers: EventHandlers = {}
    bus = MessageBus(uow, command_handlers, event_handlers)
    command_handlers[commands.PullEntity] = partial(pull_entity, uow=uow, message_bus=bus)
    event_handlers[events.InvalidOperationRequested] = [lambda event: None]
    event_handlers[events.StateChanged] = [lambda event: None]
    event_handlers[events.ProcessStarted] = [lambda event: None]
    event_handlers[events.ProcessFinished] = [lambda event: None]
    event_handlers[events.ProcessesStarted] = [lambda event: None]
    event_handlers[events.ProcessesFinished] = [lambda event: None]
    return partial(pull, message_bus=bus)


def create_delete_service(uow: UnitOfWork) -> Callable[[commands.DeleteEntities], None]:
    command_handlers: CommandHandlers = {}
    event_handlers: EventHandlers = {}
    bus = MessageBus(uow, command_handlers, event_handlers)
    command_handlers[commands.DeleteEntity] = partial(delete_entity, uow=uow, message_bus=bus)
    event_handlers[events.InvalidOperationRequested] = [lambda event: None]
    event_handlers[events.StateChanged] = [lambda event: None]
    event_handlers[events.ProcessStarted] = [lambda event: None]
    event_handlers[events.ProcessFinished] = [lambda event: None]
    event_handlers[events.ProcessesStarted] = [lambda event: None]
    event_handlers[events.ProcessesFinished] = [lambda event: None]
    return partial(delete, message_bus=bus)


class EntityConfig(TypedDict):
    state: type[State]
    is_tainted: bool
    process: Processes | None


STATES: list[EntityConfig] = [
    {"state": states.Idle, "is_tainted": False, "process": None},
    {"state": states.Activated, "is_tainted": False, "process": Processes.PULL},
    {"state": states.Activated, "is_tainted": False, "process": Processes.DELETE},
    {"state": states.Activated, "is_tainted": True, "process": Processes.PULL},
    {"state": states.Activated, "is_tainted": True, "process": Processes.DELETE},
    {"state": states.Received, "is_tainted": False, "process": Processes.PULL},
    {"state": states.Received, "is_tainted": False, "process": Processes.DELETE},
    {"state": states.Received, "is_tainted": True, "process": Processes.PULL},
    {"state": states.Received, "is_tainted": True, "process": Processes.DELETE},
    {"state": states.Pulled, "is_tainted": False, "process": None},
    {"state": states.Tainted, "is_tainted": True, "process": None},
    {"state": states.Deprecated, "is_tainted": True, "process": None},
]


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (STATES[0], states.Idle),
        (STATES[1], states.Idle),
        (STATES[2], states.Idle),
        (STATES[3], states.Deprecated),
        (STATES[4], states.Deprecated),
        (STATES[5], states.Idle),
        (STATES[6], states.Idle),
        (STATES[7], states.Deprecated),
        (STATES[8], states.Deprecated),
        (STATES[9], states.Idle),
        (STATES[10], states.Deprecated),
        (STATES[11], states.Deprecated),
    ],
)
def test_deleted_entity_ends_in_correct_state(state: EntityConfig, expected: type[State]) -> None:
    uow = create_uow(**state)
    delete_service = create_delete_service(uow)
    delete_service(commands.DeleteEntities(frozenset(create_identifiers("1"))))
    with uow:
        assert next(iter(uow.link)).state is expected


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (STATES[0], states.Pulled),
        (STATES[1], states.Pulled),
        (STATES[2], states.Pulled),
        (STATES[3], states.Deprecated),
        (STATES[4], states.Deprecated),
        (STATES[5], states.Pulled),
        (STATES[6], states.Pulled),
        (STATES[7], states.Tainted),
        (STATES[8], states.Deprecated),
        (STATES[9], states.Pulled),
        (STATES[10], states.Tainted),
        (STATES[11], states.Deprecated),
    ],
)
def test_pulled_entity_ends_in_correct_state(state: EntityConfig, expected: type[State]) -> None:
    uow = create_uow(**state)
    pull_service = create_pull_service(uow)
    pull_service(commands.PullEntities(frozenset(create_identifiers("1"))))
    with uow:
        assert next(iter(uow.link)).state is expected


def test_correct_response_model_gets_passed_to_list_idle_entities_output_port() -> None:
    uow = UnitOfWork(
        FakeLinkGateway(
            create_assignments({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"2"}, Components.LOCAL: {"2"}})
        )
    )
    output_port = FakeOutputPort[events.IdleEntitiesListed]()
    list_idle_entities(commands.ListIdleEntities(), uow=uow, output_port=output_port)
    assert set(output_port.response.identifiers) == create_identifiers("1")
