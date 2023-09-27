from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from functools import partial
from typing import Generic, TypedDict, TypeVar

import pytest

from dj_link.domain.custom_types import Identifier
from dj_link.domain.link import Link, create_link
from dj_link.domain.state import Commands, Components, Operations, Processes, State, Update, states
from dj_link.service.gateway import LinkGateway
from dj_link.service.io import Service, make_responsive
from dj_link.service.services import (
    DeleteRequest,
    DeleteResponse,
    ListIdleEntitiesRequest,
    ListIdleEntitiesResponse,
    OperationResponse,
    ProcessRequest,
    ProcessToCompletionRequest,
    PullRequest,
    PullResponse,
    Response,
    delete,
    list_idle_entities,
    process,
    process_to_completion,
    pull,
    start_delete_process,
    start_pull_process,
)
from tests.assignments import create_assignments, create_identifier, create_identifiers


class FakeLinkGateway(LinkGateway):
    def __init__(
        self,
        assignments: Mapping[Components, Iterable[Identifier]],
        *,
        tainted_identifiers: Iterable[Identifier] | None = None,
        processes: Mapping[Processes, Iterable[Identifier]] | None = None,
    ) -> None:
        self.assignments = {component: set(identifiers) for component, identifiers in assignments.items()}
        self.tainted_identifiers = set(tainted_identifiers) if tainted_identifiers is not None else set()
        self.processes: dict[Processes, set[Identifier]] = {process: set() for process in Processes}
        if processes is not None:
            for entity_process, identifiers in processes.items():
                self.processes[entity_process].update(identifiers)

    def create_link(self) -> Link:
        return create_link(self.assignments, tainted_identifiers=self.tainted_identifiers, processes=self.processes)

    def apply(self, updates: Iterable[Update]) -> None:
        for update in updates:
            if update.command is Commands.START_PULL_PROCESS:
                self.processes[Processes.PULL].add(update.identifier)
                self.assignments[Components.OUTBOUND].add(update.identifier)
            elif update.command is Commands.ADD_TO_LOCAL:
                self.assignments[Components.LOCAL].add(update.identifier)
            elif update.command is Commands.FINISH_PULL_PROCESS:
                self.processes[Processes.PULL].remove(update.identifier)
            elif update.command is Commands.START_DELETE_PROCESS:
                self.processes[Processes.DELETE].add(update.identifier)
            elif update.command is Commands.REMOVE_FROM_LOCAL:
                self.assignments[Components.LOCAL].remove(update.identifier)
            elif update.command is Commands.FINISH_DELETE_PROCESS:
                self.processes[Processes.DELETE].remove(update.identifier)
                self.assignments[Components.OUTBOUND].remove(update.identifier)
            elif update.command is Commands.DEPRECATE:
                try:
                    self.processes[Processes.DELETE].remove(update.identifier)
                except KeyError:
                    self.processes[Processes.PULL].remove(update.identifier)
            else:
                raise ValueError("Unsupported command encountered")


T = TypeVar("T", bound=Response)


class FakeOutputPort(Generic[T]):
    def __init__(self) -> None:
        self._response: T | None = None

    @property
    def response(self) -> T:
        assert self._response is not None
        return self._response

    def __call__(self, response: T) -> None:
        self._response = response


def create_gateway(state: type[State], process: Processes | None = None, is_tainted: bool = False) -> FakeLinkGateway:
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
        return FakeLinkGateway(
            create_assignments(assignments), tainted_identifiers=tainted_identifiers, processes=processes
        )
    assignments[Components.OUTBOUND] = {"1"}
    if state in (states.Deprecated, states.Activated):
        return FakeLinkGateway(
            create_assignments(assignments), tainted_identifiers=tainted_identifiers, processes=processes
        )
    assignments[Components.LOCAL] = {"1"}
    return FakeLinkGateway(
        create_assignments(assignments), tainted_identifiers=tainted_identifiers, processes=processes
    )


def create_process_to_completion_service(gateway: FakeLinkGateway) -> Callable[[ProcessToCompletionRequest], None]:
    process_service = partial(make_responsive(partial(process, link_gateway=gateway)), output_port=lambda x: None)
    return partial(
        make_responsive(
            partial(
                process_to_completion,
                process_service=process_service,
            ),
        ),
        output_port=lambda x: None,
    )


def create_pull_service(gateway: FakeLinkGateway) -> Service[PullRequest, PullResponse]:
    process_to_completion_service = create_process_to_completion_service(gateway)
    start_pull_process_service = partial(
        make_responsive(partial(start_pull_process, link_gateway=gateway)), output_port=lambda x: None
    )
    return partial(
        pull,
        process_to_completion_service=process_to_completion_service,
        start_pull_process_service=start_pull_process_service,
    )


def create_delete_service(gateway: FakeLinkGateway) -> Service[DeleteRequest, DeleteResponse]:
    process_to_completion_service = create_process_to_completion_service(gateway)
    start_delete_process_service = partial(
        make_responsive(partial(start_delete_process, link_gateway=gateway)), output_port=lambda x: None
    )
    return partial(
        delete,
        process_to_completion_service=process_to_completion_service,
        start_delete_process_service=start_delete_process_service,
    )


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
    gateway = create_gateway(**state)
    delete_service = create_delete_service(gateway)
    delete_service(DeleteRequest(frozenset(create_identifiers("1"))), output_port=lambda x: None)
    assert next(iter(gateway.create_link())).state is expected


def test_correct_response_model_gets_passed_to_delete_output_port() -> None:
    gateway = FakeLinkGateway(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    output_port = FakeOutputPort[DeleteResponse]()
    delete_service = create_delete_service(gateway)
    delete_service(DeleteRequest(frozenset(create_identifiers("1"))), output_port=output_port)
    assert output_port.response.requested == create_identifiers("1")


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
    gateway = create_gateway(**state)
    pull_service = create_pull_service(gateway)
    pull_service(
        PullRequest(frozenset(create_identifiers("1"))),
        output_port=lambda x: None,
    )
    assert next(iter(gateway.create_link())).state is expected


def test_correct_response_model_gets_passed_to_pull_output_port() -> None:
    gateway = FakeLinkGateway(create_assignments({Components.SOURCE: {"1"}}))
    output_port = FakeOutputPort[PullResponse]()
    pull_service = create_pull_service(gateway)
    pull_service(
        PullRequest(frozenset(create_identifiers("1"))),
        output_port=output_port,
    )
    assert output_port.response.requested == create_identifiers("1")


def test_entity_undergoing_process_gets_processed() -> None:
    gateway = FakeLinkGateway(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        processes={Processes.PULL: create_identifiers("1")},
    )
    process(
        ProcessRequest(frozenset(create_identifiers("1"))),
        link_gateway=gateway,
        output_port=FakeOutputPort[OperationResponse](),
    )
    entity = next(entity for entity in gateway.create_link() if entity.identifier == create_identifier("1"))
    assert entity.state is states.Received


def test_correct_response_model_gets_passed_to_process_output_port() -> None:
    gateway = FakeLinkGateway(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        processes={Processes.PULL: create_identifiers("1")},
    )
    output_port = FakeOutputPort[OperationResponse]()
    process(
        ProcessRequest(frozenset(create_identifiers("1"))),
        link_gateway=gateway,
        output_port=output_port,
    )
    assert output_port.response.requested == create_identifiers("1")
    assert output_port.response.operation is Operations.PROCESS


def test_correct_response_model_gets_passed_to_list_idle_entities_output_port() -> None:
    link_gateway = FakeLinkGateway(
        create_assignments({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"2"}, Components.LOCAL: {"2"}})
    )
    output_port = FakeOutputPort[ListIdleEntitiesResponse]()
    list_idle_entities(ListIdleEntitiesRequest(), link_gateway=link_gateway, output_port=output_port)
    assert set(output_port.response.identifiers) == create_identifiers("1")
