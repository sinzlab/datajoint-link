from __future__ import annotations

from collections.abc import Iterable, Mapping
from functools import partial
from typing import Generic, TypedDict, TypeVar

import pytest

from dj_link.domain.custom_types import Identifier
from dj_link.domain.link import Link, create_link
from dj_link.domain.state import Commands, Components, Operations, Processes, State, Update, states
from dj_link.service.gateway import LinkGateway
from dj_link.service.io import ResponseRelay, create_returning_service
from dj_link.service.services import (
    DeleteRequest,
    DeleteResponse,
    ListIdleEntitiesRequest,
    ListIdleEntitiesResponse,
    OperationResponse,
    ProcessRequest,
    ProcessToCompletionResponse,
    PullRequest,
    PullResponse,
    Response,
    delete,
    list_idle_entities,
    process_to_completion,
    pull,
    start_delete_process,
    start_pull_process,
)
from dj_link.service.services import process as process_service
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
            for process, identifiers in processes.items():
                self.processes[process].update(identifiers)

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


class EntityConfig(TypedDict):
    state: type[State]
    is_tainted: bool
    process: Processes


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ({"state": states.Idle, "is_tainted": False, "process": None}, states.Idle),
        ({"state": states.Activated, "is_tainted": False, "process": Processes.PULL}, states.Idle),
        ({"state": states.Activated, "is_tainted": False, "process": Processes.DELETE}, states.Idle),
        ({"state": states.Activated, "is_tainted": True, "process": Processes.PULL}, states.Deprecated),
        ({"state": states.Activated, "is_tainted": True, "process": Processes.DELETE}, states.Deprecated),
        ({"state": states.Received, "is_tainted": False, "process": Processes.PULL}, states.Idle),
        ({"state": states.Received, "is_tainted": False, "process": Processes.DELETE}, states.Idle),
        ({"state": states.Received, "is_tainted": True, "process": Processes.PULL}, states.Deprecated),
        ({"state": states.Received, "is_tainted": True, "process": Processes.DELETE}, states.Deprecated),
        ({"state": states.Pulled, "is_tainted": False, "process": None}, states.Idle),
        ({"state": states.Tainted, "is_tainted": True, "process": None}, states.Deprecated),
        ({"state": states.Deprecated, "is_tainted": True, "process": None}, states.Deprecated),
    ],
)
def test_deleted_entity_ends_in_correct_state(state: EntityConfig, expected: type[State]) -> None:
    gateway = create_gateway(**state)
    process_relay: ResponseRelay[OperationResponse] = ResponseRelay()
    complete_process_relay: ResponseRelay[ProcessToCompletionResponse] = ResponseRelay()
    start_delete_process_relay: ResponseRelay[OperationResponse] = ResponseRelay()
    delete(
        DeleteRequest(frozenset(create_identifiers("1"))),
        process_to_completion_service=create_returning_service(
            partial(
                process_to_completion,
                process_service=create_returning_service(
                    partial(process_service, link_gateway=gateway, output_port=process_relay),
                    process_relay.get_response,
                ),
                output_port=complete_process_relay,
            ),
            complete_process_relay.get_response,
        ),
        start_delete_process_service=create_returning_service(
            partial(start_delete_process, link_gateway=gateway, output_port=start_delete_process_relay),
            start_delete_process_relay.get_response,
        ),
        output_port=FakeOutputPort[DeleteResponse](),
    )
    assert next(iter(gateway.create_link())).state is expected


@pytest.mark.xfail()
def test_correct_response_model_gets_passed_to_pull_output_port() -> None:
    gateway = FakeLinkGateway(create_assignments({Components.SOURCE: {"1"}}))
    output_port = FakeOutputPort[OperationResponse]()
    process_relay: ResponseRelay[OperationResponse] = ResponseRelay()
    process_to_completion_relay: ResponseRelay[ProcessToCompletionResponse] = ResponseRelay()
    start_pull_process_relay: ResponseRelay[OperationResponse] = ResponseRelay()
    pull(
        PullRequest(frozenset(create_identifiers("1"))),
        process_to_completion_service=create_returning_service(
            partial(
                process_to_completion,
                process_service=create_returning_service(
                    partial(process_service, link_gateway=gateway, output_port=process_relay),
                    process_relay.get_response,
                ),
                output_port=process_to_completion_relay,
            ),
            process_to_completion_relay.get_response,
        ),
        start_pull_process_service=create_returning_service(
            partial(start_pull_process, link_gateway=gateway, output_port=start_pull_process_relay),
            start_pull_process_relay.get_response,
        ),
        output_port=FakeOutputPort[PullResponse](),
    )
    assert output_port.response.requested == create_identifiers("1")
    assert output_port.response.operation is Operations.START_PULL


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ({"state": states.Idle, "is_tainted": False, "process": None}, states.Pulled),
        ({"state": states.Activated, "is_tainted": False, "process": Processes.PULL}, states.Pulled),
        ({"state": states.Activated, "is_tainted": False, "process": Processes.DELETE}, states.Pulled),
        ({"state": states.Activated, "is_tainted": True, "process": Processes.PULL}, states.Deprecated),
        ({"state": states.Activated, "is_tainted": True, "process": Processes.DELETE}, states.Deprecated),
        ({"state": states.Received, "is_tainted": False, "process": Processes.PULL}, states.Pulled),
        ({"state": states.Received, "is_tainted": False, "process": Processes.DELETE}, states.Pulled),
        ({"state": states.Received, "is_tainted": True, "process": Processes.PULL}, states.Tainted),
        ({"state": states.Received, "is_tainted": True, "process": Processes.DELETE}, states.Deprecated),
        ({"state": states.Pulled, "is_tainted": False, "process": None}, states.Pulled),
        ({"state": states.Tainted, "is_tainted": True, "process": None}, states.Tainted),
        ({"state": states.Deprecated, "is_tainted": True, "process": None}, states.Deprecated),
    ],
)
def test_pulled_entity_ends_in_correct_state(state: EntityConfig, expected: type[State]) -> None:
    gateway = create_gateway(**state)
    process_relay: ResponseRelay[OperationResponse] = ResponseRelay()
    process_to_completion_relay: ResponseRelay[ProcessToCompletionResponse] = ResponseRelay()
    start_pull_process_relay: ResponseRelay[OperationResponse] = ResponseRelay()
    pull(
        PullRequest(frozenset(create_identifiers("1"))),
        process_to_completion_service=create_returning_service(
            partial(
                process_to_completion,
                process_service=create_returning_service(
                    partial(process_service, link_gateway=gateway, output_port=process_relay),
                    process_relay.get_response,
                ),
                output_port=process_to_completion_relay,
            ),
            process_to_completion_relay.get_response,
        ),
        start_pull_process_service=create_returning_service(
            partial(start_pull_process, link_gateway=gateway, output_port=start_pull_process_relay),
            start_pull_process_relay.get_response,
        ),
        output_port=FakeOutputPort[PullResponse](),
    )
    assert next(iter(gateway.create_link())).state is expected


@pytest.mark.xfail()
def test_correct_response_model_gets_passed_to_delete_output_port() -> None:
    gateway = FakeLinkGateway(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    output_port = FakeOutputPort[OperationResponse]()
    process_relay: ResponseRelay[OperationResponse] = ResponseRelay()
    process_to_completion_relay: ResponseRelay[ProcessToCompletionResponse] = ResponseRelay()
    start_delete_process_relay: ResponseRelay[OperationResponse] = ResponseRelay()
    delete(
        DeleteRequest(frozenset(create_identifiers("1"))),
        process_to_completion_service=create_returning_service(
            partial(
                process_to_completion,
                process_service=create_returning_service(
                    partial(process_service, link_gateway=gateway, output_port=process_relay),
                    process_relay.get_response,
                ),
                output_port=process_to_completion_relay,
            ),
            process_to_completion_relay.get_response,
        ),
        start_delete_process_service=create_returning_service(
            partial(start_delete_process, link_gateway=gateway, output_port=start_delete_process_relay),
            start_delete_process_relay.get_response,
        ),
        output_port=FakeOutputPort[DeleteResponse](),
    )
    assert output_port.response.requested == create_identifiers("1")
    assert output_port.response.operation is Operations.START_DELETE


def test_entity_undergoing_process_gets_processed() -> None:
    gateway = FakeLinkGateway(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        processes={Processes.PULL: create_identifiers("1")},
    )
    process_service(
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
    process_service(
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
