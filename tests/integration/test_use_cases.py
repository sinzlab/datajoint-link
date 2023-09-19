from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Generic, TypedDict, TypeVar

import pytest

from dj_link.domain.custom_types import Identifier
from dj_link.domain.link import Link, create_link
from dj_link.domain.state import Commands, Components, Operations, Processes, State, Update, states
from dj_link.service.gateway import LinkGateway
from dj_link.service.use_cases import (
    DeleteRequestModel,
    ListIdleEntitiesRequestModel,
    ListIdleEntitiesResponseModel,
    OperationResponse,
    ProcessRequestModel,
    PullRequestModel,
    ResponseModel,
    delete,
    list_idle_entities,
    pull,
)
from dj_link.service.use_cases import process as process_use_case
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


T = TypeVar("T", bound=ResponseModel)


class FakeOutputPort(Generic[T]):
    def __init__(self) -> None:
        self._response: T | None = None

    @property
    def response(self) -> T:
        assert self._response is not None
        return self._response

    def __call__(self, response: T) -> None:
        self._response = response


def test_idle_entity_gets_pulled() -> None:
    gateway = FakeLinkGateway(create_assignments({Components.SOURCE: {"1"}}))
    pull(
        PullRequestModel(frozenset(create_identifiers("1"))),
        link_gateway=gateway,
        output_port=FakeOutputPort[OperationResponse](),
    )
    assert next(iter(gateway.create_link())).state is states.Pulled


def test_untainted_processing_entities_get_pulled() -> None:
    gateway = FakeLinkGateway(
        create_assignments(
            {
                Components.SOURCE: {"1", "2", "3", "4"},
                Components.OUTBOUND: {"1", "2", "3", "4"},
                Components.LOCAL: {"3", "4"},
            }
        ),
        processes={Processes.PULL: create_identifiers("1", "3"), Processes.DELETE: create_identifiers("2", "4")},
    )
    pull(
        PullRequestModel(frozenset(create_identifiers("1", "2", "3", "4"))),
        link_gateway=gateway,
        output_port=FakeOutputPort[OperationResponse](),
    )
    assert all(entity.state is states.Pulled for entity in gateway.create_link())


def test_tainted_processing_entities_get_processed_but_not_pulled() -> None:
    gateway = FakeLinkGateway(
        create_assignments(
            {
                Components.SOURCE: {"1", "2", "3", "4"},
                Components.OUTBOUND: {"1", "2", "3", "4"},
                Components.LOCAL: {"3", "4"},
            }
        ),
        processes={Processes.PULL: create_identifiers("1", "3"), Processes.DELETE: create_identifiers("2", "4")},
        tainted_identifiers=create_identifiers("1", "2", "3", "4"),
    )
    pull(
        PullRequestModel(frozenset(create_identifiers("1", "2", "3", "4"))),
        link_gateway=gateway,
        output_port=FakeOutputPort[OperationResponse](),
    )
    assert {entity.identifier: entity.state for entity in gateway.create_link()} == {
        create_identifier("1"): states.Deprecated,
        create_identifier("2"): states.Deprecated,
        create_identifier("3"): states.Tainted,
        create_identifier("4"): states.Deprecated,
    }


@pytest.mark.xfail()
def test_correct_response_model_gets_passed_to_pull_output_port() -> None:
    gateway = FakeLinkGateway(create_assignments({Components.SOURCE: {"1"}}))
    output_port = FakeOutputPort[OperationResponse]()
    pull(
        PullRequestModel(frozenset(create_identifiers("1"))),
        link_gateway=gateway,
        output_port=output_port,
    )
    assert output_port.response.requested == create_identifiers("1")
    assert output_port.response.operation is Operations.PULL


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
    delete(
        DeleteRequestModel(frozenset(create_identifiers("1"))),
        link_gateway=gateway,
        output_port=FakeOutputPort[OperationResponse](),
    )
    assert next(iter(gateway.create_link())).state is expected


@pytest.mark.xfail()
def test_correct_response_model_gets_passed_to_delete_output_port() -> None:
    gateway = FakeLinkGateway(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    output_port = FakeOutputPort[OperationResponse]()
    delete(
        DeleteRequestModel(frozenset(create_identifiers("1"))),
        link_gateway=gateway,
        output_port=output_port,
    )
    assert output_port.response.requested == create_identifiers("1")
    assert output_port.response.operation is Operations.DELETE


def test_entity_undergoing_process_gets_processed() -> None:
    gateway = FakeLinkGateway(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        processes={Processes.PULL: create_identifiers("1")},
    )
    process_use_case(
        ProcessRequestModel(frozenset(create_identifiers("1"))),
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
    process_use_case(
        ProcessRequestModel(frozenset(create_identifiers("1"))),
        link_gateway=gateway,
        output_port=output_port,
    )
    assert output_port.response.requested == create_identifiers("1")
    assert output_port.response.operation is Operations.PROCESS


def test_correct_response_model_gets_passed_to_list_idle_entities_output_port() -> None:
    link_gateway = FakeLinkGateway(
        create_assignments({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"2"}, Components.LOCAL: {"2"}})
    )
    output_port = FakeOutputPort[ListIdleEntitiesResponseModel]()
    list_idle_entities(ListIdleEntitiesRequestModel(), link_gateway=link_gateway, output_port=output_port)
    assert set(output_port.response.identifiers) == create_identifiers("1")
