from __future__ import annotations

from typing import Iterable

import pytest

from dj_link.domain.custom_types import Identifier
from dj_link.domain.link import create_link
from dj_link.domain.state import (
    Commands,
    Components,
    InvalidOperation,
    Operations,
    Processes,
    State,
    Transition,
    Update,
    states,
)
from tests.assignments import create_assignments, create_identifier, create_identifiers


@pytest.mark.parametrize(
    ("identifier", "state", "methods"),
    [
        (create_identifier("1"), states.Idle, ["start_delete", "process"]),
        (create_identifier("2"), states.Activated, ["start_pull", "start_delete"]),
        (create_identifier("3"), states.Received, ["start_pull", "start_delete"]),
        (create_identifier("4"), states.Pulled, ["start_pull", "process"]),
        (create_identifier("5"), states.Tainted, ["start_pull", "process"]),
        (create_identifier("6"), states.Deprecated, ["start_pull", "start_delete", "process"]),
    ],
)
def test_invalid_transitions_produce_no_updates(identifier: Identifier, state: type[State], methods: str) -> None:
    link = create_link(
        create_assignments(
            {
                Components.SOURCE: {"1", "2", "3", "4", "5", "6"},
                Components.OUTBOUND: {"2", "3", "4", "5", "6"},
                Components.LOCAL: {"3", "4", "5"},
            }
        ),
        tainted_identifiers=create_identifiers("5", "6"),
        processes={Processes.PULL: create_identifiers("2", "3")},
    )
    entity = next(entity for entity in link if entity.identifier == identifier)
    method_operation_map = {
        "start_pull": Operations.START_PULL,
        "start_delete": Operations.START_DELETE,
        "process": Operations.PROCESS,
    }
    assert all(
        getattr(entity, method)() == InvalidOperation(method_operation_map[method], entity.identifier, state)
        for method in methods
    )


def test_start_pulling_idle_entity_returns_correct_commands() -> None:
    link = create_link(create_assignments({Components.SOURCE: {"1"}}))
    entity = next(iter(link))
    assert entity.start_pull() == Update(
        Operations.START_PULL,
        create_identifier("1"),
        Transition(states.Idle, states.Activated),
        command=Commands.START_PULL_PROCESS,
    )


@pytest.mark.parametrize(
    ("process", "tainted_identifiers", "new_state", "command"),
    [
        (Processes.PULL, set(), states.Received, Commands.ADD_TO_LOCAL),
        (Processes.PULL, create_identifiers("1"), states.Deprecated, Commands.DEPRECATE),
        (Processes.DELETE, set(), states.Idle, Commands.FINISH_DELETE_PROCESS),
        (Processes.DELETE, create_identifiers("1"), states.Deprecated, Commands.DEPRECATE),
    ],
)
def test_processing_activated_entity_returns_correct_commands(
    process: Processes,
    tainted_identifiers: Iterable[Identifier],
    new_state: type[State],
    command: Commands,
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        processes={process: create_identifiers("1")},
        tainted_identifiers=tainted_identifiers,
    )
    entity = next(iter(link))
    assert entity.process() == Update(
        Operations.PROCESS,
        create_identifier("1"),
        Transition(states.Activated, new_state),
        command=command,
    )


@pytest.mark.parametrize(
    ("process", "tainted_identifiers", "new_state", "command"),
    [
        (Processes.PULL, set(), states.Pulled, Commands.FINISH_PULL_PROCESS),
        (Processes.PULL, create_identifiers("1"), states.Tainted, Commands.FINISH_PULL_PROCESS),
        (Processes.DELETE, set(), states.Activated, Commands.REMOVE_FROM_LOCAL),
        (Processes.DELETE, create_identifiers("1"), states.Activated, Commands.REMOVE_FROM_LOCAL),
    ],
)
def test_processing_received_entity_returns_correct_commands(
    process: Processes, tainted_identifiers: Iterable[Identifier], new_state: type[State], command: Commands
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        processes={process: {create_identifier("1")}},
        tainted_identifiers=tainted_identifiers,
    )
    entity = next(iter(link))
    assert entity.process() == Update(
        Operations.PROCESS,
        create_identifier("1"),
        Transition(states.Received, new_state),
        command=command,
    )


def test_starting_delete_on_pulled_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    entity = next(iter(link))
    assert entity.start_delete() == Update(
        Operations.START_DELETE,
        create_identifier("1"),
        Transition(states.Pulled, states.Received),
        command=Commands.START_DELETE_PROCESS,
    )


def test_starting_delete_on_tainted_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        tainted_identifiers={create_identifier("1")},
    )
    entity = next(iter(link))
    assert entity.start_delete() == Update(
        Operations.START_DELETE,
        create_identifier("1"),
        Transition(states.Tainted, states.Received),
        command=Commands.START_DELETE_PROCESS,
    )
