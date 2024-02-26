from __future__ import annotations

from typing import Iterable

import pytest

from link.domain import events
from link.domain.custom_types import Identifier
from link.domain.link import create_link
from link.domain.state import Commands, Components, Operations, Processes, State, Transition, states
from tests.assignments import create_assignments, create_identifier, create_identifiers


@pytest.mark.parametrize(
    ("identifier", "state", "operations"),
    [
        (create_identifier("1"), states.Unshared, [Operations.START_DELETE, Operations.PROCESS]),
        (create_identifier("2"), states.Activated, [Operations.START_PULL, Operations.START_DELETE]),
        (create_identifier("3"), states.Received, [Operations.START_PULL, Operations.START_DELETE]),
        (create_identifier("4"), states.Shared, [Operations.START_PULL, Operations.PROCESS]),
        (create_identifier("5"), states.Tainted, [Operations.START_PULL, Operations.PROCESS]),
        (
            create_identifier("6"),
            states.Deprecated,
            [Operations.START_PULL, Operations.START_DELETE, Operations.PROCESS],
        ),
    ],
)
def test_invalid_transitions_returns_unchanged_entity(
    identifier: Identifier, state: type[State], operations: list[Operations]
) -> None:
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
    for operation in operations:
        result = events.InvalidOperationRequested(operation, identifier, state)
        entity.apply(operation)
        assert entity.events.pop() == result


def test_start_pulling_unshared_entity_returns_correct_entity() -> None:
    link = create_link(create_assignments({Components.SOURCE: {"1"}}))
    entity = next(iter(link))
    entity.apply(Operations.START_PULL)
    assert entity.state is states.Activated
    assert entity.current_process is Processes.PULL
    assert list(entity.events) == [
        events.StateChanged(
            Operations.START_PULL,
            entity.identifier,
            Transition(states.Unshared, states.Activated),
            Commands.START_PULL_PROCESS,
        )
    ]


@pytest.mark.parametrize(
    ("process", "tainted_identifiers", "new_state", "new_process", "command"),
    [
        (Processes.PULL, set(), states.Received, Processes.PULL, Commands.ADD_TO_LOCAL),
        (Processes.PULL, create_identifiers("1"), states.Deprecated, Processes.NONE, Commands.DEPRECATE),
        (Processes.DELETE, set(), states.Unshared, Processes.NONE, Commands.FINISH_DELETE_PROCESS),
        (Processes.DELETE, create_identifiers("1"), states.Deprecated, Processes.NONE, Commands.DEPRECATE),
    ],
)
def test_processing_activated_entity_returns_correct_entity(
    process: Processes,
    tainted_identifiers: Iterable[Identifier],
    new_state: type[State],
    new_process: Processes,
    command: Commands,
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        processes={process: create_identifiers("1")},
        tainted_identifiers=tainted_identifiers,
    )
    entity = next(iter(link))
    entity.events.append(
        events.StateChanged(Operations.PROCESS, entity.identifier, Transition(entity.state, new_state), command),
    )
    entity.apply(Operations.PROCESS)
    assert entity.state == new_state
    assert entity.current_process == new_process


@pytest.mark.parametrize(
    ("process", "tainted_identifiers", "new_state", "new_process", "command"),
    [
        (Processes.PULL, set(), states.Shared, Processes.NONE, Commands.FINISH_PULL_PROCESS),
        (Processes.PULL, create_identifiers("1"), states.Tainted, Processes.NONE, Commands.FINISH_PULL_PROCESS),
        (Processes.DELETE, set(), states.Activated, Processes.DELETE, Commands.REMOVE_FROM_LOCAL),
        (Processes.DELETE, create_identifiers("1"), states.Activated, Processes.DELETE, Commands.REMOVE_FROM_LOCAL),
    ],
)
def test_processing_received_entity_returns_correct_entity(
    process: Processes,
    tainted_identifiers: Iterable[Identifier],
    new_state: type[State],
    new_process: Processes,
    command: Commands,
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        processes={process: {create_identifier("1")}},
        tainted_identifiers=tainted_identifiers,
    )
    entity = next(iter(link))
    expected_events = [
        events.StateChanged(Operations.PROCESS, entity.identifier, Transition(entity.state, new_state), command),
    ]
    entity.apply(Operations.PROCESS)
    assert entity.state == new_state
    assert entity.current_process == new_process
    assert list(entity.events) == expected_events


def test_starting_delete_on_shared_entity_returns_correct_entity() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    entity = next(iter(link))
    transition = Transition(states.Shared, states.Received)
    expected_events = [
        events.StateChanged(
            Operations.START_DELETE,
            entity.identifier,
            transition,
            Commands.START_DELETE_PROCESS,
        ),
    ]
    entity.apply(Operations.START_DELETE)
    assert entity.state == transition.new
    assert entity.current_process == Processes.DELETE
    assert list(entity.events) == expected_events


def test_starting_delete_on_tainted_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        tainted_identifiers={create_identifier("1")},
    )
    entity = next(iter(link))
    transition = Transition(states.Tainted, states.Received)
    expected_events = [
        events.StateChanged(Operations.START_DELETE, entity.identifier, transition, Commands.START_DELETE_PROCESS),
    ]
    entity.apply(Operations.START_DELETE)
    assert entity.state == transition.new
    assert entity.current_process == Processes.DELETE
    assert list(entity.events) == expected_events
