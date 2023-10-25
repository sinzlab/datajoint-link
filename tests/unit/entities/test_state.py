from __future__ import annotations

from dataclasses import replace
from typing import Iterable

import pytest

from link.domain import events
from link.domain.custom_types import Identifier
from link.domain.link import create_link
from link.domain.state import (
    Commands,
    Components,
    Operations,
    Processes,
    State,
    Transition,
    states,
)
from tests.assignments import create_assignments, create_identifier, create_identifiers


@pytest.mark.parametrize(
    ("identifier", "state", "operations"),
    [
        (create_identifier("1"), states.Idle, [Operations.START_DELETE, Operations.PROCESS]),
        (create_identifier("2"), states.Activated, [Operations.START_PULL, Operations.START_DELETE]),
        (create_identifier("3"), states.Received, [Operations.START_PULL, Operations.START_DELETE]),
        (create_identifier("4"), states.Pulled, [Operations.START_PULL, Operations.PROCESS]),
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
        assert entity.apply(operation) == replace(entity, operation_results=(result,))


def test_start_pulling_idle_entity_returns_correct_entity() -> None:
    link = create_link(create_assignments({Components.SOURCE: {"1"}}))
    entity = next(iter(link))
    assert entity.apply(Operations.START_PULL) == replace(
        entity,
        state=states.Activated,
        current_process=Processes.PULL,
        operation_results=(
            events.EntityStateChanged(
                Operations.START_PULL,
                entity.identifier,
                Transition(states.Idle, states.Activated),
                Commands.START_PULL_PROCESS,
            ),
        ),
    )


@pytest.mark.parametrize(
    ("process", "tainted_identifiers", "new_state", "new_process", "command"),
    [
        (Processes.PULL, set(), states.Received, Processes.PULL, Commands.ADD_TO_LOCAL),
        (Processes.PULL, create_identifiers("1"), states.Deprecated, Processes.NONE, Commands.DEPRECATE),
        (Processes.DELETE, set(), states.Idle, Processes.NONE, Commands.FINISH_DELETE_PROCESS),
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
    updated_results = entity.operation_results + (
        events.EntityStateChanged(Operations.PROCESS, entity.identifier, Transition(entity.state, new_state), command),
    )
    assert entity.apply(Operations.PROCESS) == replace(
        entity, state=new_state, current_process=new_process, operation_results=updated_results
    )


@pytest.mark.parametrize(
    ("process", "tainted_identifiers", "new_state", "new_process", "command"),
    [
        (Processes.PULL, set(), states.Pulled, Processes.NONE, Commands.FINISH_PULL_PROCESS),
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
    operation_results = (
        events.EntityStateChanged(Operations.PROCESS, entity.identifier, Transition(entity.state, new_state), command),
    )
    assert entity.apply(Operations.PROCESS) == replace(
        entity, state=new_state, current_process=new_process, operation_results=operation_results
    )


def test_starting_delete_on_pulled_entity_returns_correct_entity() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    entity = next(iter(link))
    transition = Transition(states.Pulled, states.Received)
    operation_results = (
        events.EntityStateChanged(
            Operations.START_DELETE,
            entity.identifier,
            transition,
            Commands.START_DELETE_PROCESS,
        ),
    )
    assert entity.apply(Operations.START_DELETE) == replace(
        entity, state=transition.new, current_process=Processes.DELETE, operation_results=operation_results
    )


def test_starting_delete_on_tainted_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        tainted_identifiers={create_identifier("1")},
    )
    entity = next(iter(link))
    transition = Transition(states.Tainted, states.Received)
    operation_results = (
        events.EntityStateChanged(
            Operations.START_DELETE, entity.identifier, transition, Commands.START_DELETE_PROCESS
        ),
    )
    assert entity.apply(Operations.START_DELETE) == replace(
        entity, state=transition.new, current_process=Processes.DELETE, operation_results=operation_results
    )
