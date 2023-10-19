from __future__ import annotations

from dataclasses import replace
from typing import Iterable

import pytest

from link.domain.custom_types import Identifier
from link.domain.link import create_link
from link.domain.state import Components, Operations, Processes, State, states
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
    assert all(entity.apply(operation) == entity for operation in operations)


def test_start_pulling_idle_entity_returns_correct_entity() -> None:
    link = create_link(create_assignments({Components.SOURCE: {"1"}}))
    entity = next(iter(link))
    assert entity.apply(Operations.START_PULL) == replace(
        entity, state=states.Activated, current_process=Processes.PULL
    )


@pytest.mark.parametrize(
    ("process", "tainted_identifiers", "new_state", "new_process"),
    [
        (Processes.PULL, set(), states.Received, Processes.PULL),
        (Processes.PULL, create_identifiers("1"), states.Deprecated, None),
        (Processes.DELETE, set(), states.Idle, None),
        (Processes.DELETE, create_identifiers("1"), states.Deprecated, None),
    ],
)
def test_processing_activated_entity_returns_correct_entity(
    process: Processes,
    tainted_identifiers: Iterable[Identifier],
    new_state: type[State],
    new_process: Processes | None,
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        processes={process: create_identifiers("1")},
        tainted_identifiers=tainted_identifiers,
    )
    entity = next(iter(link))
    assert entity.apply(Operations.PROCESS) == replace(entity, state=new_state, current_process=new_process)


@pytest.mark.parametrize(
    ("process", "tainted_identifiers", "new_state", "new_process"),
    [
        (Processes.PULL, set(), states.Pulled, None),
        (Processes.PULL, create_identifiers("1"), states.Tainted, None),
        (Processes.DELETE, set(), states.Activated, Processes.DELETE),
        (Processes.DELETE, create_identifiers("1"), states.Activated, Processes.DELETE),
    ],
)
def test_processing_received_entity_returns_correct_entity(
    process: Processes, tainted_identifiers: Iterable[Identifier], new_state: type[State], new_process: Processes | None
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        processes={process: {create_identifier("1")}},
        tainted_identifiers=tainted_identifiers,
    )
    entity = next(iter(link))
    assert entity.apply(Operations.PROCESS) == replace(entity, state=new_state, current_process=new_process)


def test_starting_delete_on_pulled_entity_returns_correct_entity() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    entity = next(iter(link))
    assert entity.apply(Operations.START_DELETE) == replace(
        entity, state=states.Received, current_process=Processes.DELETE
    )


def test_starting_delete_on_tainted_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        tainted_identifiers={create_identifier("1")},
    )
    entity = next(iter(link))
    assert entity.apply(Operations.START_DELETE) == replace(
        entity, state=states.Received, current_process=Processes.DELETE
    )
