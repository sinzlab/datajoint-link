from __future__ import annotations

from typing import TypedDict

import pytest

from link.domain import events
from link.domain.link import create_entity
from link.domain.state import Commands, Components, Operations, Processes, State, Transition, states
from tests.assignments import create_identifier


class EntityConfig(TypedDict):
    components: list[Components]
    is_tainted: bool
    process: Processes


@pytest.mark.parametrize(
    ("entity_config", "invalid_operations"),
    [
        (
            {"components": [Components.SOURCE], "is_tainted": False, "process": Processes.NONE},
            [Operations.START_DELETE, Operations.PROCESS],
        ),
        (
            {"components": [Components.SOURCE, Components.OUTBOUND], "is_tainted": False, "process": Processes.PULL},
            [Operations.START_PULL, Operations.START_DELETE],
        ),
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": False,
                "process": Processes.PULL,
            },
            [Operations.START_PULL, Operations.START_DELETE],
        ),
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": False,
                "process": Processes.NONE,
            },
            [Operations.START_PULL, Operations.PROCESS],
        ),
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": True,
                "process": Processes.NONE,
            },
            [Operations.START_PULL, Operations.PROCESS],
        ),
        (
            {"components": [Components.SOURCE, Components.OUTBOUND], "is_tainted": True, "process": Processes.NONE},
            [Operations.START_PULL, Operations.START_DELETE, Operations.PROCESS],
        ),
    ],
)
def test_invalid_transitions_do_not_change_entity(
    entity_config: EntityConfig, invalid_operations: list[Operations]
) -> None:
    entity = create_entity(create_identifier("1"), **entity_config)
    for operation in invalid_operations:
        expected = (
            entity.state,
            entity.current_process,
            events.InvalidOperationRequested(operation, entity.identifier, entity.state),
        )
        entity.apply(operation)
        assert (entity.state, entity.current_process, entity.events.pop()) == expected


def test_start_pulling_unshared_entity() -> None:
    entity = create_entity(
        create_identifier("1"), components=[Components.SOURCE], is_tainted=False, process=Processes.NONE
    )
    expected = (
        states.Activated,
        Processes.PULL,
        events.StateChanged(
            Operations.START_PULL,
            entity.identifier,
            Transition(states.Unshared, states.Activated),
            Commands.START_PULL_PROCESS,
        ),
    )
    entity.apply(Operations.START_PULL)
    assert (entity.state, entity.current_process, entity.events.pop()) == expected


@pytest.mark.parametrize(
    ("entity_config", "new_state", "new_process", "command"),
    [
        (
            {"components": [Components.SOURCE, Components.OUTBOUND], "is_tainted": False, "process": Processes.PULL},
            states.Received,
            Processes.PULL,
            Commands.ADD_TO_LOCAL,
        ),
        (
            {"components": [Components.SOURCE, Components.OUTBOUND], "is_tainted": True, "process": Processes.PULL},
            states.Deprecated,
            Processes.NONE,
            Commands.DEPRECATE,
        ),
        (
            {"components": [Components.SOURCE, Components.OUTBOUND], "is_tainted": False, "process": Processes.DELETE},
            states.Unshared,
            Processes.NONE,
            Commands.FINISH_DELETE_PROCESS,
        ),
        (
            {"components": [Components.SOURCE, Components.OUTBOUND], "is_tainted": True, "process": Processes.DELETE},
            states.Deprecated,
            Processes.NONE,
            Commands.DEPRECATE,
        ),
    ],
)
def test_processing_activated_entity(
    entity_config: EntityConfig, new_state: type[State], new_process: Processes, command: Commands
) -> None:
    entity = create_entity(create_identifier("1"), **entity_config)
    expected = (
        new_state,
        new_process,
        events.StateChanged(Operations.PROCESS, entity.identifier, Transition(entity.state, new_state), command),
    )
    entity.apply(Operations.PROCESS)
    assert (entity.state, entity.current_process, entity.events.pop()) == expected


@pytest.mark.parametrize(
    ("entity_config", "new_state", "new_process", "command"),
    [
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": False,
                "process": Processes.PULL,
            },
            states.Shared,
            Processes.NONE,
            Commands.FINISH_PULL_PROCESS,
        ),
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": True,
                "process": Processes.PULL,
            },
            states.Tainted,
            Processes.NONE,
            Commands.FINISH_PULL_PROCESS,
        ),
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": False,
                "process": Processes.DELETE,
            },
            states.Activated,
            Processes.DELETE,
            Commands.REMOVE_FROM_LOCAL,
        ),
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": True,
                "process": Processes.DELETE,
            },
            states.Activated,
            Processes.DELETE,
            Commands.REMOVE_FROM_LOCAL,
        ),
    ],
)
def test_processing_received_entity(
    entity_config: EntityConfig, new_state: type[State], new_process: Processes, command: Commands
) -> None:
    entity = create_entity(create_identifier("1"), **entity_config)
    expected = (
        new_state,
        new_process,
        events.StateChanged(Operations.PROCESS, entity.identifier, Transition(entity.state, new_state), command),
    )
    entity.apply(Operations.PROCESS)
    assert (entity.state, entity.current_process, entity.events.pop()) == expected


def test_starting_delete_on_shared_entity() -> None:
    entity = create_entity(
        create_identifier("1"),
        components=[Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
        is_tainted=False,
        process=Processes.NONE,
    )
    transition = Transition(states.Shared, states.Received)
    expected = (
        transition.new,
        Processes.DELETE,
        events.StateChanged(Operations.START_DELETE, entity.identifier, transition, Commands.START_DELETE_PROCESS),
    )
    entity.apply(Operations.START_DELETE)
    assert (entity.state, entity.current_process, entity.events.pop()) == expected


def test_starting_delete_on_tainted_entity() -> None:
    entity = create_entity(
        create_identifier("1"),
        components=[Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
        is_tainted=True,
        process=Processes.NONE,
    )
    transition = Transition(states.Tainted, states.Received)
    expected_events = [
        events.StateChanged(Operations.START_DELETE, entity.identifier, transition, Commands.START_DELETE_PROCESS),
    ]
    entity.apply(Operations.START_DELETE)
    assert entity.state == transition.new
    assert entity.current_process == Processes.DELETE
    assert list(entity.events) == expected_events
