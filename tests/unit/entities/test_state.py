from __future__ import annotations

from typing import Iterable

import pytest

from dj_link.entities.custom_types import Identifier
from dj_link.entities.link import create_link
from dj_link.entities.state import Commands, Components, Processes, State, Transition, Update, states

from .assignments import create_assignments, create_identifier, create_identifiers


@pytest.mark.parametrize(
    ("identifier", "state", "methods"),
    [
        (create_identifier("1"), states.Idle, ["delete", "process", "flag", "unflag"]),
        (create_identifier("2"), states.Activated, ["pull", "delete", "flag", "unflag"]),
        (create_identifier("3"), states.Received, ["pull", "delete", "flag", "unflag"]),
        (create_identifier("4"), states.Pulled, ["pull", "process", "unflag"]),
        (create_identifier("5"), states.Tainted, ["pull", "process", "flag"]),
        (create_identifier("6"), states.Deprecated, ["pull", "delete", "process", "flag"]),
    ],
)
def test_invalid_transitions_produce_empty_updates(identifier: Identifier, state: type[State], methods: str) -> None:
    link = create_link(
        create_assignments(
            {
                Components.SOURCE: {"1", "2", "3", "4", "5", "6"},
                Components.OUTBOUND: {"2", "3", "4", "5"},
                Components.LOCAL: {"3", "4", "5"},
            }
        ),
        tainted_identifiers=create_identifiers("5", "6"),
        processes={Processes.PULL: create_identifiers("2", "3")},
    )
    entity = next(entity for entity in link[Components.SOURCE] if entity.identifier == identifier)
    assert all(not getattr(entity, method)() for method in methods)


def test_pulling_idle_entity_returns_correct_commands() -> None:
    link = create_link(create_assignments({Components.SOURCE: {"1"}}))
    entity = next(iter(link[Components.SOURCE]))
    assert entity.pull() == Update(
        create_identifier("1"),
        Transition(states.Idle, states.Activated),
        commands=frozenset({Commands.START_PULL_PROCESS}),
    )


@pytest.mark.parametrize(
    ("process", "tainted_identifiers", "new_state", "commands"),
    [
        (Processes.PULL, set(), states.Received, {Commands.ADD_TO_LOCAL}),
        (Processes.DELETE, set(), states.Idle, {Commands.FINISH_DELETE_PROCESS}),
        (Processes.DELETE, {create_identifier("1")}, states.Deprecated, {Commands.FINISH_DELETE_PROCESS}),
    ],
)
def test_processing_activated_entity_returns_correct_commands(
    process: Processes,
    tainted_identifiers: Iterable[Identifier],
    new_state: type[State],
    commands: Iterable[Commands],
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        processes={process: create_identifiers("1")},
        tainted_identifiers=tainted_identifiers,
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.process() == Update(
        create_identifier("1"),
        Transition(states.Activated, new_state),
        commands=frozenset(commands),
    )


@pytest.mark.parametrize(
    ("process", "new_state", "commands"),
    [
        (Processes.PULL, states.Pulled, {Commands.FINISH_PULL_PROCESS}),
        (Processes.DELETE, states.Activated, {Commands.REMOVE_FROM_LOCAL}),
    ],
)
def test_processing_received_entity_returns_correct_commands(
    process: Processes, new_state: type[State], commands: Iterable[Commands]
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        processes={process: {create_identifier("1")}},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.process() == Update(
        create_identifier("1"),
        Transition(states.Received, new_state),
        commands=frozenset(commands),
    )


def test_deleting_pulled_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.delete() == Update(
        create_identifier("1"),
        Transition(states.Pulled, states.Received),
        commands=frozenset({Commands.START_DELETE_PROCESS}),
    )


def test_flagging_pulled_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.flag() == Update(
        create_identifier("1"), Transition(states.Pulled, states.Tainted), commands=frozenset({Commands.FLAG})
    )


def test_unflagging_tainted_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        tainted_identifiers={create_identifier("1")},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.unflag() == Update(
        create_identifier("1"), Transition(states.Tainted, states.Pulled), commands=frozenset({Commands.UNFLAG})
    )


def test_deleting_tainted_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        tainted_identifiers={create_identifier("1")},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.delete() == Update(
        create_identifier("1"),
        Transition(states.Tainted, states.Received),
        commands=frozenset({Commands.START_DELETE_PROCESS}),
    )


def test_unflagging_deprecated_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}}),
        tainted_identifiers={create_identifier("1")},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.unflag() == Update(
        create_identifier("1"), Transition(states.Deprecated, states.Idle), commands=frozenset({Commands.UNFLAG})
    )
