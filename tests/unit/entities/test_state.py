from __future__ import annotations

from typing import Iterable

import pytest

from dj_link.entities import command
from dj_link.entities.custom_types import Identifier
from dj_link.entities.link import create_link
from dj_link.entities.state import (
    Activated,
    Components,
    Deprecated,
    Idle,
    Operations,
    Pulled,
    Received,
    State,
    Tainted,
    Transition,
    Update,
)

from .assignments import create_assignments


def test_pulling_idle_entity_returns_correct_commands() -> None:
    link = create_link(create_assignments({Components.SOURCE: {"1"}}))
    entity = next(iter(link[Components.SOURCE]))
    assert entity.pull() == Update(
        Identifier("1"),
        Transition(Idle, Activated),
        commands=frozenset({command.AddToOutbound(Identifier("1")), command.StartPullOperation(Identifier("1"))}),
    )


@pytest.mark.parametrize(
    "operation,tainted_identifiers,new_state,commands",
    [
        (Operations.PULL, set(), Received, {command.AddToLocal}),
        (Operations.DELETE, set(), Idle, {command.RemoveFromOutbound, command.FinishDeleteOperation}),
        (Operations.DELETE, {Identifier("1")}, Deprecated, {command.RemoveFromOutbound, command.FinishDeleteOperation}),
    ],
)
def test_processing_activated_entity_returns_correct_commands(
    operation: Operations,
    tainted_identifiers: Iterable[Identifier],
    new_state: type[State],
    commands: Iterable[type[command.Command]],
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        operations={operation: {Identifier("1")}},
        tainted_identifiers=tainted_identifiers,
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.process() == Update(
        Identifier("1"),
        Transition(Activated, new_state),
        commands=frozenset({command(Identifier("1")) for command in commands}),
    )


@pytest.mark.parametrize(
    "operation,new_state,commands",
    [
        (Operations.PULL, Pulled, {command.FinishPullOperation}),
        (Operations.DELETE, Activated, {command.RemoveFromLocal}),
    ],
)
def test_processing_received_entity_returns_correct_commands(
    operation: Operations, new_state: type[State], commands: Iterable[type[command.Command]]
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        operations={operation: {Identifier("1")}},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.process() == Update(
        Identifier("1"),
        Transition(Received, new_state),
        commands=frozenset({command(Identifier("1")) for command in commands}),
    )


def test_deleting_pulled_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.delete() == Update(
        Identifier("1"),
        Transition(Pulled, Received),
        commands=frozenset({command.StartDeleteOperation(Identifier("1"))}),
    )


def test_flagging_pulled_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.flag() == Update(
        Identifier("1"), Transition(Pulled, Tainted), commands=frozenset({command.Flag(Identifier("1"))})
    )


def test_unflagging_tainted_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        tainted_identifiers={Identifier("1")},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.unflag() == Update(
        Identifier("1"), Transition(Tainted, Pulled), commands=frozenset({command.Unflag(Identifier("1"))})
    )


def test_deleting_tainted_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        tainted_identifiers={Identifier("1")},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.delete() == Update(
        Identifier("1"),
        Transition(Tainted, Received),
        commands=frozenset({command.StartDeleteOperation(Identifier("1"))}),
    )


def test_unflagging_deprecated_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}}),
        tainted_identifiers={Identifier("1")},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.unflag() == Update(
        Identifier("1"), Transition(Deprecated, Idle), commands=frozenset({command.Unflag(Identifier("1"))})
    )
