from __future__ import annotations

from typing import Iterable

import pytest

from dj_link.entities import command
from dj_link.entities.link import create_link
from dj_link.entities.state import Components, Identifier, Operations

from .assignments import create_assignments


def test_pulling_idle_entity_returns_correct_commands() -> None:
    link = create_link(create_assignments({Components.SOURCE: {"1"}}))
    entity = next(iter(link[Components.SOURCE]))
    assert entity.pull() == {command.AddToOutbound(Identifier("1")), command.StartPullOperation(Identifier("1"))}


@pytest.mark.parametrize(
    "operation,commands",
    [
        (Operations.PULL, {command.AddToLocal}),
        (Operations.DELETE, {command.RemoveFromOutbound, command.FinishDeleteOperation}),
    ],
)
def test_processing_activated_entity_returns_correct_commands(
    operation: Operations, commands: Iterable[type[command.Command]]
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        operations={operation: {Identifier("1")}},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.process() == {command(Identifier("1")) for command in commands}


@pytest.mark.parametrize(
    "operation,commands",
    [
        (Operations.PULL, {command.FinishPullOperation}),
        (Operations.DELETE, {command.RemoveFromLocal}),
    ],
)
def test_processing_received_entity_returns_correct_commands(
    operation: Operations, commands: Iterable[type[command.Command]]
) -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        operations={operation: {Identifier("1")}},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.process() == {command(Identifier("1")) for command in commands}


def test_deleting_pulled_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.delete() == {command.StartDeleteOperation(Identifier("1"))}


def test_flagging_pulled_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.flag() == {command.Flag(Identifier("1"))}


def test_unflagging_tainted_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        tainted_identifiers={Identifier("1")},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.unflag() == {command.Unflag(Identifier("1"))}


def test_deleting_tainted_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        tainted_identifiers={Identifier("1")},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.delete() == {command.StartDeleteOperation(Identifier("1"))}


def test_unflagging_deprecated_entity_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}}),
        tainted_identifiers={Identifier("1")},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.unflag() == {command.Unflag(Identifier("1"))}
