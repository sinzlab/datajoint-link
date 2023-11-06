from __future__ import annotations

from typing import Iterable, Mapping

import pytest

from link.domain import events
from link.domain.state import Commands, Components, Operations, Transition, states
from link.service.uow import UnitOfWork
from tests.assignments import create_assignments, create_identifier, create_identifiers

from .gateway import FakeLinkGateway


def initialize(assignments: Mapping[Components, Iterable[str]]) -> tuple[FakeLinkGateway, UnitOfWork]:
    gateway = FakeLinkGateway(create_assignments(assignments))
    return gateway, UnitOfWork(gateway)


def test_updates_are_applied_to_gateway_on_commit() -> None:
    gateway, uow = initialize({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"2"}, Components.LOCAL: {"2"}})
    with uow:
        uow.link.pull(create_identifiers("1"))
        uow.link.delete(create_identifiers("2"))
        uow.commit()
    actual = {(entity.identifier, entity.state) for entity in gateway.create_link()}
    expected = {(create_identifier("1"), states.Pulled), (create_identifier("2"), states.Idle)}
    assert actual == expected


def test_updates_are_discarded_on_context_exit() -> None:
    gateway, uow = initialize({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"2"}, Components.LOCAL: {"2"}})
    with uow:
        uow.link.pull(create_identifiers("1"))
        uow.link.delete(create_identifiers("2"))
    actual = {(entity.identifier, entity.state) for entity in gateway.create_link()}
    expected = {(create_identifier("1"), states.Idle), (create_identifier("2"), states.Pulled)}
    assert actual == expected


def test_updates_are_discarded_on_rollback() -> None:
    gateway, uow = initialize({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"2"}, Components.LOCAL: {"2"}})
    with uow:
        uow.link.pull(create_identifiers("1"))
        uow.link.delete(create_identifiers("2"))
        uow.rollback()
    actual = {(entity.identifier, entity.state) for entity in gateway.create_link()}
    expected = {(create_identifier("1"), states.Idle), (create_identifier("2"), states.Pulled)}
    assert actual == expected


def test_link_can_not_be_accessed_outside_of_context() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        pass
    with pytest.raises(RuntimeError, match="outside"):
        uow.link


def test_unable_to_commit_outside_of_context() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with pytest.raises(RuntimeError, match="outside"):
        uow.commit()


def test_unable_to_rollback_outside_of_context() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with pytest.raises(RuntimeError, match="outside"):
        uow.rollback()


def test_entity_expires_when_committing() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        entity = next(entity for entity in uow.link if entity.identifier == create_identifier("1"))
        uow.commit()
        with pytest.raises(RuntimeError, match="expired entity"):
            entity.apply(Operations.START_PULL)


def test_entity_expires_when_rolling_back() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        entity = next(entity for entity in uow.link if entity.identifier == create_identifier("1"))
        uow.rollback()
        with pytest.raises(RuntimeError, match="expired entity"):
            entity.apply(Operations.START_PULL)


def test_entity_expires_when_leaving_context() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        entity = next(entity for entity in uow.link if entity.identifier == create_identifier("1"))
    with pytest.raises(RuntimeError, match="expired entity"):
        entity.apply(Operations.START_PULL)


def test_link_expires_when_committing() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        link = uow.link
        uow.commit()
        with pytest.raises(RuntimeError, match="expired entity"):
            link.pull(create_identifiers("1"))


def test_link_expires_when_rolling_back() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        link = uow.link
        uow.rollback()
        with pytest.raises(RuntimeError, match="expired entity"):
            link.pull(create_identifiers("1"))


def test_link_expires_when_exiting_context() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        link = uow.link
    with pytest.raises(RuntimeError, match="expired entity"):
        link.pull(create_identifiers("1"))


def test_correct_events_are_collected() -> None:
    _, uow = initialize({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"2"}, Components.LOCAL: {"2"}})
    with uow:
        uow.link.pull(create_identifiers("1"))
        uow.link.delete(create_identifiers("2"))
        uow.commit()
    expected = [
        events.EntityStateChanged(
            Operations.START_PULL,
            create_identifier("1"),
            Transition(states.Idle, states.Activated),
            Commands.START_PULL_PROCESS,
        ),
        events.EntityStateChanged(
            Operations.PROCESS,
            create_identifier("1"),
            Transition(states.Activated, states.Received),
            Commands.ADD_TO_LOCAL,
        ),
        events.EntityStateChanged(
            Operations.PROCESS,
            create_identifier("1"),
            Transition(states.Received, states.Pulled),
            Commands.FINISH_PULL_PROCESS,
        ),
        events.EntityStateChanged(
            Operations.START_DELETE,
            create_identifier("2"),
            Transition(states.Pulled, states.Received),
            Commands.START_DELETE_PROCESS,
        ),
        events.EntityStateChanged(
            Operations.PROCESS,
            create_identifier("2"),
            Transition(states.Received, states.Activated),
            Commands.REMOVE_FROM_LOCAL,
        ),
        events.EntityStateChanged(
            Operations.PROCESS,
            create_identifier("2"),
            Transition(states.Activated, states.Idle),
            Commands.FINISH_DELETE_PROCESS,
        ),
    ]
    actual = list(uow.collect_new_events())
    assert actual == expected


def test_unit_must_be_committed_to_collect_events() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        uow.link.pull(create_identifiers("1"))
    assert list(uow.collect_new_events()) == []


def test_events_can_only_be_collected_once() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        uow.link.pull(create_identifiers("1"))
        uow.commit()
    list(uow.collect_new_events())
    assert list(uow.collect_new_events()) == []


def test_events_can_only_be_collected_outside_of_context() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        uow.link.pull(create_identifiers("1"))
        uow.commit()
        with pytest.raises(RuntimeError, match="inside context"):
            list(uow.collect_new_events())
