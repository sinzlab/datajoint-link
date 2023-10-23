from __future__ import annotations

from typing import Iterable, Mapping

import pytest

from link.domain.state import Components, Operations, states
from link.service.uow import UnitOfWork
from tests.assignments import create_assignments, create_identifier, create_identifiers

from .gateway import FakeLinkGateway


def initialize(assignments: Mapping[Components, Iterable[str]]) -> tuple[FakeLinkGateway, UnitOfWork]:
    gateway = FakeLinkGateway(create_assignments(assignments))
    return gateway, UnitOfWork(gateway)


def test_updates_are_applied_to_gateway_on_commit() -> None:
    gateway, uow = initialize({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"2"}, Components.LOCAL: {"2"}})
    with uow:
        link = uow.link.apply(Operations.START_PULL, requested=create_identifiers("1"))
        link = link.apply(Operations.START_DELETE, requested=create_identifiers("2"))
        link = link.apply(Operations.PROCESS, requested=create_identifiers("1", "2"))
        link.apply(Operations.PROCESS, requested=create_identifiers("1", "2"))
        uow.commit()
    actual = {(entity.identifier, entity.state) for entity in gateway.create_link()}
    expected = {(create_identifier("1"), states.Pulled), (create_identifier("2"), states.Idle)}
    assert actual == expected


def test_updates_are_discarded_on_context_exit() -> None:
    gateway, uow = initialize({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"2"}, Components.LOCAL: {"2"}})
    with uow:
        link = uow.link.apply(Operations.START_PULL, requested=create_identifiers("1"))
        link = link.apply(Operations.START_DELETE, requested=create_identifiers("2"))
        link = link.apply(Operations.PROCESS, requested=create_identifiers("1", "2"))
        link.apply(Operations.PROCESS, requested=create_identifiers("1", "2"))
    actual = {(entity.identifier, entity.state) for entity in gateway.create_link()}
    expected = {(create_identifier("1"), states.Idle), (create_identifier("2"), states.Pulled)}
    assert actual == expected


def test_updates_are_discarded_on_rollback() -> None:
    gateway, uow = initialize({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"2"}, Components.LOCAL: {"2"}})
    with uow:
        link = uow.link.apply(Operations.START_PULL, requested=create_identifiers("1"))
        link = link.apply(Operations.START_DELETE, requested=create_identifiers("2"))
        link = link.apply(Operations.PROCESS, requested=create_identifiers("1", "2"))
        link.apply(Operations.PROCESS, requested=create_identifiers("1", "2"))
        uow.rollback()
    actual = {(entity.identifier, entity.state) for entity in gateway.create_link()}
    expected = {(create_identifier("1"), states.Idle), (create_identifier("2"), states.Pulled)}
    assert actual == expected


def test_link_can_not_be_accessed_outside_of_context() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with pytest.raises(RuntimeError, match="outside"):
        uow.link.apply(Operations.START_PULL, requested=create_identifiers("1"))


def test_no_more_operations_can_be_applied_after_commit() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        link = uow.link.apply(Operations.START_PULL, requested=create_identifiers("1"))
        uow.commit()
        with pytest.raises(RuntimeError, match="expired"):
            link.apply(Operations.PROCESS, requested=create_identifiers("1"))


def test_no_more_operations_can_be_applied_after_rollback() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        link = uow.link.apply(Operations.START_PULL, requested=create_identifiers("1"))
        uow.rollback()
        with pytest.raises(RuntimeError, match="expired"):
            link.apply(Operations.START_PULL, requested=create_identifiers("1"))


def test_no_more_operations_can_be_applied_after_exiting_context() -> None:
    _, uow = initialize({Components.SOURCE: {"1"}})
    with uow:
        link = uow.link.apply(Operations.START_PULL, requested=create_identifiers("1"))
    with pytest.raises(RuntimeError, match="expired"):
        link.apply(Operations.START_PULL, requested=create_identifiers("1"))
