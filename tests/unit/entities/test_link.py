from __future__ import annotations

from contextlib import nullcontext as does_not_raise
from typing import ContextManager, Iterable, Mapping, Optional

import pytest

from dj_link.entities import command
from dj_link.entities.link import Transfer, create_link, pull
from dj_link.entities.state import (
    Activated,
    Components,
    Deprecated,
    Identifier,
    Idle,
    Operations,
    Pulled,
    Received,
    State,
    Tainted,
)


def create_assignments(
    assignments: Optional[Mapping[Components, Iterable[str]]] = None
) -> dict[Components, set[Identifier]]:
    if assignments is None:
        assignments = {}
    else:
        assignments = dict(assignments)
    for component in Components:
        if component not in assignments:
            assignments[component] = set()
    return {
        component: {Identifier(identifier) for identifier in identifiers}
        for component, identifiers in assignments.items()
    }


class TestCreateLink:
    @staticmethod
    @pytest.mark.parametrize(
        "state,expected",
        [
            (Idle, {Identifier("1")}),
            (Activated, {Identifier("2")}),
            (Received, {Identifier("3")}),
            (Pulled, {Identifier("4")}),
            (Tainted, {Identifier("5")}),
            (Deprecated, {Identifier("6")}),
        ],
    )
    def test_entities_get_correct_state_assigned(
        state: type[State],
        expected: Iterable[Identifier],
    ) -> None:
        assignments = create_assignments(
            {
                Components.SOURCE: {"1", "2", "3", "4", "5", "6"},
                Components.OUTBOUND: {"2", "3", "4", "5"},
                Components.LOCAL: {"3", "4", "5"},
            }
        )
        link = create_link(
            assignments,
            tainted_identifiers={Identifier("5"), Identifier("6")},
            operations={Operations.PULL: {Identifier("2"), Identifier("3")}},
        )
        assert {entity.identifier for entity in link[Components.SOURCE] if entity.state is state} == set(expected)

    @staticmethod
    @pytest.mark.parametrize(
        "operations,expectation",
        [
            (
                {Operations.PULL: {Identifier("1")}, Operations.DELETE: {Identifier("1")}},
                pytest.raises(AssertionError),
            ),
            ({Operations.PULL: {Identifier("1")}}, does_not_raise()),
        ],
    )
    def test_identifiers_can_only_be_associated_with_single_operation(
        operations: Mapping[Operations, Iterable[Identifier]], expectation: ContextManager[None]
    ) -> None:
        with expectation:
            create_link(create_assignments(), operations=operations)

    @staticmethod
    def test_entities_get_correct_operation_assigned() -> None:
        link = create_link(
            create_assignments(
                {
                    Components.SOURCE: {"1", "2", "3", "4", "5"},
                    Components.OUTBOUND: {"1", "2", "3", "4"},
                    Components.LOCAL: {"3", "4"},
                }
            ),
            operations={
                Operations.PULL: {Identifier("1"), Identifier("3")},
                Operations.DELETE: {Identifier("2"), Identifier("4")},
            },
        )
        expected = {
            (Identifier("1"), Operations.PULL),
            (Identifier("2"), Operations.DELETE),
            (Identifier("3"), Operations.PULL),
            (Identifier("4"), Operations.DELETE),
            (Identifier("5"), None),
        }
        assert {(entity.identifier, entity.operation) for entity in link[Components.SOURCE]} == set(expected)

    @staticmethod
    @pytest.mark.parametrize(
        "assignments,expectation",
        [
            (create_assignments(), pytest.raises(AssertionError)),
            (
                create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
                does_not_raise(),
            ),
            (
                create_assignments(
                    {Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"1", "2"}, Components.LOCAL: {"1", "2"}}
                ),
                does_not_raise(),
            ),
        ],
    )
    def test_tainted_identifiers_can_not_be_superset_of_source_identifiers(
        assignments: Mapping[Components, Iterable[Identifier]], expectation: ContextManager[None]
    ) -> None:
        with expectation:
            create_link(assignments, tainted_identifiers={Identifier("1")})

    @staticmethod
    @pytest.mark.parametrize(
        "assignments,expectation",
        [
            (create_assignments({Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}), pytest.raises(AssertionError)),
            (create_assignments(), does_not_raise()),
            (create_assignments({Components.SOURCE: {"1"}}), does_not_raise()),
        ],
    )
    def test_outbound_identifiers_can_not_be_superset_of_source_identifiers(
        assignments: Mapping[Components, Iterable[Identifier]], expectation: ContextManager[None]
    ) -> None:
        with expectation:
            create_link(assignments)

    @staticmethod
    @pytest.mark.parametrize(
        "operations,assignments,expectation",
        [
            ({}, create_assignments({Components.LOCAL: {"1"}}), pytest.raises(AssertionError)),
            ({}, create_assignments(), does_not_raise()),
            (
                {Operations.PULL: {Identifier("1")}},
                create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
                does_not_raise(),
            ),
        ],
    )
    def test_local_identifiers_can_not_be_superset_of_outbound_identifiers(
        operations: Mapping[Operations, Iterable[Identifier]],
        assignments: Mapping[Components, Iterable[Identifier]],
        expectation: ContextManager[None],
    ) -> None:
        with expectation:
            create_link(assignments, operations=operations)


class TestLink:
    @staticmethod
    @pytest.fixture
    def assignments() -> dict[Components, set[Identifier]]:
        return create_assignments({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})

    @staticmethod
    def test_can_get_entities_in_component(assignments: Mapping[Components, Iterable[Identifier]]) -> None:
        link = create_link(assignments)
        assert {entity.identifier for entity in link[Components.SOURCE]} == {Identifier("1"), Identifier("2")}

    @staticmethod
    def test_can_get_identifiers_of_entities_in_component(
        assignments: Mapping[Components, Iterable[Identifier]]
    ) -> None:
        link = create_link(assignments)
        assert set(link[Components.SOURCE].identifiers) == {Identifier("1"), Identifier("2")}


class TestTransfer:
    @staticmethod
    @pytest.mark.parametrize(
        "origin,expectation",
        [
            (Components.SOURCE, does_not_raise()),
            (Components.OUTBOUND, pytest.raises(AssertionError)),
            (Components.LOCAL, pytest.raises(AssertionError)),
        ],
    )
    def test_origin_must_be_source(origin: Components, expectation: ContextManager[None]) -> None:
        with expectation:
            Transfer(Identifier("1"), origin, Components.LOCAL, identifier_only=False)

    @staticmethod
    @pytest.mark.parametrize(
        "destination,identifier_only,expectation",
        [
            (Components.SOURCE, False, pytest.raises(AssertionError)),
            (Components.OUTBOUND, True, does_not_raise()),
            (Components.LOCAL, False, does_not_raise()),
        ],
    )
    def test_destination_must_be_outbound_or_local(
        destination: Components, identifier_only: bool, expectation: ContextManager[None]
    ) -> None:
        with expectation:
            Transfer(Identifier("1"), Components.SOURCE, destination, identifier_only)

    @staticmethod
    @pytest.mark.parametrize(
        "destination,identifier_only,expectation",
        [
            (Components.OUTBOUND, False, pytest.raises(AssertionError)),
            (Components.LOCAL, False, does_not_raise()),
            (Components.OUTBOUND, True, does_not_raise()),
            (Components.LOCAL, True, pytest.raises(AssertionError)),
        ],
    )
    def test_identifier_only_must_be_true_only_if_destination_is_outbound(
        destination: Components, identifier_only: bool, expectation: ContextManager[None]
    ) -> None:
        with expectation:
            Transfer(Identifier("1"), Components.SOURCE, destination, identifier_only)


class TestPull:
    @staticmethod
    @pytest.mark.parametrize(
        "assignments,requested,expectation",
        [
            (
                create_assignments({Components.SOURCE: {"1"}}),
                {Identifier("1"), Identifier("2")},
                pytest.raises(AssertionError),
            ),
            (create_assignments({Components.SOURCE: {Identifier("1")}}), {Identifier("1")}, does_not_raise()),
            (create_assignments({Components.SOURCE: {"1", "2"}}), {Identifier("1")}, does_not_raise()),
        ],
    )
    def test_requested_identifiers_can_not_be_superset_of_source_identifiers(
        assignments: Mapping[Components, Iterable[Identifier]],
        requested: set[Identifier],
        expectation: ContextManager[None],
    ) -> None:
        link = create_link(assignments)
        with expectation:
            pull(link, requested=requested)

    @staticmethod
    @pytest.mark.parametrize(
        "assignments,requested,expectation",
        [
            (
                create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
                {Identifier("1")},
                pytest.raises(AssertionError),
            ),
            (create_assignments({Components.SOURCE: {"1"}}), {Identifier("1")}, does_not_raise()),
        ],
    )
    def test_can_not_pull_already_pulled_entities(
        assignments: Mapping[Components, Iterable[Identifier]],
        requested: Iterable[Identifier],
        expectation: ContextManager[None],
    ) -> None:
        link = create_link(assignments)
        with expectation:
            pull(link, requested=requested)

    @staticmethod
    def test_if_correct_transfer_specifications_are_returned() -> None:
        link = create_link(create_assignments({Components.SOURCE: {"1", "2"}}))
        expected = {
            Transfer(Identifier("1"), Components.SOURCE, Components.OUTBOUND, identifier_only=True),
            Transfer(Identifier("1"), Components.SOURCE, Components.LOCAL, identifier_only=False),
        }
        actual = pull(link, requested={Identifier("1")})
        assert actual == expected


def test_pulling_idle_entity_returns_correct_commands() -> None:
    link = create_link(create_assignments({Components.SOURCE: {"1"}}))
    entity = next(iter(link[Components.SOURCE]))
    assert entity.pull() == {command.AddToOutbound(Identifier("1")), command.StartPullOperation(Identifier("1"))}


def test_processing_activated_entity_undergoing_pull_operation_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        operations={Operations.PULL: {Identifier("1")}},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.process() == {command.AddToLocal(Identifier("1"))}


def test_processing_activated_entity_undergoing_delete_operation_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
        operations={Operations.DELETE: {Identifier("1")}},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.process() == {
        command.RemoveFromOutbound(Identifier("1")),
        command.FinishDeleteOperation(Identifier("1")),
    }


def test_processing_received_entity_undergoing_pull_operation_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        operations={Operations.PULL: {Identifier("1")}},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.process() == {command.FinishPullOperation(Identifier("1"))}


def test_processing_received_entity_undergoing_delete_operation_returns_correct_commands() -> None:
    link = create_link(
        create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}}),
        operations={Operations.DELETE: {Identifier("1")}},
    )
    entity = next(iter(link[Components.SOURCE]))
    assert entity.process() == {command.RemoveFromLocal(Identifier("1"))}


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
