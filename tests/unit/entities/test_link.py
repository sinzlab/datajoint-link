from __future__ import annotations

from contextlib import nullcontext as does_not_raise
from typing import ContextManager, Iterable, Mapping

import pytest

from dj_link.entities.link import Component, Components, Entity, Identifier, States, Transfer, create_link, pull


class TestCreateLink:
    @staticmethod
    @pytest.fixture
    def assignments() -> dict[Components, set[Identifier]]:
        return {
            Components.SOURCE: {Identifier("1"), Identifier("2")},
            Components.OUTBOUND: {Identifier("1")},
            Components.LOCAL: {Identifier("1")},
        }

    @staticmethod
    @pytest.mark.parametrize(
        "state,expected",
        [
            (States.IDLE, {Identifier("2")}),
            (States.PULLED, {Identifier("1")}),
        ],
    )
    def test_entities_get_correct_state_assigned(
        assignments: Mapping[Components, Iterable[Identifier]], state: States, expected: Iterable[Identifier]
    ) -> None:
        link = create_link(assignments)
        assert {entity.identifier for entity in link[Components.SOURCE] if entity.state is state} == set(expected)

    @staticmethod
    @pytest.mark.parametrize(
        "assignments,expectation",
        [
            (
                {Components.SOURCE: set(), Components.OUTBOUND: {Identifier("1")}, Components.LOCAL: {Identifier("1")}},
                pytest.raises(AssertionError),
            ),
            ({Components.SOURCE: set(), Components.OUTBOUND: set(), Components.LOCAL: set()}, does_not_raise()),
            (
                {Components.SOURCE: {Identifier("1")}, Components.OUTBOUND: set(), Components.LOCAL: set()},
                does_not_raise(),
            ),
        ],
    )
    def test_outbound_identifiers_can_not_be_superset_of_source_identifiers(
        assignments: Mapping[Components, Iterable[Identifier]], expectation: ContextManager[None]
    ) -> None:
        with expectation:
            create_link(assignments)

    @staticmethod
    @pytest.mark.parametrize(
        "assignments,expectation",
        [
            (
                {
                    Components.SOURCE: {Identifier("1")},
                    Components.OUTBOUND: {Identifier("1")},
                    Components.LOCAL: {Identifier("1")},
                },
                does_not_raise(),
            ),
            (
                {Components.SOURCE: {Identifier("1")}, Components.OUTBOUND: {Identifier("1")}, Components.LOCAL: set()},
                pytest.raises(AssertionError),
            ),
            (
                {Components.SOURCE: {Identifier("1")}, Components.OUTBOUND: set(), Components.LOCAL: {Identifier("1")}},
                pytest.raises(AssertionError),
            ),
        ],
    )
    def test_local_identifiers_must_be_identical_to_outbound_identifiers(
        assignments: Mapping[Components, Iterable[Identifier]], expectation: ContextManager[None]
    ) -> None:
        with expectation:
            create_link(assignments)


def test_can_get_entities_in_component() -> None:
    assignments = {
        Components.SOURCE: {Identifier("1"), Identifier("2")},
        Components.OUTBOUND: {Identifier("1")},
        Components.LOCAL: {Identifier("1")},
    }
    link = create_link(assignments)
    assert link[Components.SOURCE] == Component(
        {Entity(Identifier("1"), state=States.PULLED), Entity(Identifier("2"), state=States.IDLE)}
    )


def test_can_get_identifiers_of_entities_in_component() -> None:
    assignments = {
        Components.SOURCE: {Identifier("1"), Identifier("2")},
        Components.OUTBOUND: {Identifier("1")},
        Components.LOCAL: {Identifier("1")},
    }
    link = create_link(assignments)
    assert link[Components.SOURCE].identifiers == frozenset({Identifier("1"), Identifier("2")})


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
                {Components.SOURCE: {Identifier("1")}, Components.OUTBOUND: set(), Components.LOCAL: set()},
                {Identifier("1"), Identifier("2")},
                pytest.raises(AssertionError),
            ),
            (
                {Components.SOURCE: {Identifier("1")}, Components.OUTBOUND: set(), Components.LOCAL: set()},
                {Identifier("1")},
                does_not_raise(),
            ),
            (
                {
                    Components.SOURCE: {Identifier("1"), Identifier("2")},
                    Components.OUTBOUND: set(),
                    Components.LOCAL: set(),
                },
                {Identifier("1")},
                does_not_raise(),
            ),
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
                {
                    Components.SOURCE: {Identifier("1")},
                    Components.OUTBOUND: {Identifier("1")},
                    Components.LOCAL: {Identifier("1")},
                },
                {Identifier("1")},
                pytest.raises(AssertionError),
            ),
            (
                {Components.SOURCE: {Identifier("1")}, Components.OUTBOUND: set(), Components.LOCAL: set()},
                {Identifier("1")},
                does_not_raise(),
            ),
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
        link = create_link(
            {Components.SOURCE: {Identifier("1"), Identifier("2")}, Components.OUTBOUND: set(), Components.LOCAL: set()}
        )
        expected = {
            Transfer(Identifier("1"), Components.SOURCE, Components.OUTBOUND, identifier_only=True),
            Transfer(Identifier("1"), Components.SOURCE, Components.LOCAL, identifier_only=False),
        }
        actual = pull(link, requested={Identifier("1")})
        assert actual == expected
