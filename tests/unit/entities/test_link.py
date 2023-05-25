from __future__ import annotations

from contextlib import nullcontext as does_not_raise
from typing import ContextManager, Iterable, Mapping, Optional

import pytest

from dj_link.entities.link import Components, Identifier, Marks, States, Transfer, create_link, pull


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
            (States.IDLE, {Identifier("1")}),
            (States.ACTIVATED, {Identifier("2")}),
            (States.RECEIVED, {Identifier("3")}),
            (States.PULLED, {Identifier("4")}),
            (States.TAINTED, {Identifier("5")}),
            (States.DEPRECATED, {Identifier("6")}),
        ],
    )
    def test_entities_get_correct_state_assigned(
        state: States,
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
            marks={Marks.PULL: {Identifier("2"), Identifier("3")}},
        )
        assert {entity.identifier for entity in link[Components.SOURCE] if entity.state is state} == set(expected)

    @staticmethod
    @pytest.mark.parametrize(
        "marks,expectation",
        [
            ({Marks.PULL: {Identifier("1")}, Marks.DELETE: {Identifier("1")}}, pytest.raises(AssertionError)),
            ({Marks.PULL: {Identifier("1")}}, does_not_raise()),
        ],
    )
    def test_identifiers_can_only_be_associated_with_single_mark(
        marks: Mapping[Marks, Iterable[Identifier]], expectation: ContextManager[None]
    ) -> None:
        with expectation:
            create_link(create_assignments(), marks=marks)

    @staticmethod
    def test_entities_get_correct_mark_assigned() -> None:
        link = create_link(
            create_assignments(
                {
                    Components.SOURCE: {"1", "2", "3", "4", "5"},
                    Components.OUTBOUND: {"1", "2", "3", "4"},
                    Components.LOCAL: {"3", "4"},
                }
            ),
            marks={Marks.PULL: {Identifier("1"), Identifier("3")}, Marks.DELETE: {Identifier("2"), Identifier("4")}},
        )
        expected = {
            (Identifier("1"), Marks.PULL),
            (Identifier("2"), Marks.DELETE),
            (Identifier("3"), Marks.PULL),
            (Identifier("4"), Marks.DELETE),
            (Identifier("5"), None),
        }
        assert {(entity.identifier, entity.mark) for entity in link[Components.SOURCE]} == set(expected)

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
        "marks,assignments,expectation",
        [
            ({}, create_assignments({Components.LOCAL: {"1"}}), pytest.raises(AssertionError)),
            ({}, create_assignments(), does_not_raise()),
            (
                {Marks.PULL: {Identifier("1")}},
                create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
                does_not_raise(),
            ),
        ],
    )
    def test_local_identifiers_can_not_be_superset_of_outbound_identifiers(
        marks: Mapping[Marks, Iterable[Identifier]],
        assignments: Mapping[Components, Iterable[Identifier]],
        expectation: ContextManager[None],
    ) -> None:
        with expectation:
            create_link(assignments, marks=marks)


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
