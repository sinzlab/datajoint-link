from __future__ import annotations

from contextlib import nullcontext as does_not_raise
from typing import ContextManager, Iterable, Mapping

import pytest

from link.domain.custom_types import Identifier
from link.domain.link import create_link
from link.domain.state import Components, Processes, State, states
from tests.assignments import create_assignments, create_identifier, create_identifiers


class TestCreateLink:
    @staticmethod
    @pytest.mark.parametrize(
        ("state", "expected"),
        [
            (states.Unshared, create_identifiers("1")),
            (states.Activated, create_identifiers("2", "7")),
            (states.Received, create_identifiers("3", "8")),
            (states.Shared, create_identifiers("4")),
            (states.Tainted, create_identifiers("5")),
            (states.Deprecated, create_identifiers("6")),
        ],
    )
    def test_entities_get_correct_state_assigned(
        state: type[State],
        expected: Iterable[Identifier],
    ) -> None:
        assignments = create_assignments(
            {
                Components.SOURCE: {"1", "2", "3", "4", "5", "6", "7", "8"},
                Components.OUTBOUND: {"2", "3", "4", "5", "6", "7", "8"},
                Components.LOCAL: {"3", "4", "5", "8"},
            }
        )
        link = create_link(
            assignments,
            tainted_identifiers=create_identifiers("5", "6", "7", "8"),
            processes={Processes.PULL: create_identifiers("2", "3", "7", "8")},
        )
        assert {entity.identifier for entity in link if entity.state is state} == set(expected)

    @staticmethod
    @pytest.mark.parametrize(
        ("processes", "expectation"),
        [
            (
                {Processes.PULL: create_identifiers("1"), Processes.DELETE: create_identifiers("1")},
                pytest.raises(AssertionError),
            ),
            ({Processes.PULL: create_identifiers("1")}, does_not_raise()),
        ],
    )
    def test_identifiers_can_only_be_associated_with_single_process(
        processes: Mapping[Processes, Iterable[Identifier]], expectation: ContextManager[None]
    ) -> None:
        with expectation:
            create_link(create_assignments(), processes=processes)

    @staticmethod
    def test_entities_get_correct_process_assigned() -> None:
        link = create_link(
            create_assignments(
                {
                    Components.SOURCE: {"1", "2", "3", "4", "5"},
                    Components.OUTBOUND: {"1", "2", "3", "4"},
                    Components.LOCAL: {"3", "4"},
                }
            ),
            processes={
                Processes.PULL: create_identifiers("1", "3"),
                Processes.DELETE: create_identifiers("2", "4"),
            },
        )
        expected = {
            (create_identifier("1"), Processes.PULL),
            (create_identifier("2"), Processes.DELETE),
            (create_identifier("3"), Processes.PULL),
            (create_identifier("4"), Processes.DELETE),
            (create_identifier("5"), Processes.NONE),
        }
        assert {(entity.identifier, entity.current_process) for entity in link} == set(expected)

    @staticmethod
    def test_tainted_attribute_is_set() -> None:
        link = create_link(
            create_assignments({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"1"}}),
            tainted_identifiers=create_identifiers("1"),
        )
        expected = {(create_identifier("1"), True), (create_identifier("2"), False)}
        actual = {(entity.identifier, entity.is_tainted) for entity in link}
        assert actual == expected

    @staticmethod
    @pytest.mark.parametrize(
        ("assignments", "expectation"),
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
            create_link(assignments, tainted_identifiers=create_identifiers("1"))

    @staticmethod
    @pytest.mark.parametrize(
        ("assignments", "expectation"),
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
        ("processes", "assignments", "expectation"),
        [
            ({}, create_assignments({Components.LOCAL: {"1"}}), pytest.raises(AssertionError)),
            ({}, create_assignments(), does_not_raise()),
            (
                {Processes.PULL: create_identifiers("1")},
                create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}}),
                does_not_raise(),
            ),
        ],
    )
    def test_local_identifiers_can_not_be_superset_of_outbound_identifiers(
        processes: Mapping[Processes, Iterable[Identifier]],
        assignments: Mapping[Components, Iterable[Identifier]],
        expectation: ContextManager[None],
    ) -> None:
        with expectation:
            create_link(assignments, processes=processes)


class TestLink:
    @staticmethod
    @pytest.fixture()
    def assignments() -> dict[Components, set[Identifier]]:
        return create_assignments({Components.SOURCE: {"1", "2"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})

    @staticmethod
    def test_can_get_identifiers_of_entities_in_component(
        assignments: Mapping[Components, Iterable[Identifier]]
    ) -> None:
        link = create_link(assignments)
        assert set(link.identifiers) == create_identifiers("1", "2")

    @staticmethod
    def test_accessing_entity_not_present_in_link_raises_error() -> None:
        link = create_link(create_assignments({Components.SOURCE: {"1"}}))
        with pytest.raises(KeyError, match="Requested entity not present in link."):
            link[create_identifier("2")]
