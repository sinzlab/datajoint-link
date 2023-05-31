from __future__ import annotations

from contextlib import nullcontext as does_not_raise
from typing import ContextManager, Iterable, Mapping

import pytest

from dj_link.entities.custom_types import Identifier
from dj_link.entities.link import Transfer, create_link, delete, flag, process, pull, pull_legacy
from dj_link.entities.state import Components, Processes, State, states

from .assignments import create_assignments


class TestCreateLink:
    @staticmethod
    @pytest.mark.parametrize(
        "state,expected",
        [
            (states.Idle, {Identifier("1")}),
            (states.Activated, {Identifier("2"), Identifier("7")}),
            (states.Received, {Identifier("3"), Identifier("8")}),
            (states.Pulled, {Identifier("4")}),
            (states.Tainted, {Identifier("5")}),
            (states.Deprecated, {Identifier("6")}),
        ],
    )
    def test_entities_get_correct_state_assigned(
        state: type[State],
        expected: Iterable[Identifier],
    ) -> None:
        assignments = create_assignments(
            {
                Components.SOURCE: {"1", "2", "3", "4", "5", "6", "7", "8"},
                Components.OUTBOUND: {"2", "3", "4", "5", "7", "8"},
                Components.LOCAL: {"3", "4", "5", "8"},
            }
        )
        link = create_link(
            assignments,
            tainted_identifiers={Identifier("5"), Identifier("6"), Identifier("7"), Identifier("8")},
            processes={Processes.PULL: {Identifier("2"), Identifier("3"), Identifier("7"), Identifier("8")}},
        )
        assert {entity.identifier for entity in link[Components.SOURCE] if entity.state is state} == set(expected)

    @staticmethod
    @pytest.mark.parametrize(
        "processes,expectation",
        [
            (
                {Processes.PULL: {Identifier("1")}, Processes.DELETE: {Identifier("1")}},
                pytest.raises(AssertionError),
            ),
            ({Processes.PULL: {Identifier("1")}}, does_not_raise()),
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
                Processes.PULL: {Identifier("1"), Identifier("3")},
                Processes.DELETE: {Identifier("2"), Identifier("4")},
            },
        )
        expected = {
            (Identifier("1"), Processes.PULL),
            (Identifier("2"), Processes.DELETE),
            (Identifier("3"), Processes.PULL),
            (Identifier("4"), Processes.DELETE),
            (Identifier("5"), None),
        }
        assert {(entity.identifier, entity.current_process) for entity in link[Components.SOURCE]} == set(expected)

    @staticmethod
    @pytest.mark.parametrize("tainted_identifiers,is_tainted", [({Identifier("1")}, True), (set(), False)])
    def test_tainted_attribute_is_set(tainted_identifiers: Iterable[Identifier], is_tainted: bool) -> None:
        link = create_link(create_assignments({Components.SOURCE: {"1"}}), tainted_identifiers=tainted_identifiers)
        entity = next(iter(link[Components.SOURCE]))
        assert entity.is_tainted is is_tainted

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
        "processes,assignments,expectation",
        [
            ({}, create_assignments({Components.LOCAL: {"1"}}), pytest.raises(AssertionError)),
            ({}, create_assignments(), does_not_raise()),
            (
                {Processes.PULL: {Identifier("1")}},
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


class TestPullLegacy:
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
            pull_legacy(link, requested=requested)

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
            pull_legacy(link, requested=requested)

    @staticmethod
    def test_if_correct_transfer_specifications_are_returned() -> None:
        link = create_link(create_assignments({Components.SOURCE: {"1", "2"}}))
        expected = {
            Transfer(Identifier("1"), Components.SOURCE, Components.OUTBOUND, identifier_only=True),
            Transfer(Identifier("1"), Components.SOURCE, Components.LOCAL, identifier_only=False),
        }
        actual = pull_legacy(link, requested={Identifier("1")})
        assert actual == expected


def test_process_produces_correct_transitions() -> None:
    link = create_link(
        create_assignments(
            {
                Components.SOURCE: {"1", "2", "3", "4", "5"},
                Components.OUTBOUND: {"1", "2", "3", "4"},
                Components.LOCAL: {"2", "4"},
            }
        ),
        processes={
            Processes.PULL: {Identifier("1"), Identifier("2")},
            Processes.DELETE: {Identifier("3"), Identifier("4")},
        },
    )
    actual = {(update.identifier, update.transition.new) for update in process(link)}
    expected = {
        (Identifier("1"), states.Received),
        (Identifier("2"), states.Pulled),
        (Identifier("3"), states.Idle),
        (Identifier("4"), states.Activated),
        (Identifier("5"), states.Idle),
    }
    assert actual == expected


class TestPull:
    @staticmethod
    def test_idle_entity_becomes_activated() -> None:
        link = create_link(create_assignments({Components.SOURCE: {"1"}}))
        update = next(iter(pull(link, requested={Identifier("1")})))
        assert update.identifier == Identifier("1") and update.transition.new is states.Activated

    @staticmethod
    def test_not_specifying_requested_identifiers_raises_error() -> None:
        link = create_link(create_assignments({Components.SOURCE: {"1"}}))
        with pytest.raises(AssertionError, match="No identifiers to be pulled requested."):
            pull(link, requested={})

    @staticmethod
    def test_specifying_identifiers_not_present_in_link_raises_error() -> None:
        link = create_link(create_assignments({Components.SOURCE: {"1"}}))
        with pytest.raises(AssertionError, match="Requested identifiers not present in link."):
            pull(link, requested={Identifier("2")})


class TestDelete:
    @staticmethod
    def test_pulled_entity_becomes_received() -> None:
        link = create_link(
            create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
        )
        update = next(iter(delete(link, requested={Identifier("1")})))
        assert update.identifier == Identifier("1") and update.transition.new is states.Received

    @staticmethod
    def test_not_specifying_requested_identifiers_raises_error() -> None:
        link = create_link(
            create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
        )
        with pytest.raises(AssertionError, match="No identifiers to be deleted requested."):
            delete(link, requested={})

    @staticmethod
    def test_specifying_identifiers_not_present_in_link_raises_error() -> None:
        link = create_link(
            create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
        )
        with pytest.raises(AssertionError, match="Requested identifiers not present in link."):
            delete(link, requested={Identifier("2")})


class TestFlag:
    @staticmethod
    def test_pulled_entity_becomes_tainted() -> None:
        link = create_link(
            create_assignments({Components.SOURCE: {"1"}, Components.OUTBOUND: {"1"}, Components.LOCAL: {"1"}})
        )
        update = next(iter(flag(link, requested={Identifier("1")})))
        assert update.identifier == Identifier("1") and update.transition.new is states.Tainted
