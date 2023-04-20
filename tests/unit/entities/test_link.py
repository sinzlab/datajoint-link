from __future__ import annotations

from contextlib import nullcontext as does_not_raise
from typing import ContextManager

import pytest

from dj_link.entities.link import Components, Identifier, Link, Transfer, pull


class TestLink:
    @staticmethod
    @pytest.mark.parametrize(
        "source,outbound,local,expectation",
        [
            (set(), {Identifier("1")}, {Identifier("1")}, pytest.raises(AssertionError)),
            (set(), set(), set(), does_not_raise()),
            ({Identifier("1")}, set(), set(), does_not_raise()),
        ],
    )
    def test_outbound_identifiers_can_not_be_superset_of_source_identifiers(
        source: set[Identifier], outbound: set[Identifier], local: set[Identifier], expectation: ContextManager[None]
    ) -> None:
        with expectation:
            Link(source=source, outbound=outbound, local=local)

    @staticmethod
    @pytest.mark.parametrize(
        "source,outbound,local,expectation",
        [
            ({Identifier("1")}, {Identifier("1")}, {Identifier("1")}, does_not_raise()),
            ({Identifier("1")}, {Identifier("1")}, set(), pytest.raises(AssertionError)),
            ({Identifier("1")}, set(), {Identifier("1")}, pytest.raises(AssertionError)),
        ],
    )
    def test_local_identifiers_must_be_identical_to_outbound_identifiers(
        source: set[Identifier], outbound: set[Identifier], local: set[Identifier], expectation: ContextManager[None]
    ) -> None:
        with expectation:
            Link(source=source, outbound=outbound, local=local)


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
        "source,requested,expectation",
        [
            ({Identifier("1")}, {Identifier("1"), Identifier("2")}, pytest.raises(AssertionError)),
            ({Identifier("1")}, {Identifier("1")}, does_not_raise()),
            ({Identifier("1"), Identifier("2")}, {Identifier("1")}, does_not_raise()),
        ],
    )
    def test_requested_identifiers_can_not_be_superset_of_source_identifiers(
        source: set[Identifier], requested: set[Identifier], expectation: ContextManager[None]
    ) -> None:
        link = Link(source=source, outbound=set(), local=set())
        with expectation:
            pull(link, requested=requested)

    @staticmethod
    def test_if_correct_transfer_specifications_are_returned() -> None:
        link = Link(source={Identifier("1"), Identifier("2")}, outbound=set(), local=set())
        expected = {
            Transfer(Identifier("1"), Components.SOURCE, Components.OUTBOUND, identifier_only=True),
            Transfer(Identifier("1"), Components.SOURCE, Components.LOCAL, identifier_only=False),
        }
        actual = pull(link, requested={Identifier("1")})
        assert actual == expected
