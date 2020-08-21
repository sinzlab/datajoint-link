from unittest.mock import call

import pytest


USE_CASE = "delete"


@pytest.fixture
def to_be_deleted_identifiers(create_identifiers):
    return create_identifiers(3)


@pytest.fixture(autouse=True)
def execute_delete(use_case, to_be_deleted_identifiers):
    use_case(to_be_deleted_identifiers)


@pytest.fixture
def deletion_requested_identifiers():
    return ["identifier" + str(i) for i in [0, 2]]


def test_if_entities_that_had_their_deletion_requested_have_it_approved(
    gateway_link_spy, deletion_requested_identifiers
):
    gateway_link_spy.outbound.set_flag.assert_has_calls(
        [call(i, "deletion_approved", True) for i in deletion_requested_identifiers], any_order=True,
    )


def test_if_entities_that_had_their_deletion_not_requested_are_deleted_from_outbound_repository(
    gateway_link_spy, deletion_requested_identifiers, to_be_deleted_identifiers
):
    gateway_link_spy.outbound.delete.assert_has_calls(
        [call(i) for i in to_be_deleted_identifiers if i not in deletion_requested_identifiers], any_order=True
    )


def test_if_entities_are_deleted_from_local_repository(to_be_deleted_identifiers, gateway_link_spy):
    gateway_link_spy.local.delete.assert_has_calls([call(i) for i in to_be_deleted_identifiers], any_order=True)
