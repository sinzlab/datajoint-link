from unittest.mock import call

import pytest


USE_CASE = "delete"


@pytest.fixture
def config():
    return {
        "identifiers": {"source": 10, "outbound": 5, "local": 5},
        "flags": {
            "outbound": {"deletion_requested": [0, 2], "deletion_approved": []},
            "local": {"deletion_requested": []},
        },
    }


@pytest.fixture
def to_be_deleted_identifiers(create_identifiers):
    return create_identifiers(3)


@pytest.fixture(autouse=True)
def execute_delete(use_case, request_model, to_be_deleted_identifiers):
    use_case(request_model(to_be_deleted_identifiers))


def test_if_entities_that_had_their_deletion_requested_have_it_approved(processed_config, gateway_link_spy):
    gateway_link_spy.outbound.set_flag.assert_has_calls(
        [call(i, "deletion_approved", True) for i in processed_config["flags"]["outbound"]["deletion_requested"]],
        any_order=True,
    )


def test_if_entities_that_had_their_deletion_not_requested_are_deleted_from_outbound_repository(
    processed_config, gateway_link_spy, to_be_deleted_identifiers
):
    gateway_link_spy.outbound.delete.assert_has_calls(
        [
            call(i)
            for i in to_be_deleted_identifiers
            if i not in processed_config["flags"]["outbound"]["deletion_requested"]
        ],
        any_order=True,
    )


def test_if_entities_are_deleted_from_local_repository(to_be_deleted_identifiers, gateway_link_spy):
    gateway_link_spy.local.delete.assert_has_calls([call(i) for i in to_be_deleted_identifiers], any_order=True)
