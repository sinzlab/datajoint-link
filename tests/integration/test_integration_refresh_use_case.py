from unittest.mock import call

import pytest

USE_CASE = "refresh"


@pytest.fixture
def config():
    return {
        "identifiers": {"source": 10, "outbound": 5, "local": 5},
        "flags": {
            "outbound": {"deletion_requested": [0, 2, 3], "deletion_approved": []},
            "local": {"deletion_requested": [0, 3]},
        },
    }


def test_if_deletion_requested_flag_is_enabled_on_correct_entities_in_local_gateway(
    processed_config, use_case, request_model, gateway_link_spy
):
    use_case(request_model())
    to_be_enabled = [
        i
        for i in processed_config["flags"]["outbound"]["deletion_requested"]
        if i not in processed_config["flags"]["local"]["deletion_requested"]
    ]
    gateway_link_spy.local.set_flag.assert_has_calls(
        [call(i, "deletion_requested", True) for i in to_be_enabled], any_order=True
    )
