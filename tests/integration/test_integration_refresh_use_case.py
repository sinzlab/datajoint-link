from unittest.mock import call

import pytest


USE_CASE = "refresh"


@pytest.fixture
def outbound_deletion_requested_identifiers(create_identifiers):
    return create_identifiers([0, 2, 3])


@pytest.fixture
def local_deletion_requested_identifiers(create_identifiers):
    return create_identifiers([0, 3])


def test_if_deletion_requested_flag_is_enabled_on_correct_entities_in_local_gateway(
    use_case, gateway_link_spy, outbound_deletion_requested_identifiers, local_deletion_requested_identifiers
):
    use_case()
    to_be_enabled = [
        i for i in outbound_deletion_requested_identifiers if i not in local_deletion_requested_identifiers
    ]
    gateway_link_spy.local.set_flag.assert_has_calls(
        [call(i, "deletion_requested", True) for i in to_be_enabled], any_order=True
    )
