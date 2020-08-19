from unittest.mock import call

import pytest


USE_CASE = "delete"


@pytest.fixture
def to_be_deleted_identifiers(local_identifiers):
    return local_identifiers[:3]


@pytest.fixture(autouse=True)
def execute_delete(use_case, to_be_deleted_identifiers):
    use_case(to_be_deleted_identifiers)


@pytest.fixture
def all_flags(outbound_identifiers):
    outbound_flags = {i: dict(deletion_requested=False, deletion_approved=False) for i in outbound_identifiers}
    outbound_flags[outbound_identifiers[0]]["deletion_requested"] = True
    outbound_flags[outbound_identifiers[2]]["deletion_requested"] = True
    local_flags = {i: dict(deletion_requested=f["deletion_requested"]) for i, f in outbound_flags.items()}
    return dict(outbound=outbound_flags, local=local_flags)


@pytest.fixture
def gateway_link_spy(gateway_link_spy, all_flags):
    def make_get_flags(flags):
        def get_flags(identifier):
            return flags[identifier]

        return get_flags

    for name, repo_flags in all_flags.items():
        getattr(gateway_link_spy, name).get_flags.side_effect = make_get_flags(repo_flags)
    return gateway_link_spy


def test_if_entities_that_had_their_deletion_requested_have_it_approved(gateway_link_spy, outbound_identifiers):
    gateway_link_spy.outbound.set_flag.assert_has_calls(
        [
            call(outbound_identifiers[0], "deletion_approved", True),
            call(outbound_identifiers[2], "deletion_approved", True),
        ],
        any_order=True,
    )


def test_if_entities_that_had_their_deletion_not_requested_are_deleted_from_outbound_repository(
    gateway_link_spy, outbound_identifiers
):
    gateway_link_spy.outbound.delete.assert_has_calls([call(outbound_identifiers[1])], any_order=True)


def test_if_entities_are_deleted_from_local_repository(to_be_deleted_identifiers, gateway_link_spy):
    gateway_link_spy.local.delete.assert_has_calls([call(i) for i in to_be_deleted_identifiers], any_order=True)
