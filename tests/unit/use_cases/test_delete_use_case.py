from unittest.mock import call
from itertools import compress

import pytest

from link.use_cases.delete import DeleteUseCase
from link.use_cases.base import UseCase


USE_CASE = DeleteUseCase


def test_if_subclass_of_use_case():
    assert issubclass(DeleteUseCase, UseCase)


@pytest.fixture
def deletion_requested():
    return [False, True, False]


@pytest.fixture
def flag_manager_spies(create_flag_manager_spies, identifiers, deletion_requested):
    return create_flag_manager_spies(identifiers, deletion_requested)


@pytest.fixture
def repo_link_spy(repo_link_spy, flag_manager_spies):
    repo_link_spy.outbound.flags = flag_manager_spies
    return repo_link_spy


def test_if_deletion_requested_flag_is_checked_in_flag_managers(use_case, identifiers, flag_manager_spies):
    use_case(identifiers)
    for spy in flag_manager_spies.values():
        spy.__getitem__.assert_called_once_with("deletion_requested")


def test_if_deletion_is_approved_on_entities_that_had_it_requested(
    use_case, identifiers, deletion_requested, flag_manager_spies
):
    use_case(identifiers)
    for spy in compress(flag_manager_spies.values(), deletion_requested):
        spy.__setitem__.assert_called_once_with("deletion_approved", True)


def test_if_entities_that_had_their_deletion_not_requested_are_deleted_from_outbound_repository(
    use_case, identifiers, deletion_requested, repo_link_spy
):
    use_case(identifiers)
    repo_link_spy.outbound.__delitem__.assert_has_calls(
        [call(i) for i in compress(identifiers, [not f for f in deletion_requested])], any_order=True
    )


def test_if_all_entities_are_deleted_from_local_repository(use_case, identifiers, repo_link_spy):
    use_case(identifiers)
    assert repo_link_spy.local.__delitem__.call_args_list == [call(i) for i in identifiers]
