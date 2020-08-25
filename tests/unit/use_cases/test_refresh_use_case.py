from itertools import compress
from unittest.mock import create_autospec

import pytest

from link.use_cases.refresh import RefreshUseCase, RefreshResponseModel


USE_CASE = RefreshUseCase


@pytest.fixture
def response_model_cls_spy():
    return create_autospec(RefreshResponseModel)


@pytest.fixture
def use_case_cls(response_model_cls_spy):
    RefreshUseCase.response_model_cls = response_model_cls_spy
    return RefreshUseCase


@pytest.fixture
def outbound_deletion_requested():
    return [True, False, True]


@pytest.fixture
def outbound_flag_manager_spies(create_flag_manager_spies, identifiers, outbound_deletion_requested):
    return create_flag_manager_spies(identifiers, outbound_deletion_requested)


@pytest.fixture
def local_deletion_requested():
    return [False, False, True]


@pytest.fixture
def local_flag_manager_spies(create_flag_manager_spies, identifiers, local_deletion_requested):
    return create_flag_manager_spies(identifiers, local_deletion_requested)


@pytest.fixture
def repo_link_spy(repo_link_spy, identifiers, outbound_flag_manager_spies, local_flag_manager_spies):
    repo_link_spy.outbound.__iter__.return_value = identifiers
    repo_link_spy.outbound.flags = outbound_flag_manager_spies
    repo_link_spy.local.flags = local_flag_manager_spies
    return repo_link_spy


def test_if_deletion_requested_flag_is_checked_on_all_entities_in_outbound_repo(use_case, outbound_flag_manager_spies):
    use_case()
    for spy in outbound_flag_manager_spies.values():
        spy.__getitem__.assert_called_once_with("deletion_requested")


def test_if_deletion_requested_flag_is_checked_on_local_entities_corresponding_to_outbound_entities_that_had_it_enabled(
    use_case, outbound_deletion_requested, local_flag_manager_spies
):
    use_case()
    for spy in compress(local_flag_manager_spies.values(), outbound_deletion_requested):
        spy.__getitem__.assert_called_once_with("deletion_requested")


@pytest.fixture
def to_be_enabled(outbound_deletion_requested, local_deletion_requested):
    return [b1 and not b2 for b1, b2 in zip(outbound_deletion_requested, local_deletion_requested)]


def test_if_deletion_requested_flag_is_enabled_on_local_entities(use_case, to_be_enabled, local_flag_manager_spies):
    use_case()
    for spy in compress(local_flag_manager_spies.values(), to_be_enabled):
        spy.__setitem__.assert_called_once_with("deletion_requested", True)


def test_if_initialization_of_response_model_class_is_correct(
    use_case, response_model_cls_spy, identifiers, to_be_enabled
):
    use_case()
    response_model_cls_spy.assert_called_once_with(set(compress(identifiers, to_be_enabled)))


def test_if_response_model_is_passed_to_output_port(use_case, response_model_cls_spy, output_port_spy):
    use_case()
    output_port_spy.assert_called_once_with(response_model_cls_spy.return_value)
