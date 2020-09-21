from unittest.mock import call, create_autospec
from itertools import compress

import pytest

from link.use_cases.delete import DeleteRequestModel, DeleteResponseModel, DeleteUseCase


USE_CASE = DeleteUseCase


@pytest.fixture
def request_model_stub(identifiers):
    stub = create_autospec(DeleteRequestModel, instance=True)
    stub.identifiers = identifiers
    return stub


@pytest.fixture
def response_model_cls_spy():
    return create_autospec(DeleteResponseModel)


@pytest.fixture
def use_case_cls(response_model_cls_spy):
    DeleteUseCase.response_model_cls = response_model_cls_spy
    return DeleteUseCase


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


def test_if_deletion_requested_flag_is_checked_in_flag_managers(use_case, request_model_stub, flag_manager_spies):
    use_case(request_model_stub)
    for spy in flag_manager_spies.values():
        spy.__getitem__.assert_called_once_with("deletion_requested")


def test_if_deletion_is_approved_on_entities_that_had_it_requested(
    use_case, request_model_stub, deletion_requested, flag_manager_spies
):
    use_case(request_model_stub)
    for spy in compress(flag_manager_spies.values(), deletion_requested):
        spy.__setitem__.assert_called_once_with("deletion_approved", True)


@pytest.fixture
def deletion_not_requested_identifiers(identifiers, deletion_requested):
    return compress(identifiers, [not f for f in deletion_requested])


def test_if_entities_that_had_their_deletion_not_requested_are_deleted_from_outbound_repository(
    use_case, request_model_stub, repo_link_spy, deletion_not_requested_identifiers
):
    use_case(request_model_stub)
    repo_link_spy.outbound.__delitem__.assert_has_calls(
        [call(i) for i in deletion_not_requested_identifiers], any_order=True
    )


def test_if_all_entities_are_deleted_from_local_repository(use_case, request_model_stub, identifiers, repo_link_spy):
    use_case(request_model_stub)
    assert repo_link_spy.local.__delitem__.call_args_list == [call(i) for i in identifiers]


def test_if_initialization_of_response_model_class_is_correct(
    use_case,
    request_model_stub,
    identifiers,
    response_model_cls_spy,
    deletion_requested,
    deletion_not_requested_identifiers,
):
    use_case(request_model_stub)
    response_model_cls_spy.assert_called_once_with(
        requested=set(identifiers),
        deletion_approved=set(compress(identifiers, deletion_requested)),
        deleted_from_outbound=set(deletion_not_requested_identifiers),
        deleted_from_local=set(identifiers),
    )


def test_if_response_model_is_passed_to_output_port(
    use_case, request_model_stub, output_port_spy, response_model_cls_spy
):
    use_case(request_model_stub)
    output_port_spy.assert_called_once_with(response_model_cls_spy.return_value)
