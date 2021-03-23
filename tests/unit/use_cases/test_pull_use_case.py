from functools import partial
from itertools import compress
from unittest.mock import call, create_autospec

import pytest

from dj_link.entities.repository import TransferEntity
from dj_link.use_cases.pull import LOGGER

USE_CASE_NAME = "pull"


@pytest.fixture
def is_valid():
    return [True, False, True]


@pytest.fixture
def valid_identifiers(identifiers, is_valid):
    return list(compress(identifiers, is_valid))


@pytest.fixture
def transfer_entities(valid_identifiers):
    return [create_autospec(TransferEntity, instance=True, identifier=identifier) for identifier in valid_identifiers]


@pytest.fixture
def identifier_only_transfer_entities(transfer_entities):
    return [entity.create_identifier_only_copy.return_value for entity in transfer_entities]


@pytest.fixture
def repo_link_spy(repo_link_spy, is_valid, transfer_entities):
    repo_link_spy.outbound.__contains__.side_effect = [not flag for flag in is_valid]
    repo_link_spy.source.__getitem__.side_effect = transfer_entities
    return repo_link_spy


def test_if_presence_of_entities_in_outbound_repo_is_checked(use_case, request_model_stub, repo_link_spy, identifiers):
    use_case(request_model_stub)
    calls = [call(identifier) for identifier in identifiers]
    assert repo_link_spy.outbound.__contains__.call_args_list == calls


def test_if_entities_are_fetched(use_case, request_model_stub, repo_link_spy, valid_identifiers):
    use_case(request_model_stub)
    calls = [call(identifier) for identifier in valid_identifiers]
    assert repo_link_spy.source.__getitem__.call_args_list == calls


def test_if_transaction_is_started_in_outbound_repo(use_case, request_model_stub, repo_link_spy, identifiers):
    use_case(request_model_stub)
    repo_link_spy.outbound.transaction.assert_called_once_with()


def test_if_transaction_is_started_in_local_repo(use_case, request_model_stub, repo_link_spy):
    use_case(request_model_stub)
    repo_link_spy.local.transaction.assert_called_once_with()


@pytest.fixture
def pull_with_error(use_case, request_model_stub):
    def _pull_with_error():
        try:
            use_case(request_model_stub)
        except RuntimeError:
            pass

    return _pull_with_error


def test_if_transaction_is_started_in_outbound_repo_first(repo_link_spy, pull_with_error):
    repo_link_spy.outbound.transaction_manager.transaction.side_effect = RuntimeError
    pull_with_error()
    repo_link_spy.local.transaction_manager.transaction.assert_not_called()


def test_if_entities_are_inserted_into_outbound_repo(
    use_case, request_model_stub, repo_link_spy, valid_identifiers, identifier_only_transfer_entities
):
    use_case(request_model_stub)
    assert repo_link_spy.outbound.__setitem__.call_args_list == [
        call(identifier, entity) for identifier, entity in zip(valid_identifiers, identifier_only_transfer_entities)
    ]


def test_if_entities_are_inserted_into_local_repo(
    use_case, request_model_stub, repo_link_spy, valid_identifiers, transfer_entities
):
    use_case(request_model_stub)
    assert repo_link_spy.local.__setitem__.call_args_list == [
        call(identifier, entity) for identifier, entity in zip(valid_identifiers, transfer_entities)
    ]


def test_if_entities_are_inserted_into_outbound_repo_first(repo_link_spy, pull_with_error):
    repo_link_spy.outbound.__setitem__.side_effect = RuntimeError
    pull_with_error()
    repo_link_spy.local.__setitem__.assert_not_called()


def test_if_entities_are_inserted_into_outbound_repo_after_transaction_in_outbound_repo_is_started(
    repo_link_spy, pull_with_error
):
    repo_link_spy.outbound.transaction.side_effect = RuntimeError
    pull_with_error()
    repo_link_spy.outbound.contents.__setitem__.assert_not_called()


def test_if_entities_are_inserted_into_local_repo_after_transaction_in_local_repo_is_started(
    repo_link_spy, pull_with_error
):
    repo_link_spy.local.transaction.side_effect = RuntimeError
    pull_with_error()
    repo_link_spy.local.contents.__setitem__.assert_not_called()


def test_if_initialization_of_response_model_class_is_correct(
    use_case, request_model_stub, response_model_cls_spy, identifiers, valid_identifiers
):
    use_case(request_model_stub)
    response_model_cls_spy.assert_called_once_with(
        requested=set(identifiers),
        valid=set(valid_identifiers),
        invalid={i for i in identifiers if i not in valid_identifiers},
    )


def test_if_logged_messages_are_correct(is_correct_log, use_case, request_model_stub, valid_identifiers):
    messages = [f"Pulled entity with identifier {identifier}" for identifier in valid_identifiers]
    assert is_correct_log(LOGGER, partial(use_case, request_model_stub), messages)


def test_if_response_model_is_passed_to_output_port(
    use_case, request_model_stub, response_model_cls_spy, output_port_spy
):
    use_case(request_model_stub)
    output_port_spy.assert_called_once_with(response_model_cls_spy.return_value)
