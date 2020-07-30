from unittest.mock import MagicMock, call
from itertools import compress

import pytest

from link.use_cases import RepositoryLinkFactory
from link.use_cases.pull import Pull
from link.use_cases.base import UseCase


@pytest.fixture
def identifiers():
    return ["identifier" + str(i) for i in range(3)]


@pytest.fixture
def is_valid():
    return [True, False, True]


@pytest.fixture
def valid_identifiers(identifiers, is_valid):
    return list(compress(identifiers, is_valid))


@pytest.fixture
def fetched_entities(valid_identifiers):
    return [MagicMock(name="entity", identifier=identifier) for identifier in valid_identifiers]


@pytest.fixture
def repo_link_spy(is_valid, fetched_entities):
    repo_link_spy = MagicMock(name="repo_link_spy")
    repo_link_spy.local.contents.__contains__.side_effect = [not flag for flag in is_valid]
    repo_link_spy.source.contents.__getitem__.side_effect = fetched_entities
    return repo_link_spy


@pytest.fixture
def repo_link_factory_stub(repo_link_spy):
    return MagicMock(name="repo_link_factory", spec=RepositoryLinkFactory, return_value=repo_link_spy)


@pytest.fixture
def dummy_output_port():
    return MagicMock(name="dummy_output_port")


@pytest.fixture
def use_case(repo_link_factory_stub, dummy_output_port):
    return Pull(repo_link_factory_stub, dummy_output_port)


def test_if_pull_is_use_case():
    assert issubclass(Pull, UseCase)


def test_if_presence_of_entities_in_local_repo_is_checked(use_case, repo_link_spy, identifiers):
    use_case(identifiers)
    calls = [call(identifier) for identifier in identifiers]
    assert repo_link_spy.local.contents.__contains__.call_args_list == calls


def test_if_entities_are_fetched(use_case, repo_link_spy, identifiers, valid_identifiers):
    use_case(identifiers)
    calls = [call(identifier) for identifier in valid_identifiers]
    assert repo_link_spy.source.contents.__getitem__.call_args_list == calls


def test_if_transaction_is_started_in_outbound_repo(use_case, repo_link_spy, identifiers):
    use_case(identifiers)
    repo_link_spy.outbound.transaction.transaction.assert_called_once_with()


def test_if_transaction_is_started_in_local_repo(use_case, repo_link_spy, identifiers):
    use_case(identifiers)
    repo_link_spy.local.transaction.transaction.assert_called_once_with()


@pytest.fixture
def pull_with_error(use_case, identifiers):
    def _pull_with_error():
        try:
            use_case(identifiers)
        except RuntimeError:
            pass

    return _pull_with_error


def test_if_transaction_is_started_in_outbound_repo_first(repo_link_spy, pull_with_error):
    repo_link_spy.outbound.transaction.transaction.side_effect = RuntimeError
    pull_with_error()
    repo_link_spy.local.transaction.transaction.assert_not_called()


def test_if_entities_are_inserted_into_outbound_repo(
    use_case, identifiers, repo_link_spy, valid_identifiers, fetched_entities
):
    use_case(identifiers)
    assert repo_link_spy.outbound.contents.__setitem__.call_args_list == [
        call(identifier, entity) for identifier, entity in zip(valid_identifiers, fetched_entities)
    ]


def test_if_entities_are_inserted_into_local_repo(
    use_case, identifiers, repo_link_spy, valid_identifiers, fetched_entities
):
    use_case(identifiers)
    assert repo_link_spy.local.contents.__setitem__.call_args_list == [
        call(identifier, entity) for identifier, entity in zip(valid_identifiers, fetched_entities)
    ]


def test_if_entities_are_inserted_into_outbound_repo_first(repo_link_spy, pull_with_error):
    repo_link_spy.outbound.contents.__setitem__.side_effect = RuntimeError
    pull_with_error()
    repo_link_spy.local.contents.__setitem__.assert_not_called()


def test_if_entities_are_inserted_into_outbound_repo_after_transaction_in_outbound_repo_is_started(
    repo_link_spy, pull_with_error
):
    repo_link_spy.outbound.transaction.transaction.side_effect = RuntimeError
    pull_with_error()
    repo_link_spy.outbound.contents.__setitem__.assert_not_called()


def test_if_entities_are_inserted_into_local_repo_after_transaction_in_local_repo_is_started(
    repo_link_spy, pull_with_error
):
    repo_link_spy.local.transaction.transaction.side_effect = RuntimeError
    pull_with_error()
    repo_link_spy.local.contents.__setitem__.assert_not_called()
