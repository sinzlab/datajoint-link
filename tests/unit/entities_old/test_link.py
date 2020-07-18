from unittest.mock import MagicMock, call

import pytest

from link.entities.local import LocalRepository
from link.entities.outbound import OutboundRepository
from link.entities import link


@pytest.fixture
def create_repo_mock():
    def _create_repo_mock(name, spec):
        repo = MagicMock(name=name, spec=spec)
        repo.__contains__ = MagicMock(name=name + ".__contains__", return_value=True)
        repo.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
        return repo

    return _create_repo_mock


@pytest.fixture
def local_repo(create_repo_mock):
    return create_repo_mock("local_repo", LocalRepository)


@pytest.fixture
def outbound_repo(create_repo_mock):
    return create_repo_mock("outbound_repo", spec=OutboundRepository)


@pytest.fixture
def link_instance(local_repo, outbound_repo):
    return link.Link(local_repo, outbound_repo)


class TestInit:
    def test_if_local_repository_is_stored_as_instance_attribute(self, link_instance, local_repo):
        assert link_instance.local_repo is local_repo

    def test_if_outbound_repository_is_stored_as_instance_attribute(self, link_instance, outbound_repo):
        assert link_instance.outbound_repo is outbound_repo

    def test_if_link_attribute_is_set_in_local_repository(self, link_instance, local_repo):
        assert local_repo.link is link_instance

    def test_if_link_attribute_is_set_in_outbound_repository(self, link_instance, outbound_repo):
        assert outbound_repo.link is link_instance


class TestPresentInLocalRepository:
    def test_if_presence_is_checked_in_local_repository(self, identifiers, link_instance, local_repo):
        for identifier in identifiers:
            link_instance.present_in_local_repo(identifier)
        assert local_repo.__contains__.mock_calls == [call(identifier) for identifier in identifiers]

    def test_if_correct_value_is_returned(self, identifiers, link_instance, local_repo):
        assert all(link_instance.present_in_local_repo(identifier) is True for identifier in identifiers)


class TestNotPresentInOutboundRepository:
    def test_if_presence_is_checked_in_outbound_repository(self, identifiers, link_instance, outbound_repo):
        for identifier in identifiers:
            link_instance.not_present_in_outbound_repo(identifier)
        assert outbound_repo.__contains__.mock_calls == [call(identifier) for identifier in identifiers]

    def test_if_correct_value_is_returned(self, identifiers, link_instance, outbound_repo):
        assert all(link_instance.not_present_in_outbound_repo(identifier) is False for identifier in identifiers)


def test_if_delete_in_outbound_repository_deletes_in_outbound_repository(identifiers, link_instance, outbound_repo):
    link_instance.delete_in_outbound_repo(identifiers)
    outbound_repo.delete.assert_called_once_with(identifiers)


def test_repr(link_instance):
    assert repr(link_instance) == "Link(local_repo, outbound_repo)"
