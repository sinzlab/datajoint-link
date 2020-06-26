from unittest.mock import MagicMock, call

import pytest

from link.entities import outbound
from link.entities import repository


@pytest.fixture
def repo_cls():
    class Repository(repository.Repository):
        __init__ = MagicMock(name="Repository.__init__")

    return Repository


@pytest.fixture
def outbound_repo_cls(address, entities, repo_cls):
    class OutboundRepository(outbound.OutboundRepository, repo_cls):
        pass

    OutboundRepository.__qualname__ = OutboundRepository.__name__
    OutboundRepository.address = address
    OutboundRepository.__getitem__ = MagicMock(name=OutboundRepository.__qualname__, side_effect=entities)
    return OutboundRepository


@pytest.fixture
def local_repo():
    name = "local_repo"
    local_repo = MagicMock(name=name)
    local_repo.__contains__ = MagicMock(name=name + ".__contains__", return_value=False)
    local_repo.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return local_repo


@pytest.fixture
def outbound_repo(outbound_repo_cls, address, local_repo):
    return outbound_repo_cls(address, local_repo)


class TestInit:
    @pytest.mark.usefixtures("outbound_repo")
    def test_if_super_class_is_initialized(self, repo_cls, address):
        repo_cls.__init__.assert_called_once_with(address)

    def test_if_local_repository_is_stored_as_instance_attribute(self, local_repo, outbound_repo):
        assert outbound_repo.local_repo is local_repo


class TestDelete:
    @pytest.fixture
    def entities(self, entities):
        for entity in entities:
            entity.deletion_requested = False
        return entities

    @pytest.fixture
    def repo_cls(self, repo_cls):
        repo_cls.delete = MagicMock(name=repo_cls.__name__ + ".delete")
        return repo_cls

    @pytest.fixture
    def request_deletion(self, entities):
        for entity in entities:
            entity.deletion_requested = True

    def test_if_presence_of_entities_in_local_repository_is_checked(self, identifiers, local_repo, outbound_repo):
        outbound_repo.delete(identifiers)
        assert local_repo.__contains__.mock_calls == [call(identifier) for identifier in identifiers]

    def test_if_runtime_error_is_raised_if_one_or_more_entities_are_present_in_local_repository(
        self, identifiers, local_repo, outbound_repo
    ):
        local_repo.__contains__.return_value = True
        with pytest.raises(RuntimeError):
            outbound_repo.delete(identifiers)

    @pytest.mark.usefixtures("request_deletion")
    def test_if_runtime_error_is_raised_if_one_or_more_entities_had_deletion_requested(
        self, identifiers, outbound_repo
    ):
        with pytest.raises(RuntimeError):
            outbound_repo.delete(identifiers)

    def test_if_entities_are_deleted(self, identifiers, repo_cls, outbound_repo):
        outbound_repo.delete(identifiers)
        repo_cls.delete.assert_called_once_with(identifiers)

    def test_if_runtime_error_due_to_local_presence_is_raised_before_deletion(
        self, identifiers, repo_cls, local_repo, outbound_repo
    ):
        local_repo.__contains__.return_value = True
        try:
            outbound_repo.delete(identifiers)
        except RuntimeError:
            pass
        repo_cls.delete.assert_not_called()

    @pytest.mark.usefixtures("request_deletion")
    def test_if_runtime_error_due_to_deletion_request_is_raised_before_deletion(
        self, identifiers, repo_cls, outbound_repo
    ):
        try:
            outbound_repo.delete(identifiers)
        except RuntimeError:
            pass
        repo_cls.delete.assert_not_called()


def test_repr(outbound_repo):
    assert repr(outbound_repo) == "OutboundRepository(address, local_repo)"
