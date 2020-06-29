from unittest.mock import MagicMock, call

import pytest

from link.entities import local
from link.entities import repository


def test_if_local_repository_is_subclass_of_repository():
    assert issubclass(local.LocalRepository, repository.Repository)


@pytest.fixture
def local_repo_cls(gateway, entity_creator):
    class LocalRepository(local.LocalRepository):
        pass

    LocalRepository.__qualname__ = LocalRepository.__name__
    LocalRepository.gateway = gateway
    LocalRepository.entity_creator = entity_creator
    return LocalRepository


@pytest.fixture
def outbound_repo():
    name = "outbound_repo"
    outbound_repo = MagicMock(name=name)
    outbound_repo.__contains__ = MagicMock(name=name + ".__contains__", return_value=True)
    outbound_repo.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return outbound_repo


@pytest.fixture
def local_repo(address, local_repo_cls, outbound_repo):
    return local_repo_cls(address, outbound_repo)


def test_if_outbound_repository_is_stored_as_instance_attribute(outbound_repo, local_repo):
    assert local_repo.outbound_repo is outbound_repo


class TestDelete:
    @pytest.fixture
    def non_selected_identifiers(self, identifiers, selected_identifiers):
        return [identifier for identifier in identifiers if identifier not in selected_identifiers]

    def test_if_entities_are_deleted(self, identifiers, local_repo):
        local_repo.delete(identifiers)
        assert local_repo.identifiers == []

    def test_if_entities_are_deleted_in_outbound_repository(self, identifiers, outbound_repo, local_repo):
        local_repo.delete(identifiers)
        outbound_repo.delete.assert_called_once_with(identifiers)

    def test_if_entities_are_not_deleted_if_deletion_fails_in_outbound_repository(
        self, identifiers, outbound_repo, local_repo
    ):
        outbound_repo.delete.side_effect = RuntimeError
        try:
            local_repo.delete(identifiers)
        except RuntimeError:
            pass
        assert local_repo.identifiers == identifiers

    def test_if_entities_are_not_deleted_in_outbound_repository_if_deletion_fails_in_local_repository(
        self, identifiers, gateway, outbound_repo, local_repo
    ):
        gateway.delete.side_effect = RuntimeError
        try:
            local_repo.delete(identifiers)
        except RuntimeError:
            pass
        outbound_repo.delete.assert_not_called()


class TestInsert:
    def test_if_presence_of_entities_in_outbound_repository_is_checked(self, new_entities, outbound_repo, local_repo):
        local_repo.insert(new_entities)
        assert outbound_repo.__contains__.mock_calls == [call(entity.identifier) for entity in new_entities]

    def test_if_runtime_error_is_raised_if_one_or_more_entities_are_not_present_in_outbound_repository(
        self, new_entities, outbound_repo, local_repo
    ):
        outbound_repo.__contains__.return_value = False
        with pytest.raises(RuntimeError):
            local_repo.insert(new_entities)

    @pytest.mark.usefixtures("request_deletion_of_new_entities")
    def test_if_runtime_error_is_raised_if_one_or_more_entities_had_their_deletion_requested(
        self, new_entities, local_repo
    ):
        with pytest.raises(RuntimeError):
            local_repo.insert(new_entities)

    def test_if_entities_are_inserted(self, entities, new_entities, local_repo):
        local_repo.insert(new_entities)
        assert local_repo.entities == entities + new_entities

    def test_if_runtime_error_due_to_absence_in_outbound_repository_is_raised_before_insertion(
        self, entities, new_entities, outbound_repo, local_repo
    ):
        outbound_repo.__contains__.return_value = False
        try:
            local_repo.insert(new_entities)
        except RuntimeError:
            pass
        assert local_repo.entities == entities

    @pytest.mark.usefixtures("request_deletion_of_new_entities")
    def test_if_runtime_error_due_to_deletion_requested_is_raised_before_insertion(
        self, entities, new_entities, local_repo
    ):
        try:
            local_repo.insert(new_entities)
        except RuntimeError:
            pass
        assert local_repo.entities == entities


def test_repr(local_repo):
    assert repr(local_repo) == "LocalRepository(address, outbound_repo)"
