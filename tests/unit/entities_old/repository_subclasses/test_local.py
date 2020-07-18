from unittest.mock import call

import pytest

from link.entities import local
from link.entities import repository


def test_if_local_repository_is_subclass_of_repository():
    assert issubclass(local.LocalRepository, repository.NonSourceRepository)


@pytest.fixture
def repo_cls():
    class LocalRepository(local.LocalRepository):
        pass

    return LocalRepository


def test_if_link_is_none(repo):
    assert repo.link is None


@pytest.mark.usefixtures("add_link")
class TestDelete:
    @pytest.fixture
    def non_selected_identifiers(self, identifiers, selected_identifiers):
        return [identifier for identifier in identifiers if identifier not in selected_identifiers]

    def test_if_entities_are_deleted(self, identifiers, repo):
        repo.delete(identifiers)
        assert repo.identifiers == []

    def test_if_entities_are_deleted_in_outbound_repository(self, identifiers, repo, link):
        repo.delete(identifiers)
        link.delete_in_outbound_repo.assert_called_once_with(identifiers)

    def test_if_entities_are_not_deleted_if_deletion_fails_in_outbound_repository(self, identifiers, repo, link):
        link.delete_in_outbound_repo.side_effect = RuntimeError
        try:
            repo.delete(identifiers)
        except RuntimeError:
            pass
        assert repo.identifiers == identifiers

    def test_if_entities_are_not_deleted_in_outbound_repository_if_deletion_fails_in_local_repository(
        self, identifiers, gateway, repo, link
    ):
        gateway.delete.side_effect = RuntimeError
        try:
            repo.delete(identifiers)
        except RuntimeError:
            pass
        link.delete_in_outbound_repo.assert_not_called()


@pytest.mark.usefixtures("add_link")
class TestInsert:
    @pytest.fixture
    def test_if_error_is_raised_before_insertion(self, entities, new_entities, repo):
        def _test_if_error_is_raised_before_insertion():
            try:
                repo.insert(new_entities)
            except RuntimeError:
                pass
            assert repo.entities == entities

        return _test_if_error_is_raised_before_insertion

    def test_if_presence_of_entities_in_outbound_repository_is_checked(self, new_entities, repo, link):
        repo.insert(new_entities)
        assert link.not_present_in_outbound_repo.mock_calls == [call(entity.identifier) for entity in new_entities]

    def test_if_runtime_error_is_raised_if_one_or_more_entities_are_not_present_in_outbound_repository(
        self, new_entities, repo, link
    ):
        link.not_present_in_outbound_repo.return_value = True
        with pytest.raises(RuntimeError):
            repo.insert(new_entities)

    @pytest.mark.usefixtures("request_deletion_of_new_entities")
    def test_if_runtime_error_is_raised_if_one_or_more_entities_had_their_deletion_requested(self, new_entities, repo):
        with pytest.raises(RuntimeError):
            repo.insert(new_entities)

    def test_if_entities_are_inserted(self, entities, new_entities, repo):
        repo.insert(new_entities)
        assert repo.entities == entities + new_entities

    def test_if_runtime_error_due_to_absence_in_outbound_repository_is_raised_before_insertion(
        self, link, test_if_error_is_raised_before_insertion
    ):
        link.not_present_in_outbound_repo.return_value = True
        test_if_error_is_raised_before_insertion()

    @pytest.mark.usefixtures("request_deletion_of_new_entities")
    def test_if_runtime_error_due_to_deletion_requested_is_raised_before_insertion(
        self, test_if_error_is_raised_before_insertion
    ):
        test_if_error_is_raised_before_insertion()


def test_repr(repo):
    assert repr(repo) == "LocalRepository()"
