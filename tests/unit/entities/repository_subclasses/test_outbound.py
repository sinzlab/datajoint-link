from unittest.mock import call

import pytest

from link.entities import outbound
from link.entities import repository


def test_if_outbound_repository_is_subclass_of_repository():
    assert issubclass(outbound.OutboundRepository, repository.NonSourceRepository)


@pytest.fixture
def repo_cls():
    class OutboundRepository(outbound.OutboundRepository):
        pass

    return OutboundRepository


def test_if_link_is_none(repo):
    assert repo.link is None


@pytest.mark.usefixtures("add_link")
class TestDelete:
    @pytest.fixture
    def selected_entities(self, entities, selected_identifiers):
        return [entity for entity in entities if entity.identifier in selected_identifiers]

    def test_if_presence_of_entities_in_local_repository_is_checked(self, identifiers, repo, link):
        repo.delete(identifiers)
        assert link.present_in_local_repo.mock_calls == [call(identifier) for identifier in identifiers]

    def test_if_runtime_error_is_raised_if_one_or_more_entities_are_present_in_local_repository(
        self, identifiers, link, repo
    ):
        link.present_in_local_repo.return_value = True
        with pytest.raises(RuntimeError):
            repo.delete(identifiers)

    @pytest.mark.usefixtures("request_deletion_of_present_entities")
    def test_if_entities_that_had_their_deletion_requested_have_it_approved(self, identifiers, repo, selected_entities):
        repo.delete(identifiers)
        assert all(entity.deletion_approved is True for entity in selected_entities)

    @pytest.mark.usefixtures("request_deletion_of_present_entities")
    def test_if_entities_that_had_their_deletion_requested_have_it_approved_in_gateway(
        self, identifiers, gateway, repo, selected_identifiers
    ):
        repo.delete(identifiers)
        gateway.approve_deletion.assert_called_once_with(selected_identifiers)

    @pytest.mark.usefixtures("request_deletion_of_present_entities")
    def test_if_entities_that_had_their_deletion_not_requested_are_deleted(
        self, identifiers, selected_identifiers, repo
    ):
        repo.delete(identifiers)
        assert repo.identifiers == selected_identifiers

    def test_if_runtime_error_is_raised_before_deletion(self, identifiers, repo, link):
        link.present_in_local_repo.return_value = True
        try:
            repo.delete(identifiers)
        except RuntimeError:
            pass
        assert repo.identifiers == identifiers


def test_repr(repo):
    assert repr(repo) == "OutboundRepository()"
