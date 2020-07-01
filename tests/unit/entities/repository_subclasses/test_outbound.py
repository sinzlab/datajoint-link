from unittest.mock import call

import pytest

from link.entities import outbound
from link.entities import repository


def test_if_outbound_repository_is_subclass_of_repository():
    assert issubclass(outbound.OutboundRepository, repository.Repository)


@pytest.fixture
def outbound_repo_cls(configure_repo_cls):
    class OutboundRepository(outbound.OutboundRepository):
        pass

    configure_repo_cls(OutboundRepository)
    return OutboundRepository


@pytest.fixture
def outbound_repo(address, outbound_repo_cls):
    outbound_repo = outbound_repo_cls()
    outbound_repo.address = address
    return outbound_repo


def test_if_link_is_none(outbound_repo):
    assert outbound_repo.link is None


@pytest.fixture
def link(link):
    link.present_in_local_repo.return_value = False
    return link


@pytest.fixture
def add_link(outbound_repo, link):
    outbound_repo.link = link


@pytest.mark.usefixtures("add_link")
class TestDelete:
    @pytest.fixture
    def selected_entities(self, entities, selected_identifiers):
        return [entity for entity in entities if entity.identifier in selected_identifiers]

    def test_if_presence_of_entities_in_local_repository_is_checked(self, identifiers, outbound_repo, link):
        outbound_repo.delete(identifiers)
        assert link.present_in_local_repo.mock_calls == [call(identifier) for identifier in identifiers]

    def test_if_runtime_error_is_raised_if_one_or_more_entities_are_present_in_local_repository(
        self, identifiers, link, outbound_repo
    ):
        link.present_in_local_repo.return_value = True
        with pytest.raises(RuntimeError):
            outbound_repo.delete(identifiers)

    @pytest.mark.usefixtures("request_deletion_of_present_entities")
    def test_if_entities_that_had_their_deletion_requested_have_it_approved(
        self, identifiers, outbound_repo, selected_entities
    ):
        outbound_repo.delete(identifiers)
        assert all(entity.deletion_approved is True for entity in selected_entities)

    @pytest.mark.usefixtures("request_deletion_of_present_entities")
    def test_if_entities_that_had_their_deletion_requested_have_it_approved_in_gateway(
        self, identifiers, gateway, outbound_repo, selected_identifiers
    ):
        outbound_repo.delete(identifiers)
        gateway.approve_deletion.assert_called_once_with(selected_identifiers)

    @pytest.mark.usefixtures("request_deletion_of_present_entities")
    def test_if_entities_that_had_their_deletion_not_requested_are_deleted(
        self, identifiers, selected_identifiers, outbound_repo
    ):
        outbound_repo.delete(identifiers)
        assert outbound_repo.identifiers == selected_identifiers

    def test_if_runtime_error_is_raised_before_deletion(self, identifiers, outbound_repo, link):
        link.present_in_local_repo.return_value = True
        try:
            outbound_repo.delete(identifiers)
        except RuntimeError:
            pass
        assert outbound_repo.identifiers == identifiers


def test_repr(outbound_repo):
    assert repr(outbound_repo) == "OutboundRepository()"
