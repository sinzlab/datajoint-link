from unittest.mock import MagicMock, call

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
def local_repo():
    name = "local_repo"
    local_repo = MagicMock(name=name)
    local_repo.__contains__ = MagicMock(name=name + ".__contains__", return_value=False)
    local_repo.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return local_repo


@pytest.fixture
def outbound_repo(outbound_repo_cls, address, local_repo):
    return outbound_repo_cls(address, local_repo)


def test_if_local_repository_is_stored_as_instance_attribute(local_repo, outbound_repo):
    assert outbound_repo.local_repo is local_repo


class TestDelete:
    @pytest.fixture
    def selected_entities(self, entities, selected_identifiers):
        return [entity for entity in entities if entity.identifier in selected_identifiers]

    def test_if_presence_of_entities_in_local_repository_is_checked(self, identifiers, local_repo, outbound_repo):
        outbound_repo.delete(identifiers)
        assert local_repo.__contains__.mock_calls == [call(identifier) for identifier in identifiers]

    def test_if_runtime_error_is_raised_if_one_or_more_entities_are_present_in_local_repository(
        self, identifiers, local_repo, outbound_repo
    ):
        local_repo.__contains__.return_value = True
        with pytest.raises(RuntimeError):
            outbound_repo.delete(identifiers)

    @pytest.mark.usefixtures("request_deletion_of_present_entities")
    def test_if_entities_that_had_their_deletion_requested_have_it_granted(
        self, identifiers, outbound_repo, selected_entities
    ):
        outbound_repo.delete(identifiers)
        assert all(entity.deletion_approved is True for entity in selected_entities)

    @pytest.mark.usefixtures("request_deletion_of_present_entities")
    def test_if_entities_that_had_their_deletion_not_requested_are_deleted(
        self, identifiers, selected_identifiers, outbound_repo
    ):
        outbound_repo.delete(identifiers)
        assert outbound_repo.identifiers == selected_identifiers

    def test_if_runtime_error_is_raised_before_deletion(self, identifiers, local_repo, outbound_repo):
        local_repo.__contains__.return_value = True
        try:
            outbound_repo.delete(identifiers)
        except RuntimeError:
            pass
        assert outbound_repo.identifiers == identifiers


def test_repr(outbound_repo):
    assert repr(outbound_repo) == "OutboundRepository(address, local_repo)"
