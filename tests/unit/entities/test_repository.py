from unittest.mock import MagicMock

import pytest

from link.entities.repository import Repository


@pytest.fixture
def address():
    return MagicMock(name="address")


@pytest.fixture
def identifiers():
    return ["ID" + str(i) for i in range(10)]


@pytest.fixture
def gateway(identifiers):
    gateway = MagicMock(name="gateway")
    gateway.get_identifiers.return_value = identifiers
    return gateway


@pytest.fixture
def entity_cls():
    class EntityClass:
        def __init__(self, address, identifier):
            self.address = address
            self.identifier = identifier

        def __eq__(self, other):
            return self.address == other.address and self.identifier == other.identifier

    return EntityClass


@pytest.fixture
def entities(address, entity_cls, identifiers):
    return [entity_cls(address, i) for i in identifiers]


@pytest.fixture
def repository_cls(gateway, entity_cls):
    Repository.gateway = gateway
    Repository.entity_cls = entity_cls
    return Repository


@pytest.fixture
def repository(repository_cls, address):
    return repository_cls(address)


class TestInit:
    def test_if_address_is_stored_as_instance_attribute(self, repository, address):
        assert repository.address is address

    @pytest.mark.usefixtures("repository")
    def test_if_gateway_gets_called_correctly(self, gateway):
        gateway.get_identifiers.assert_called_once_with()


class TestList:
    def test_if_correct_identifiers_are_returned(self, repository, identifiers):
        assert repository.list() == identifiers


@pytest.fixture
def indexes():
    return 0, 4


@pytest.fixture
def selected_identifiers(identifiers, indexes):
    return [identifiers[i] for i in indexes]


class TestFetch:
    @pytest.fixture
    def fetched_entities(self, repository, selected_identifiers):
        return repository.fetch(selected_identifiers)

    @pytest.mark.usefixtures("fetched_entities")
    def test_if_gateway_is_correctly_called(self, gateway, selected_identifiers):
        gateway.fetch.assert_called_once_with(selected_identifiers)

    def test_if_correct_entities_are_fetched(self, entities, indexes, fetched_entities):
        expected_entities = [entities[index] for index in indexes]
        assert fetched_entities == expected_entities

    def test_if_getting_non_existing_entity_raises_error(self, repository):
        with pytest.raises(KeyError):
            repository.fetch("ID999")

    def test_if_error_is_raised_before_gateway_is_called(self, repository, gateway):
        try:
            repository.fetch("ID999")
        except KeyError:
            gateway.fetch.assert_not_called()


class TestDelete:
    @pytest.fixture
    def remaining_identifiers(self, identifiers, selected_identifiers):
        return [i for i in identifiers if i not in selected_identifiers]

    def test_if_gateway_is_correctly_called(self, repository, gateway, selected_identifiers):
        repository.delete(selected_identifiers)
        gateway.delete.assert_called_once_with(selected_identifiers)

    def test_correct_entities_are_deleted(self, repository, selected_identifiers, remaining_identifiers):
        repository.delete(selected_identifiers)
        assert repository.list() == remaining_identifiers

    def test_if_deleting_non_existing_entity_raises_error(self, repository):
        with pytest.raises(KeyError):
            repository.delete("ID999")

    def test_if_error_is_raised_before_gateway_is_called(self, repository, gateway):
        try:
            repository.delete("ID999")
        except KeyError:
            gateway.delete.assert_not_called()

    def test_if_repository_is_rolled_back_if_delete_fails_in_gateway(
        self, repository, identifiers, gateway, selected_identifiers
    ):
        gateway.delete.side_effect = RuntimeError
        repository.delete(selected_identifiers)
        assert repository.list() == identifiers


class TestContains:
    def test_if_not_in_is_true_if_entity_is_not_contained(self, repository):
        assert "ID999" not in repository

    def test_if_in_is_true_if_entity_is_contained(self, repository, identifiers):
        assert identifiers[0] in repository
