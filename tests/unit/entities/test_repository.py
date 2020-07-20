from unittest.mock import MagicMock

import pytest

from link.entities.repository import Entity, Repository


class TestEntity:
    @pytest.fixture
    def flags(self):
        return dict(flag=True)

    def test_if_identifier_is_set_as_instance_attribute(self, identifier):
        assert Entity(identifier).identifier == identifier

    def test_if_flags_are_set_as_instance_attribute(self, identifier, flags):
        assert Entity(identifier, flags=flags).flags == flags

    def test_if_flags_instance_attribute_is_set_to_empty_dict_if_no_flags_are_provided(self, identifier):
        assert Entity(identifier).flags == dict()

    def test_repr(self, identifier, flags):
        assert repr(Entity(identifier, flags=flags)) == "Entity(identifier='identifier0', flags={'flag': True})"


@pytest.fixture
def storage_spy(data):
    name = "storage_spy"
    storage_spy = MagicMock(name=name)
    storage_spy.__getitem__.return_value = data
    storage_spy.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return storage_spy


@pytest.fixture
def repo(entities, gateway_spy, storage_spy):
    return Repository(entities, gateway_spy, storage_spy)


class TestInit:
    def test_if_entities_are_stored_as_instance_attribute(self, repo, entities):
        assert repo.entities is entities

    def test_if_gateway_is_stored_as_instance_attribute(self, repo, gateway_spy):
        assert repo.gateway is gateway_spy

    def test_if_storage_is_stored_as_instance_attribute(self, repo, storage_spy):
        assert repo.storage is storage_spy


class TestGetItem:
    @pytest.fixture
    def fetched_entity(self, identifier, repo):
        return repo[identifier]

    @pytest.mark.usefixtures("fetched_entity")
    def test_if_data_is_fetched_from_gateway(self, identifier, gateway_spy):
        gateway_spy.fetch.assert_called_once_with(identifier)

    @pytest.mark.usefixtures("fetched_entity")
    def test_if_data_is_stored_in_storage(self, identifier, data, storage_spy):
        storage_spy.__setitem__.assert_called_once_with(identifier, data)

    def test_if_entity_is_returned(self, entity, fetched_entity):
        assert fetched_entity is entity


class TestSetItem:
    @pytest.fixture
    def new_identifier(self):
        return "new_identifier"

    @pytest.fixture
    def new_entity(self, new_identifier):
        return Entity(new_identifier)

    @pytest.fixture
    def insert_entity(self, new_identifier, new_entity, repo):
        repo[new_identifier] = new_entity

    @pytest.mark.usefixtures("insert_entity")
    def test_if_data_is_retrieved_from_storage(self, new_identifier, storage_spy):
        storage_spy.__getitem__.assert_called_once_with(new_identifier)

    @pytest.mark.usefixtures("insert_entity")
    def test_if_data_is_inserted_into_gateway(self, new_identifier, data, gateway_spy):
        gateway_spy.insert.assert_called_once_with(new_identifier, data)

    @pytest.mark.usefixtures("insert_entity")
    def test_if_entity_is_added_to_repository(self, repo, new_identifier, new_entity):
        assert repo.entities[new_identifier] is new_entity

    def test_if_data_is_inserted_into_gateway_before_entity_is_added_to_repository(
        self, gateway_spy, repo, new_identifier, new_entity
    ):
        gateway_spy.insert.side_effect = RuntimeError
        try:
            repo[new_identifier] = new_entity
        except RuntimeError:
            pass
        with pytest.raises(KeyError):
            _ = repo[new_identifier]


class TestDelItem:
    @pytest.fixture
    def delete_entity(self, identifier, repo):
        del repo[identifier]

    @pytest.mark.usefixtures("delete_entity")
    def test_if_entity_is_deleted_in_gateway(self, identifier, gateway_spy):
        gateway_spy.delete.assert_called_once_with(identifier)

    @pytest.mark.usefixtures("delete_entity")
    def test_if_entity_is_deleted_from_repository(self, identifier, repo):
        with pytest.raises(KeyError):
            _ = repo.entities[identifier]

    def test_if_entity_is_deleted_from_gateway_before_being_deleted_from_repository(
        self, gateway_spy, repo, identifier, entity
    ):
        gateway_spy.delete.side_effect = RuntimeError
        try:
            del repo[identifier]
        except RuntimeError:
            pass
        assert repo.entities[identifier] is entity


def test_iter(repo, identifiers):
    assert all(identifier0 == identifier1 for identifier0, identifier1 in zip(repo, identifiers))


def test_len(repo):
    assert len(repo) == 10


def test_repr(repo, entities):
    assert repr(repo) == f"Repository(entities={entities}, gateway=gateway_spy, storage=storage_spy)"
