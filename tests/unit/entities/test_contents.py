from unittest.mock import MagicMock

import pytest

from link.entities.contents import Contents
from link.entities.repository import Entity


@pytest.fixture
def storage_spy(entity_data):
    name = "storage_spy"
    storage_spy = MagicMock(name=name)
    storage_spy.__getitem__.return_value = entity_data
    storage_spy.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return storage_spy


@pytest.fixture
def contents(entities, gateway_spy, storage_spy):
    return Contents(entities, gateway_spy, storage_spy)


class TestInit:
    def test_if_entities_are_stored_as_instance_attribute(self, contents, entities):
        assert contents.entities is entities

    def test_if_gateway_is_stored_as_instance_attribute(self, contents, gateway_spy):
        assert contents.gateway is gateway_spy

    def test_if_storage_is_stored_as_instance_attribute(self, contents, storage_spy):
        assert contents.storage is storage_spy


class TestGetItem:
    @pytest.fixture
    def fetched_entity(self, identifier, contents):
        return contents[identifier]

    @pytest.mark.usefixtures("fetched_entity")
    def test_if_data_is_fetched_from_gateway(self, identifier, gateway_spy):
        gateway_spy.fetch.assert_called_once_with(identifier)

    @pytest.mark.usefixtures("fetched_entity")
    def test_if_data_is_stored_in_storage(self, identifier, entity_data, storage_spy):
        storage_spy.__setitem__.assert_called_once_with(identifier, entity_data)

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
    def insert_entity(self, new_identifier, new_entity, contents):
        contents[new_identifier] = new_entity

    @pytest.mark.usefixtures("insert_entity")
    def test_if_data_is_retrieved_from_storage(self, new_identifier, storage_spy):
        storage_spy.__getitem__.assert_called_once_with(new_identifier)

    @pytest.mark.usefixtures("insert_entity")
    def test_if_data_is_inserted_into_gateway(self, new_identifier, entity_data, gateway_spy):
        gateway_spy.insert.assert_called_once_with(new_identifier, entity_data)

    @pytest.mark.usefixtures("insert_entity")
    def test_if_entity_is_added_to_contents(self, contents, new_identifier, new_entity):
        assert contents.entities[new_identifier] is new_entity

    def test_if_data_is_inserted_into_gateway_before_entity_is_added_to_contents(
        self, gateway_spy, contents, new_identifier, new_entity
    ):
        gateway_spy.insert.side_effect = RuntimeError
        try:
            contents[new_identifier] = new_entity
        except RuntimeError:
            pass
        with pytest.raises(KeyError):
            _ = contents[new_identifier]


class TestDelItem:
    @pytest.fixture
    def delete_entity(self, identifier, contents):
        del contents[identifier]

    @pytest.mark.usefixtures("delete_entity")
    def test_if_entity_is_deleted_in_gateway(self, identifier, gateway_spy):
        gateway_spy.delete.assert_called_once_with(identifier)

    @pytest.mark.usefixtures("delete_entity")
    def test_if_entity_is_deleted_from_contents(self, identifier, contents):
        with pytest.raises(KeyError):
            _ = contents.entities[identifier]

    def test_if_entity_is_deleted_from_gateway_before_being_deleted_from_contents(
        self, gateway_spy, contents, identifier, entity
    ):
        gateway_spy.delete.side_effect = RuntimeError
        try:
            del contents[identifier]
        except RuntimeError:
            pass
        assert contents.entities[identifier] is entity


def test_iter(contents, identifiers):
    assert all(identifier0 == identifier1 for identifier0, identifier1 in zip(contents, identifiers))


def test_len(contents):
    assert len(contents) == 10


def test_repr(contents, entities):
    assert repr(contents) == f"Contents(entities={entities}, gateway=gateway_spy, storage=storage_spy)"
