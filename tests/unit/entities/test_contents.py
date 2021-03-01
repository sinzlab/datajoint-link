from unittest.mock import create_autospec

import pytest

from dj_link.base import Base
from dj_link.entities.contents import Contents
from dj_link.entities.repository import TransferEntity


@pytest.fixture
def contents(entities, gateway_spy):
    return Contents(entities, gateway_spy)


def test_if_contents_is_subclass_of_base():
    assert issubclass(Contents, Base)


class TestInit:
    def test_if_entities_are_stored_as_instance_attribute(self, contents, entities):
        assert contents.entities is entities

    def test_if_gateway_is_stored_as_instance_attribute(self, contents, gateway_spy):
        assert contents.gateway is gateway_spy


class TestGetItem:
    @pytest.fixture
    def fetched_entity(self, identifier, contents):
        return contents[identifier]

    @pytest.mark.usefixtures("fetched_entity")
    def test_if_data_is_fetched_from_gateway(self, identifier, gateway_spy):
        gateway_spy.fetch.assert_called_once_with(identifier)

    @pytest.mark.usefixtures("fetched_entity")
    def test_if_transfer_entity_is_created_from_entity(self, entity):
        entity.create_transfer_entity.assert_called_once_with("data")

    def test_if_transfer_entity_is_returned(self, entity, fetched_entity):
        assert fetched_entity is entity.create_transfer_entity.return_value


class TestSetItem:
    @pytest.fixture
    def identifier(self):
        return "identifier"

    @pytest.fixture
    def transfer_entity(self, identifier):
        return create_autospec(TransferEntity, instance=True, identifier=identifier, data="data")

    @pytest.fixture
    def insert_entity(self, identifier, transfer_entity, contents):
        contents[identifier] = transfer_entity

    @pytest.mark.usefixtures("insert_entity")
    def test_if_data_is_inserted_into_gateway(self, gateway_spy):
        gateway_spy.insert.assert_called_once_with("data")

    @pytest.mark.usefixtures("insert_entity")
    def test_if_entity_is_created_from_transfer_entity(self, transfer_entity):
        transfer_entity.create_entity.assert_called_once_with()

    @pytest.mark.usefixtures("insert_entity")
    def test_if_entity_is_added_to_contents(self, contents, identifier, transfer_entity):
        assert contents.entities[identifier] is transfer_entity.create_entity.return_value

    def test_if_data_is_inserted_into_gateway_before_entity_is_added_to_contents(
        self, gateway_spy, contents, identifier, transfer_entity
    ):
        gateway_spy.insert.side_effect = RuntimeError
        try:
            contents[identifier] = transfer_entity
        except RuntimeError:
            pass
        with pytest.raises(KeyError):
            _ = contents[identifier]


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
