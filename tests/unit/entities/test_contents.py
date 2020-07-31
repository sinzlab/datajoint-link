import pytest

from link.entities.contents import Contents
from link.entities.repository import Entity
from link.entities.representation import Base


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

    def test_if_entity_is_returned(self, entity, fetched_entity):
        assert fetched_entity is entity

    def test_if_data_is_attached_to_fetched_entity(self, fetched_entity):
        assert fetched_entity.data == "data"


class TestSetItem:
    @pytest.fixture
    def new_identifier(self):
        return "new_identifier"

    @pytest.fixture
    def new_entity(self, new_identifier):
        new_entity = Entity(new_identifier)
        new_entity.data = "data"
        return new_entity

    @pytest.fixture
    def insert_entity(self, new_identifier, new_entity, contents):
        contents[new_identifier] = new_entity

    @pytest.mark.usefixtures("insert_entity")
    def test_if_data_is_inserted_into_gateway(self, gateway_spy):
        gateway_spy.insert.assert_called_once_with("data")

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
