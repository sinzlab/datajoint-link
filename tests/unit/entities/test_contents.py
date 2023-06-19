from unittest.mock import MagicMock, create_autospec

import pytest

from dj_link.base import Base
from dj_link.entities.contents import Contents
from dj_link.entities.repository import TransferEntity


@pytest.fixture()
def entity_factory_spy(entity):
    return MagicMock(name="entity_factory_spy", return_value=entity)


@pytest.fixture()
def contents(entities, gateway_spy, entity_factory_spy):
    return Contents(gateway_spy, entity_factory_spy)


def test_if_contents_is_subclass_of_base():
    assert issubclass(Contents, Base)


class TestInit:
    def test_if_gateway_is_stored_as_instance_attribute(self, contents, gateway_spy):
        assert contents.gateway is gateway_spy

    def test_if_entity_factory_is_stored_as_instance_attribute(self, contents, entity_factory_spy):
        assert contents.entity_factory is entity_factory_spy


class TestGetItem:
    @pytest.fixture()
    def fetched_entity(self, identifier, contents):
        return contents[identifier]

    @pytest.mark.usefixtures("fetched_entity")
    def test_if_call_to_entity_factory_is_correct(self, entity_factory_spy, identifier):
        entity_factory_spy.assert_called_once_with(identifier)

    @pytest.mark.usefixtures("fetched_entity")
    def test_if_data_is_fetched_from_gateway(self, identifier, gateway_spy):
        gateway_spy.fetch.assert_called_once_with(identifier)

    @pytest.mark.usefixtures("fetched_entity")
    def test_if_transfer_entity_is_created_from_entity(self, entity):
        entity.create_transfer_entity.assert_called_once_with("data")

    def test_if_transfer_entity_is_returned(self, entity, fetched_entity):
        assert fetched_entity is entity.create_transfer_entity.return_value


def test_if_data_is_inserted_into_gateway(identifier, contents, gateway_spy):
    transfer_entity = create_autospec(TransferEntity, instance=True, identifier=identifier, data="data")
    contents[identifier] = transfer_entity
    gateway_spy.insert.assert_called_once_with("data")


def test_if_entity_is_deleted_in_gateway(contents, identifier, gateway_spy):
    del contents[identifier]
    gateway_spy.delete.assert_called_once_with(identifier)


class TestLen:
    def test_if_call_to_len_method_of_gateway_is_correct(self, contents, gateway_spy):
        len(contents)
        gateway_spy.__len__.assert_called_once_with()

    def test_if_correct_length_is_returned(self, contents, gateway_spy):
        assert len(contents) == 10


class TestIter:
    def test_if_call_to_iter_method_of_gateway_is_correct(self, contents, gateway_spy):
        iter(contents)
        gateway_spy.__iter__.assert_called_once_with()

    def test_correct_values_are_returned(self, contents):
        assert list(iter(contents)) == list(iter("iterator"))
