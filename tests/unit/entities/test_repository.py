from unittest.mock import create_autospec, call
from dataclasses import is_dataclass

import pytest

from link.entities.abstract_gateway import AbstractEntityDTO
from link.entities.repository import Entity, TransferEntity, Repository, RepositoryFactory
from link.entities.contents import Contents
from link.entities.flag_manager import FlagManagerFactory
from link.entities.transaction_manager import TransactionManager
from link.base import Base


@pytest.fixture
def entity_dto_spy():
    return create_autospec(AbstractEntityDTO, instance=True)


class TestEntity:
    @pytest.fixture
    def entity(self, identifier, flags):
        return Entity(identifier, flags)

    def test_if_dataclass(self):
        assert is_dataclass(Entity)

    def test_if_identifier_is_set_as_instance_attribute(self, identifier, entity):
        assert entity.identifier == identifier

    def test_if_flags_are_set_as_instance_attribute(self, entity, flags):
        assert entity.flags is flags

    def test_if_create_transfer_entity_method_returns_correct_value(self, identifier, flags, entity_dto_spy, entity):
        assert entity.create_transfer_entity(entity_dto_spy) == TransferEntity(identifier, flags, entity_dto_spy)


class TestTransferEntity:
    @pytest.fixture
    def transfer_entity(self, identifier, flags, entity_dto_spy):
        return TransferEntity(identifier, flags, entity_dto_spy)

    def test_if_subclass_of_entity(self):
        assert issubclass(TransferEntity, Entity)

    def test_if_data_is_stored_as_instance_attribute(self, transfer_entity, entity_dto_spy):
        assert transfer_entity.data == entity_dto_spy

    def test_if_create_entity_returns_correct_value(self, identifier, flags, transfer_entity):
        assert transfer_entity.create_entity() == Entity(identifier, flags)

    def test_if_call_to_create_identifier_only_copy_method_of_entity_dto_is_correct(
        self, transfer_entity, entity_dto_spy
    ):
        transfer_entity.create_identifier_only_copy()
        entity_dto_spy.create_identifier_only_copy.assert_called_once_with()

    def test_if_create_identifier_only_copy_method_returns_correct_value(
        self, identifier, flags, entity_dto_spy, transfer_entity
    ):
        assert transfer_entity.create_identifier_only_copy() == TransferEntity(
            identifier, flags, entity_dto_spy.create_identifier_only_copy.return_value
        )


class TestRepositoryFactory:
    @pytest.fixture
    def storage(self):
        return dict()

    @pytest.fixture
    def factory(self, gateway_spy, storage):
        return RepositoryFactory(gateway_spy)

    def test_if_subclass_of_base(self):
        assert issubclass(RepositoryFactory, Base)

    def test_if_gateway_is_stored_as_instance_attribute(self, factory, gateway_spy):
        assert factory.gateway is gateway_spy

    def test_if_flags_are_retrieved_from_gateway(self, identifiers, factory, gateway_spy):
        _ = factory()
        assert gateway_spy.get_flags.call_args_list == [call(identifier) for identifier in identifiers]

    def test_if_repository_is_returned(self, factory):
        assert isinstance(factory(), Repository)

    def test_if_contents_is_assigned_to_contents_attribute_of_returned_repository(self, factory):
        assert isinstance(factory().contents, Contents)

    def test_if_entities_associated_with_contents_are_correct(self, factory, entities):
        assert factory().contents.entities == entities

    def test_if_gateway_of_contents_is_correct(self, factory, gateway_spy):
        assert factory().contents.gateway is gateway_spy

    def test_if_flag_manager_factory_is_assigned_to_flags_attribute_of_returned_repository(self, factory):
        assert isinstance(factory().flags, FlagManagerFactory)

    def test_if_entities_associated_with_contents_and_flag_manager_factory_are_identical(self, factory):
        repo = factory()
        assert all(entity is repo.flags.entities[identifier] for identifier, entity in repo.contents.entities.items())

    def test_if_gateway_of_flag_manager_factory_is_correct(self, factory, gateway_spy):
        assert factory().flags.gateway is gateway_spy

    def test_if_transaction_manager_is_assigned_to_transaction_attribute_of_returned_repository(self, factory):
        assert isinstance(factory().transaction, TransactionManager)

    def test_if_entities_associated_with_contents_and_transaction_manager_are_identical(self, factory):
        repo = factory()
        assert all(
            entity is repo.transaction.entities[identifier] for identifier, entity in repo.contents.entities.items()
        )

    def test_if_gateway_of_transaction_manager_is_correct(self, factory, gateway_spy):
        assert factory().transaction.gateway is gateway_spy
