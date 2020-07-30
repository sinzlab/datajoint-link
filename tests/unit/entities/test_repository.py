from unittest.mock import call

import pytest

from link.entities.repository import Entity, Repository, RepositoryFactory
from link.entities.contents import Contents
from link.entities.flag_manager import FlagManagerFactory
from link.entities.transaction_manager import TransactionManager


class TestEntity:
    @pytest.fixture
    def flags(self):
        return dict(flag=True)

    def test_if_identifier_is_set_as_instance_attribute(self, identifier):
        assert Entity(identifier).identifier == identifier

    def test_if_data_instance_attribute_is_initialized_to_none(self, identifier):
        assert Entity(identifier).data is None

    def test_if_flags_are_set_as_instance_attribute(self, identifier, flags):
        assert Entity(identifier, flags=flags).flags == flags

    def test_if_flags_instance_attribute_is_set_to_empty_dict_if_no_flags_are_provided(self, identifier):
        assert Entity(identifier).flags == dict()

    def test_repr(self, identifier, flags):
        assert repr(Entity(identifier, flags=flags)) == "Entity(identifier='identifier0', flags={'flag': True})"


class TestRepositoryFactory:
    @pytest.fixture
    def storage(self):
        return dict()

    @pytest.fixture
    def factory(self, gateway_spy, storage):
        return RepositoryFactory(gateway_spy, storage)

    def test_if_gateway_is_stored_as_instance_attribute(self, factory, gateway_spy):
        assert factory.gateway is gateway_spy

    def test_if_storage_is_stored_as_instance_attribute(self, factory, storage):
        assert factory.storage is storage

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

    def test_if_storage_of_contents_is_correct(self, factory, storage):
        assert factory().contents.storage is storage

    def test_if_flag_manager_factory_is_assigned_to_flags_attribute_of_returned_repository(self, factory):
        assert isinstance(factory().flags, FlagManagerFactory)

    def test_if_entities_associated_with_contents_and_flag_manager_factory_are_identical(self, factory):
        repo = factory()
        assert all(entity is repo.flags.entities[identifier] for identifier, entity in repo.contents.items())

    def test_if_gateway_of_flag_manager_factory_is_correct(self, factory, gateway_spy):
        assert factory().flags.gateway is gateway_spy

    def test_if_transaction_manager_is_assigned_to_transaction_attribute_of_returned_repository(self, factory):
        assert isinstance(factory().transaction, TransactionManager)

    def test_if_entities_associated_with_contents_and_transaction_manager_are_identical(self, factory):
        repo = factory()
        assert all(entity is repo.transaction.entities[identifier] for identifier, entity in repo.contents.items())

    def test_if_gateway_of_transaction_manager_is_correct(self, factory, gateway_spy):
        assert factory().transaction.gateway is gateway_spy
