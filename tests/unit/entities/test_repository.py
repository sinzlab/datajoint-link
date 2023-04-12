from collections.abc import MutableMapping
from dataclasses import is_dataclass
from unittest.mock import MagicMock, create_autospec

import pytest

from dj_link.base import Base
from dj_link.entities.abstract_gateway import AbstractEntityDTO
from dj_link.entities.contents import Contents
from dj_link.entities.flag_manager import FlagManagerFactory
from dj_link.entities.repository import Entity, EntityFactory, Repository, RepositoryFactory, TransferEntity


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


class TestEntityFactory:
    @pytest.fixture
    def factory(self, gateway_spy):
        return EntityFactory(gateway_spy)

    def test_if_subclass_of_base(self):
        assert issubclass(EntityFactory, Base)

    def test_if_gateway_is_stored_as_instance_attribute(self, factory, gateway_spy):
        assert factory.gateway is gateway_spy

    def test_if_flags_are_retrieved_from_gateway(self, identifier, factory, gateway_spy):
        _ = factory(identifier)
        gateway_spy.get_flags.assert_called_once_with(identifier)

    def test_if_correct_entity_is_returned(self, factory, identifier, flags):
        assert factory(identifier) == Entity(identifier, flags[identifier])


class TestRepository:
    @pytest.fixture
    def contents_spy(self):
        spy = create_autospec(Contents, instance=True)
        spy.__iter__.return_value = "iterator"
        spy.__len__.return_value = 10
        return spy

    @pytest.fixture
    def flag_manager_factory_spy(self):
        return create_autospec(FlagManagerFactory, instance=True)

    @pytest.fixture
    def repo(self, contents_spy, flag_manager_factory_spy, gateway_spy):
        return Repository(contents_spy, flag_manager_factory_spy, gateway_spy)

    def test_if_subclass_of_mutable_mapping(self):
        assert issubclass(Repository, MutableMapping)

    def test_if_subclass_of_base(self):
        assert issubclass(Repository, Base)

    def test_if_contents_are_stored_as_instance_attribute(self, repo, contents_spy):
        assert repo.contents is contents_spy

    def test_if_flag_manager_factory_is_stored_as_instance_attribute(self, repo, flag_manager_factory_spy):
        assert repo.flags is flag_manager_factory_spy

    def test_if_gateway_is_stored_as_instance_attribute(self, repo, gateway_spy):
        assert repo.gateway is gateway_spy

    def test_if_entity_is_retrieved_from_contents(self, identifier, repo, contents_spy):
        _ = repo[identifier]
        contents_spy.__getitem__.assert_called_once_with(identifier)

    def test_if_entity_is_returned(self, identifier, repo, contents_spy):
        assert repo[identifier] is contents_spy.__getitem__.return_value

    def test_if_entity_is_added_to_contents(self, identifier, repo, contents_spy):
        dummy_transfer_entity = MagicMock()
        repo[identifier] = dummy_transfer_entity
        contents_spy.__setitem__.assert_called_once_with(identifier, dummy_transfer_entity)

    def test_if_entity_is_deleted_from_contents(self, identifier, repo, contents_spy):
        del repo[identifier]
        contents_spy.__delitem__.assert_called_once_with(identifier)

    def test_iterator_is_created_from_contents(self, repo, contents_spy):
        iter(repo)
        contents_spy.__iter__.assert_called_once_with()

    def test_if_iterator_is_returned(self, repo, contents_spy):
        assert "".join(iter(repo)) == "iterator"

    def test_if_length_of_contents_is_checked(self, repo, contents_spy):
        len(repo)
        contents_spy.__len__.assert_called_once_with()

    def test_if_length_of_contents_is_returned(self, repo, contents_spy):
        assert len(repo) == 10

    def test_if_transaction_is_started_in_gateway(self, repo, gateway_spy):
        with repo.transaction():
            pass
        gateway_spy.start_transaction.assert_called_once_with()

    def test_if_transaction_is_committed_in_gateway(self, repo, gateway_spy):
        with repo.transaction():
            pass
        gateway_spy.commit_transaction.assert_called_once_with()

    def test_if_transaction_is_cancelled_in_gateway_after_runtime_error_is_raised(self, repo, gateway_spy):
        with repo.transaction():
            raise RuntimeError
        gateway_spy.cancel_transaction.assert_called_once_with()

    def test_if_none_is_returned_by_context_manager(self, repo):
        with repo.transaction() as returned:
            assert returned is None


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

    def test_if_repository_is_returned(self, factory):
        assert isinstance(factory(), Repository)

    def test_if_contents_is_assigned_to_contents_attribute_of_returned_repository(self, factory):
        assert isinstance(factory().contents, Contents)

    def test_if_gateway_of_contents_is_correct(self, factory, gateway_spy):
        assert factory().contents.gateway is gateway_spy

    def test_if_entity_factory_instance_is_assigned_to_contents(self, factory):
        assert isinstance(factory().contents.entity_factory, EntityFactory)

    def test_if_gateway_of_entity_factory_of_contents_is_correct(self, factory, gateway_spy):
        assert factory().contents.entity_factory.gateway is gateway_spy

    def test_if_flag_manager_factory_is_assigned_to_flags_attribute_of_returned_repository(self, factory):
        assert isinstance(factory().flags, FlagManagerFactory)

    def test_if_same_entity_factory_instance_is_assigned_to_flag_manager_factory(self, factory):
        repo = factory()
        assert repo.flags.entity_factory is repo.contents.entity_factory

    def test_if_gateway_of_flag_manager_factory_is_correct(self, factory, gateway_spy):
        assert factory().flags.gateway is gateway_spy

    def test_if_gateway_is_assigned_to_gateway_attribute_of_returned_repository(self, factory, gateway_spy):
        assert factory().gateway is gateway_spy
