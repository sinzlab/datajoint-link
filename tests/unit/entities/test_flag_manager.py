import pytest

from dj_link.base import Base
from dj_link.entities import flag_manager


class TestFlagManager:
    @pytest.fixture
    def entity_flags(self, entity):
        return entity.flags

    @pytest.fixture
    def manager(self, entity, gateway_spy):
        return flag_manager.FlagManager(entity, gateway_spy)

    def test_if_subclass_of_base(self):
        assert issubclass(flag_manager.FlagManager, Base)

    def test_if_entity_is_stored_as_instance_attribute(self, entity, manager):
        assert manager.entity is entity

    def test_if_gateway_is_stored_as_instance_attribute(self, gateway_spy, manager):
        assert manager.gateway is gateway_spy

    def test_if_flag_is_returned(self, manager, entity_flags):
        assert all(manager[flag] is value for flag, value in entity_flags.items())

    def test_if_flag_is_set_in_gateway(self, manager, identifier, gateway_spy):
        manager["flag3"] = True
        gateway_spy.set_flag.assert_called_once_with(identifier, "flag3", True)

    def test_if_flag_is_set_in_entity(self, manager, entity):
        manager["flag3"] = True
        assert entity.flags["flag3"] is True

    def test_if_flag_is_being_set_in_gateway_before_being_set_in_entity(self, manager, entity, gateway_spy):
        gateway_spy.set_flag.side_effect = RuntimeError
        try:
            manager["flag3"] = True
        except RuntimeError:
            pass
        with pytest.raises(KeyError):
            _ = entity.flags["flag3"]

    def test_if_trying_to_delete_flag_raises_not_implemented_error(self, manager):
        with pytest.raises(NotImplementedError):
            del manager["flag3"]

    def test_iter(self, manager, entity_flags):
        assert all(flag1 == flag2 for flag1, flag2 in zip(manager, list(entity_flags)))

    def test_len(self, manager):
        assert len(manager) == 2


class TestFlagManagerFactory:
    @pytest.fixture
    def factory(self, entities, gateway_spy):
        return flag_manager.FlagManagerFactory(entities, gateway_spy)

    def test_if_subclass_of_base(self):
        assert issubclass(flag_manager.FlagManagerFactory, Base)

    def test_if_entities_are_stored_as_instance_attribute(self, factory, entities):
        assert factory.entities is entities

    def test_if_gateway_is_stored_as_instance_attribute(self, factory, gateway_spy):
        assert factory.gateway is gateway_spy

    def test_if_entity_flags_manager_is_returned(self, factory, identifier):
        assert isinstance(factory[identifier], flag_manager.FlagManager)

    def test_if_entity_of_returned_entity_flags_manager_is_correct(self, factory, identifier, entity):
        assert factory[identifier].entity is entity

    def test_if_gateway_of_returned_entity_flags_manager_is_correct(self, factory, identifier, gateway_spy):
        assert factory[identifier].gateway is gateway_spy

    def test_if_entity_flags_managers_are_returned_when_iterating(self, factory):
        assert all(isinstance(manager, flag_manager.FlagManager) for manager in factory)

    def test_if_entity_of_returned_entity_flags_managers_are_correct_when_iterating(self, factory, entities):
        assert all(manager.entity is entity for manager, entity in zip(factory, entities.values()))

    def test_if_gateway_of_returned_entity_flags_managers_are_correct_when_iterating(self, factory, gateway_spy):
        assert all(manager.gateway is gateway_spy for manager in factory)

    def test_len(self, factory):
        assert len(factory) == 10
