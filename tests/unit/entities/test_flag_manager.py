import pytest

from link.entities import flag_manager


class TestEntityFlagsManager:
    @pytest.fixture
    def flags(self):
        return dict(flag1=True, flag2=False)

    @pytest.fixture
    def entity(self, entity, flags):
        entity.flags.update(flags)
        return entity

    @pytest.fixture
    def manager(self, entity, gateway_spy):
        return flag_manager.EntityFlagsManager(entity, gateway_spy)

    def test_if_delitem_is_none(self):
        assert flag_manager.EntityFlagsManager.__delitem__ is None

    def test_if_entity_is_stored_as_instance_attribute(self, entity, manager):
        assert manager.entity is entity

    def test_if_gateway_is_stored_as_instance_attribute(self, gateway_spy, manager):
        assert manager.gateway is gateway_spy

    def test_if_flag_is_returned(self, manager, flags):
        assert all(manager[flag] is value for flag, value in flags.items())

    def test_if_flag_is_set_in_gateway(self, manager, gateway_spy):
        manager["flag3"] = True
        gateway_spy.set_flag.assert_called_once_with("flag3", True)

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

    def test_iter(self, manager, flags):
        assert all(flag1 == flag2 for flag1, flag2 in zip(manager, list(flags)))

    def test_len(self, manager):
        assert len(manager) == 2

    def test_repr(self, manager, entity):
        assert repr(manager) == f"EntityFlagsManager(entity={entity}, gateway=gateway_spy)"
