import dataclasses
from unittest.mock import MagicMock, call
from abc import ABC

import pytest

from link.entities import domain


class TestAddress:
    def test_if_address_is_dataclass(self):
        assert dataclasses.is_dataclass(domain.Address)

    def test_if_address_is_frozen(self):
        address = domain.Address("host", "database", "table")
        with pytest.raises(dataclasses.FrozenInstanceError):
            # noinspection PyDataclass
            address.host = "host2"


class TestEntity:
    def test_if_entity_is_dataclass(self):
        assert dataclasses.is_dataclass(domain.Entity)


class TestFlaggedEntity:
    def test_if_dataclass(self):
        assert dataclasses.is_dataclass(domain.FlaggedEntity)

    def test_if_subclass_of_entity(self):
        assert issubclass(domain.FlaggedEntity, domain.Entity)


@pytest.fixture
def identifiers():
    return ["ID" + str(i) for i in range(3)]


@pytest.fixture
def entities(identifiers):
    return [MagicMock(name="entity_" + identifier) for identifier in identifiers]


@pytest.fixture
def entity_cls(entities):
    return MagicMock(name="entity_cls", side_effect=entities)


@pytest.fixture
def gateway(address, identifiers):
    return MagicMock(name="gateway", address=address, identifiers=identifiers)


@pytest.fixture
def entity_creator_cls(entity_creator_base_cls, gateway, entity_cls):
    class EntityCreator(entity_creator_base_cls):
        _entity_cls = None

        @property
        def entity_cls(self):
            return self._entity_cls

    EntityCreator.__name__ = entity_creator_base_cls.__name__
    EntityCreator.gateway = gateway
    EntityCreator._entity_cls = entity_cls
    return EntityCreator


@pytest.fixture
def entity_creator(entity_creator_cls):
    return entity_creator_cls()


class TestAbstractEntityCreator:
    @pytest.fixture
    def entity_creator_cls(self, gateway, entities):
        class EntityCreator(domain.AbstractEntityCreator):
            __qualname__ = "EntityCreator"
            entity_cls = None

            def _create_entities(self):
                return entities

        EntityCreator.gateway = gateway
        return EntityCreator

    def test_if_abstract_base_class(self, entity_creator_cls):
        assert issubclass(entity_creator_cls, ABC)

    def test_if_address_is_stored_as_instance_attribute(self, address, entity_creator):
        assert entity_creator.address == address

    def test_if_identifiers_are_stored_as_instance_attribute(self, identifiers, entity_creator):
        assert entity_creator.identifiers == identifiers

    def test_if_entities_are_returned_when_created(self, entities, entity_creator):
        assert entity_creator.create_entities() == entities

    def test_repr(self, entity_creator):
        assert repr(entity_creator) == "EntityCreator()"


class TestEntityCreator:
    def test_if_entity_class_is_entity(self):
        assert domain.EntityCreator.entity_cls is domain.Entity

    @pytest.fixture
    def entity_creator_base_cls(self):
        return domain.EntityCreator

    def test_if_entities_are_correctly_initialized(self, address, identifiers, entity_cls, entity_creator):
        entity_creator.create_entities()
        assert entity_cls.mock_calls == [call(address, identifier) for identifier in identifiers]

    def test_if_entities_are_returned(self, entities, entity_creator):
        assert entity_creator.create_entities() == entities


class TestFlaggedEntityCreator:
    def test_if_entity_class_is_flagged_entity(self):
        assert domain.FlaggedEntityCreator.entity_cls is domain.FlaggedEntity

    @pytest.fixture
    def deletion_requested_flags(self):
        return [True for _ in range(3)]

    @pytest.fixture
    def deletion_approved_flags(self):
        return [False for _ in range(3)]

    @pytest.fixture
    def gateway(self, gateway, deletion_requested_flags, deletion_approved_flags):
        gateway.deletion_requested_flags = deletion_requested_flags
        gateway.deletion_approved_flags = deletion_approved_flags
        return gateway

    @pytest.fixture
    def entity_creator_base_cls(self):
        return domain.FlaggedEntityCreator

    def test_if_abstract_entity_creator_is_initialized(self, gateway):
        class FakeAbstractEntityCreator(domain.AbstractEntityCreator, ABC):
            __init__ = MagicMock(name="FakeAbstractEntityCreator.__init__")

        class EntityCreator(domain.FlaggedEntityCreator, FakeAbstractEntityCreator):
            pass

        EntityCreator.gateway = gateway
        EntityCreator()
        FakeAbstractEntityCreator.__init__.assert_called_once_with()

    def test_if_deletion_requested_flags_are_stored_as_instance_attribute(
        self, entity_creator, deletion_requested_flags
    ):
        assert entity_creator.deletion_requested_flags == deletion_requested_flags

    def test_if_deletion_approved_flags_are_stored_as_instance_attribute(self, entity_creator, deletion_approved_flags):
        assert entity_creator.deletion_approved_flags == deletion_approved_flags

    def test_if_entities_are_correctly_initialized(
        self, address, identifiers, entity_cls, entity_creator, deletion_requested_flags, deletion_approved_flags
    ):
        entity_creator.create_entities()
        calls = []
        for identifier, deletion_requested_flag, deletion_approved_flag in zip(
            identifiers, deletion_requested_flags, deletion_approved_flags
        ):
            calls.append(call(address, identifier, deletion_requested_flag, deletion_approved_flag))
        assert entity_cls.mock_calls == calls
