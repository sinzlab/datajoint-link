import dataclasses
from unittest.mock import MagicMock, call

import pytest

from link.entities import entity


class TestEntity:
    def test_if_dataclass(self):
        assert dataclasses.is_dataclass(entity.Entity)

    def test_if_identifier_attribute_is_present(self):
        assert entity.Entity("identifier").identifier == "identifier"


class TestManagedEntity:
    def test_if_dataclass(self):
        assert dataclasses.is_dataclass(entity.ManagedEntity)

    def test_if_subclass_of_entity(self):
        assert issubclass(entity.ManagedEntity, entity.Entity)

    def test_if_deletion_requested_attribute_is_present(self):
        assert entity.ManagedEntity("identifier", True).deletion_requested is True


class TestSourceEntity:
    def test_if_dataclass(self):
        assert dataclasses.is_dataclass(entity.SourceEntity)

    def test_if_subclass_of_entity(self):
        assert issubclass(entity.SourceEntity, entity.Entity)


class TestOutboundEntity:
    def test_if_dataclass(self):
        assert dataclasses.is_dataclass(entity.OutboundEntity)

    def test_if_subclass_of_managed_entity(self):
        assert issubclass(entity.OutboundEntity, entity.Entity)

    def test_if_deletion_approved_attribute_is_present(self):
        assert entity.OutboundEntity("identifier", True, True).deletion_approved is True


class TestLocalEntity:
    def test_if_dataclass(self):
        assert dataclasses.is_dataclass(entity.LocalEntity)

    def test_if_subclass_of_managed_entity(self):
        assert issubclass(entity.LocalEntity, entity.Entity)


@pytest.fixture
def entities(identifiers):
    return [MagicMock(name="entity_" + identifier) for identifier in identifiers]


@pytest.fixture
def entity_cls(entities):
    return MagicMock(name="entity_cls", side_effect=entities)


@pytest.fixture
def entity_creator(gateway, entity_cls, entity_creator_cls):
    entity_creator_cls._entity_cls = entity_cls
    return entity_creator_cls(gateway)


class TestEntityCreator:
    def test_if_entity_cls_is_entity(self):
        assert entity.EntityCreator._entity_cls is entity.Entity

    @pytest.fixture
    def entity_creator_cls(self):
        return entity.EntityCreator

    def test_if_gateway_is_stored_as_instance_attribute(self, gateway, entity_creator):
        assert entity_creator.gateway is gateway

    def test_if_entities_are_correctly_initialized_when_creating_entities(
        self, identifiers, entity_cls, entity_creator
    ):
        entity_creator.create_entities()
        assert entity_cls.mock_calls == [call(identifier=identifier) for identifier in identifiers]

    def test_if_entities_are_returned(self, entities, entity_creator):
        assert entity_creator.create_entities() == entities

    def test_repr(self, entity_creator):
        assert repr(entity_creator) == "EntityCreator(gateway)"


class TestManagedEntityCreator:
    def test_if_entity_cls_is_managed_entity(self):
        assert entity.ManagedEntityCreator._entity_cls is entity.ManagedEntity

    def test_if_subclass_of_entity_creator(self):
        assert issubclass(entity.ManagedEntityCreator, entity.EntityCreator)

    @pytest.fixture
    def entity_creator_cls(self):
        return entity.ManagedEntityCreator

    def test_if_entities_are_correctly_initialized_when_creating_entities(
        self, identifiers, deletion_requested_identifiers, entity_cls, entity_creator
    ):
        entity_creator.create_entities()
        calls = []
        for identifier in identifiers:
            calls.append(
                call(
                    identifier=identifier,
                    deletion_requested=True if identifier in deletion_requested_identifiers else False,
                )
            )
        assert entity_cls.mock_calls == calls

    def test_if_entities_are_returned(self, entities, entity_creator):
        assert entity_creator.create_entities() == entities


class TestSourceEntityCreator:
    def test_if_entity_cls_is_source_entity(self):
        assert entity.SourceEntityCreator._entity_cls is entity.SourceEntity

    def test_if_subclass_of_entity_creator(self):
        assert issubclass(entity.SourceEntityCreator, entity.EntityCreator)


class TestOutboundEntityCreator:
    def test_if_entity_cls_is_outbound_entity(self):
        assert entity.OutboundEntityCreator._entity_cls is entity.OutboundEntity

    def test_if_subclass_of_managed_entity_creator(self):
        assert issubclass(entity.OutboundEntityCreator, entity.ManagedEntityCreator)

    @pytest.fixture
    def entity_creator_cls(self):
        return entity.OutboundEntityCreator

    def test_if_entities_are_correctly_initialized_when_creating_entities(
        self, identifiers, deletion_requested_identifiers, deletion_approved_identifiers, entity_cls, entity_creator
    ):
        entity_creator.create_entities()
        calls = []
        for identifier in identifiers:
            calls.append(
                call(
                    identifier=identifier,
                    deletion_requested=True if identifier in deletion_requested_identifiers else False,
                    deletion_approved=True if identifier in deletion_approved_identifiers else False,
                )
            )
        assert entity_cls.mock_calls == calls

    def test_if_entities_are_returned(self, entities, entity_creator):
        assert entity_creator.create_entities() == entities


class TestLocalEntityCreator:
    def test_if_entity_cls_is_local_entity(self):
        assert entity.LocalEntityCreator._entity_cls is entity.LocalEntity

    def test_if_subclass_of_managed_entity_creator(self):
        assert issubclass(entity.LocalEntityCreator, entity.ManagedEntityCreator)
