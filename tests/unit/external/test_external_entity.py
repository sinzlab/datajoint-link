from dataclasses import is_dataclass
from unittest.mock import MagicMock, call
from typing import Type

import pytest

from link.external.entity import Entity, EntityPacket, EntityPacketCreator


@pytest.fixture
def primary_attrs():
    return "a", "c"


@pytest.fixture
def n_entities():
    return 3


@pytest.fixture
def entities(n_entities):
    return [MagicMock(name="entity" + str(i), spec=Entity) for i in range(n_entities)]


class TestEntity:
    @pytest.fixture
    def master(self):
        return dict(a=0, b=1, c=2)

    @pytest.fixture
    def parts(self):
        return dict()

    @pytest.fixture
    def entity(self, master, parts):
        return Entity(master, parts)

    def test_if_dataclass(self):
        assert is_dataclass(Entity)

    def test_if_master_instance_attribute_is_set(self, entity, master):
        assert entity.master == master

    def test_if_parts_instance_attribute_is_set(self, entity, parts):
        assert entity.parts == parts

    def test_if_packet_instance_attribute_is_none_by_default(self, entity):
        assert entity.packet is None

    def test_primary_key_property(self, entity, primary_attrs):
        entity.packet = MagicMock(name="packet", primary_attrs=primary_attrs, spec=EntityPacket)
        assert entity.primary_key == dict(a=0, c=2)

    def test_repr(self, entity):
        assert repr(entity) == "Entity(master={'a': 0, 'b': 1, 'c': 2}, parts={})"


class TestEntityPacket:
    @pytest.fixture
    def entity_packet(self, primary_attrs, entities):
        return EntityPacket(primary_attrs, entities)

    def test_if_dataclass(self):
        assert is_dataclass(EntityPacket)

    def test_if_primary_attrs_instance_attribute_is_set(self, primary_attrs, entity_packet):
        assert entity_packet.primary_attrs == primary_attrs

    def test_if_entities_instance_attribute_is_set(self, entities, entity_packet):
        assert entity_packet.entities == entities

    def test_if_iteration_works_correctly(self, entities, entity_packet):
        assert all(entity1 is entity2 for entity1, entity2 in zip(entity_packet, entities))


class TestEntityPacketCreator:
    @pytest.fixture
    def entity_cls(self, entities):
        return MagicMock(name="entity_cls", side_effect=entities, spec=Type[Entity])

    @pytest.fixture
    def entity_packet_cls(self):
        return MagicMock(name="entity_packet_cls", spec=Type[EntityPacket])

    @pytest.fixture
    def entity_packet_creator(self, entity_cls, entity_packet_cls):
        EntityPacketCreator.entity_cls = entity_cls
        EntityPacketCreator.entity_packet_cls = entity_packet_cls
        return EntityPacketCreator()

    @pytest.fixture
    def master_entities(self, n_entities):
        return ["master_entity" + str(i) for i in range(n_entities)]

    @pytest.fixture
    def part_entities(self, n_entities):
        return ["part_entities" + str(i) for i in range(n_entities)]

    @pytest.fixture
    def entities_packet(self, entity_packet_creator, primary_attrs, master_entities, part_entities):
        return entity_packet_creator.create(primary_attrs, master_entities, part_entities)

    def test_if_entity_cls_is_none_by_default(self):
        assert EntityPacketCreator.entity_cls is None

    def test_if_entity_packet_cls_is_none_by_default(self):
        assert EntityPacketCreator.entity_packet_cls is None

    @pytest.mark.usefixtures("entities_packet")
    def test_if_entities_are_correctly_initialized(self, entity_cls, master_entities, part_entities):
        calls = [call(master=master, parts=part) for master, part in zip(master_entities, part_entities)]
        assert entity_cls.call_args_list == calls

    @pytest.mark.usefixtures("entities_packet")
    def test_if_entities_packet_is_correctly_initialized(self, primary_attrs, entity_packet_cls, entities):
        entity_packet_cls.assert_called_once_with(primary_attrs, entities)

    @pytest.mark.usefixtures("entities_packet")
    def test_if_packet_attribute_on_entities_is_set(self, entities, entity_packet_cls):
        assert all(entity.packet is entity_packet_cls() for entity in entities)

    def test_if_entities_packet_is_returned(self, entity_packet_cls, entities_packet):
        assert entities_packet is entity_packet_cls()

    def test_repr(self, entity_packet_creator):
        assert repr(entity_packet_creator) == "EntityPacketCreator()"
