from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Iterator

from ..types import PrimaryKey


@dataclass
class Entity:
    master: Dict[str, Any]
    parts: Dict[str, Any]
    packet: Optional[EntityPacket] = field(default=None, init=False, repr=False)

    @property
    def primary_key(self) -> PrimaryKey:
        return {attr: self.master[attr] for attr in self.packet.primary_attrs}


@dataclass
class EntityPacket:
    primary_attrs: List[str]
    entities: List[Entity]

    def __iter__(self) -> Iterator:
        return iter(self.entities)


class EntityPacketCreator:
    entity_cls: Entity = None
    entity_packet_cls: EntityPacket = None

    def create(
        self, primary_attrs: List[str], master_entities: List[Dict[str, Any]], part_entities: List[Dict[str, Any]]
    ) -> EntityPacket:
        entities = self._create_entities(master_entities, part_entities)
        return self._create_packet(primary_attrs, entities)

    def _create_entities(
        self, master_entities: List[Dict[str, Any]], parts_entities: List[Dict[str, Any]]
    ) -> List[Entity]:
        entities = []
        for master_entity, part_entities in zip(master_entities, parts_entities):
            entities.append(self._create_entity(master_entity, part_entities))
        return entities

    def _create_entity(self, master_entity: Dict[str, Any], part_entities: Dict[str, Any]) -> Entity:
        # noinspection PyCallingNonCallable
        return self.entity_cls(master=master_entity, parts=part_entities)

    def _create_packet(self, primary_attrs: List[str], entities: List[Entity]):
        # noinspection PyCallingNonCallable
        packet = self.entity_packet_cls(primary_attrs, entities)
        for entity in entities:
            entity.packet = packet
        return packet

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + "()"
