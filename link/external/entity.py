from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, List, Tuple, Optional, Iterator

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
    primary_attrs: Tuple[str]
    entities: Tuple[Entity]

    def __iter__(self) -> Iterator:
        return iter(self.entities)


class EntityPacketCreator:
    entity_cls = None
    entity_packet_cls = None

    def create(
        self, primary_attrs: List[str], master_entities: List[Dict[str, Any]], part_entities: List[Dict[str, Any]]
    ) -> EntityPacket:
        entities = tuple(
            self.entity_cls(master=master, parts=parts) for master, parts in zip(master_entities, part_entities)
        )
        packet = self.entity_packet_cls(primary_attrs, entities)
        for entity in entities:
            entity.packet = packet
        return packet

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + "()"
