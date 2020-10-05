from __future__ import annotations
from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING, Dict, Iterator

from ..base import Base
from .abstract_gateway import AbstractGateway

if TYPE_CHECKING:
    from .repository import Entity


class FlagManagerFactory(Mapping, Base):
    def __init__(self, entities: Dict[str, Entity], gateway: AbstractGateway) -> None:
        self.entities = entities
        self.gateway = gateway

    def __getitem__(self, identifier: str) -> FlagManager:
        """Gets the entity flags manager corresponding to the entity identified by the provided identifier."""
        return FlagManager(self.entities[identifier], self.gateway)

    def __iter__(self) -> Iterator[FlagManager]:
        """Iterates over flag managers."""
        for entity in self.entities.values():
            yield FlagManager(entity, self.gateway)

    def __len__(self) -> int:
        """Returns the number of entities associated with the factory."""
        return len(self.entities)


class FlagManager(MutableMapping, Base):
    def __init__(self, entity: Entity, gateway: AbstractGateway) -> None:
        self.entity = entity
        self.gateway = gateway

    def __getitem__(self, flag: str) -> bool:
        """Gets the value of a flag of the entity."""
        return self.entity.flags[flag]

    def __setitem__(self, flag: str, value: bool) -> None:
        """Sets the value of a flag of the entity."""
        self.gateway.set_flag(self.entity.identifier, flag, value)
        self.entity.flags[flag] = value

    def __delitem__(self, flag: str) -> None:
        """Deletes a flag from the entity."""
        raise NotImplementedError

    def __iter__(self) -> Iterator[str]:
        """Iterates over the flag names of the entity."""
        return iter(self.entity.flags)

    def __len__(self) -> int:
        """Returns the number of flags the entity has."""
        return len(self.entity.flags)
