"""Contains code managing flags on entities."""
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING, Dict, Iterator

from ..base import Base
from .abstract_gateway import AbstractGateway

if TYPE_CHECKING:
    from .repository import Entity


class FlagManagerFactory(Mapping, Base):
    """Factory producing flag managers."""

    def __init__(self, entities: Dict[str, Entity], gateway: AbstractGateway) -> None:
        """Initialize the flag manager factory."""
        self.entities = entities
        self.gateway = gateway

    def __getitem__(self, identifier: str) -> FlagManager:
        """Get the entity flags manager corresponding to the entity identified by the provided identifier."""
        return FlagManager(self.entities[identifier], self.gateway)

    def __iter__(self) -> Iterator[FlagManager]:
        """Iterate over flag managers."""
        for entity in self.entities.values():
            yield FlagManager(entity, self.gateway)

    def __len__(self) -> int:
        """Return the number of entities associated with the factory."""
        return len(self.entities)


class FlagManager(MutableMapping, Base):
    """Manages the flags of a single entity."""

    def __init__(self, entity: Entity, gateway: AbstractGateway) -> None:
        """Initialize the flag manager."""
        self.entity = entity
        self.gateway = gateway

    def __getitem__(self, flag: str) -> bool:
        """Get the value of a flag of the entity."""
        return self.entity.flags[flag]

    def __setitem__(self, flag: str, value: bool) -> None:
        """Set the value of a flag of the entity."""
        self.gateway.set_flag(self.entity.identifier, flag, value)
        self.entity.flags[flag] = value

    def __delitem__(self, flag: str) -> None:
        """Delete a flag from the entity."""
        raise NotImplementedError

    def __iter__(self) -> Iterator[str]:
        """Iterate over the flag names of the entity."""
        return iter(self.entity.flags)

    def __len__(self) -> int:
        """Return the number of flags the entity has."""
        return len(self.entity.flags)
