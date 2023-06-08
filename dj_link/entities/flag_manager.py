"""Contains code managing flags on entities."""
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING, Iterator

from ..base import Base
from .abstract_gateway import AbstractGateway
from .custom_types import Identifier

if TYPE_CHECKING:
    from .repository import Entity, EntityFactory


class FlagManagerFactory(Mapping, Base):
    """Factory producing flag managers."""

    def __init__(self, gateway: AbstractGateway, entity_factory: EntityFactory) -> None:
        """Initialize the flag manager factory."""
        self.gateway = gateway
        self.entity_factory = entity_factory

    def __getitem__(self, identifier: Identifier) -> FlagManager:
        """Get the entity flags manager corresponding to the entity identified by the provided identifier."""
        return FlagManager(self.entity_factory(identifier), self.gateway)

    def __iter__(self) -> Iterator[FlagManager]:
        """Iterate over flag managers."""
        for identifier in self.gateway:
            yield FlagManager(self.entity_factory(identifier), self.gateway)

    def __len__(self) -> int:
        """Return the number of entities associated with the factory."""
        return len(self.gateway)


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
