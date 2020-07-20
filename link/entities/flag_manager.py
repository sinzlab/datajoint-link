from __future__ import annotations
from collections.abc import Mapping, MutableMapping
from typing import Dict, Iterator

from .repository import Entity
from .representation import _represent
from ..adapters.gateway import GatewayTypeVar


class EntityFlagsManagerFactory(Mapping):
    def __init__(self, entities: Dict[str, Entity], gateway: GatewayTypeVar) -> None:
        pass

    def __getitem__(self, identifier: str) -> EntityFlagsManager:
        pass

    def __iter__(self) -> Iterator[EntityFlagsManager]:
        pass

    def __len__(self) -> int:
        pass


class EntityFlagsManager(MutableMapping):
    __delitem__ = None

    def __init__(self, entity: Entity, gateway: GatewayTypeVar) -> None:
        self.entity = entity
        self.gateway = gateway

    def __getitem__(self, flag: str) -> bool:
        """Gets the value of a flag of the entity."""
        return self.entity.flags[flag]

    def __setitem__(self, flag: str, value: bool) -> None:
        """Sets the value of a flag of the entity."""
        self.gateway.set_flag(flag, value)
        self.entity.flags[flag] = value

    def __iter__(self) -> Iterator[str]:
        """Iterates over the flag names of the entity."""
        return iter(self.entity.flags)

    def __len__(self) -> int:
        """Returns the number of flags the entity has."""
        return len(self.entity.flags)

    def __repr__(self) -> str:
        return _represent(self, ["entity", "gateway"])
