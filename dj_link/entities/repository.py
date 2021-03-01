from __future__ import annotations

from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import ContextManager, Dict, Iterator

from ..base import Base
from .abstract_gateway import AbstractEntityDTO, AbstractGateway
from .contents import Contents
from .flag_manager import FlagManagerFactory
from .transaction_manager import TransactionManager


@dataclass
class Entity:
    identifier: str
    flags: Dict[str, bool]

    def create_transfer_entity(self, data: AbstractEntityDTO) -> TransferEntity:
        """Create a transfer entity from the entity given some data."""
        return TransferEntity(self.identifier, self.flags, data)


@dataclass
class TransferEntity(Entity):
    data: AbstractEntityDTO

    def create_entity(self) -> Entity:
        """Create a regular entity from the transfer entity."""
        return Entity(self.identifier, self.flags)

    def create_identifier_only_copy(self):
        """Create a copy of the instance that only contains the data pertaining to the unique identifier."""
        # noinspection PyArgumentList
        return self.__class__(self.identifier, self.flags, self.data.create_identifier_only_copy())


class Repository(MutableMapping, Base):
    def __init__(self, contents: Contents, flags: FlagManagerFactory, transaction_manager: TransactionManager):
        self.contents = contents
        self.flags = flags
        self.transaction_manager = transaction_manager

    def __getitem__(self, identifier: str) -> TransferEntity:
        """Get an entity from the repository."""
        return self.contents[identifier]

    def __setitem__(self, identifier: str, transfer_entity: TransferEntity) -> None:
        """Add an entity to the repository."""
        self.contents[identifier] = transfer_entity

    def __delitem__(self, identifier: str) -> None:
        """Delete an entity from the repository."""
        del self.contents[identifier]

    def __iter__(self) -> Iterator[str]:
        """Iterate over identifiers in the repository."""
        return iter(self.contents)

    def __len__(self) -> int:
        """Return the number of identifiers in the repository."""
        return len(self.contents)

    def transaction(self) -> ContextManager:
        """Context manager that handles the starting, committing and cancelling of transactions."""
        return self.transaction_manager.transaction()


class RepositoryFactory(Base):
    def __init__(self, gateway: AbstractGateway) -> None:
        self.gateway = gateway

    def __call__(self) -> Repository:
        """Create a repository."""
        entities = {
            identifier: Entity(identifier, self.gateway.get_flags(identifier))
            for identifier in self.gateway.identifiers
        }
        return Repository(
            contents=Contents(entities, self.gateway),
            flags=FlagManagerFactory(entities, self.gateway),
            transaction_manager=TransactionManager(entities, self.gateway),
        )
