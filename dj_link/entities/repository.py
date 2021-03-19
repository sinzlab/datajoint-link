"""Contains the entity repository class and related classes."""
from __future__ import annotations

from collections.abc import MutableMapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterator

from ..base import Base
from .abstract_gateway import AbstractEntityDTO, AbstractGateway
from .contents import Contents
from .flag_manager import FlagManagerFactory


@dataclass
class Entity:
    """Represents an entity in a repository."""

    identifier: str
    flags: Dict[str, bool]

    def create_transfer_entity(self, data: AbstractEntityDTO) -> TransferEntity:
        """Create a transfer entity from the entity given some data."""
        return TransferEntity(self.identifier, self.flags, data)


@dataclass
class TransferEntity(Entity):
    """Represents an entity in transit between repositories."""

    data: AbstractEntityDTO

    def create_entity(self) -> Entity:
        """Create a regular entity from the transfer entity."""
        return Entity(self.identifier, self.flags)

    def create_identifier_only_copy(self):
        """Create a copy of the instance that only contains the data pertaining to the unique identifier."""
        # noinspection PyArgumentList
        return self.__class__(self.identifier, self.flags, self.data.create_identifier_only_copy())


class EntityFactory(Base):  # pylint: disable=too-few-public-methods
    """Factory that produces entities."""

    def __init__(self, gateway: AbstractGateway) -> None:
        """Initialize the entity factory."""
        self.gateway = gateway

    def __call__(self, identifier: str) -> Entity:
        """Create an entity based on the provided identifier."""
        return Entity(identifier, self.gateway.get_flags(identifier))


class Repository(MutableMapping, Base):  # pylint: disable=too-many-ancestors
    """Repository containing entities."""

    def __init__(
        self,
        contents: Contents,
        flags: FlagManagerFactory,
        gateway: AbstractGateway,
    ):
        """Initialize the repository."""
        self.contents = contents
        self.flags = flags
        self.gateway = gateway

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

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Context manager that handles the starting, committing and cancelling of transactions."""
        self.gateway.start_transaction()
        try:
            yield
        except RuntimeError:
            self.gateway.cancel_transaction()
        else:
            self.gateway.commit_transaction()


class RepositoryFactory(Base):  # pylint: disable=too-few-public-methods
    """Factory that produces repositories."""

    def __init__(self, gateway: AbstractGateway) -> None:
        """Initialize the repository factory."""
        self.gateway = gateway

    def __call__(self) -> Repository:
        """Create a repository."""
        entity_factory = EntityFactory(self.gateway)
        return Repository(
            contents=Contents(self.gateway, entity_factory),
            flags=FlagManagerFactory(self.gateway, entity_factory),
            gateway=self.gateway,
        )
