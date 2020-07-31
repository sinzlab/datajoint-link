from __future__ import annotations
from typing import Dict
from dataclasses import dataclass

from .contents import Contents
from .flag_manager import FlagManagerFactory
from .transaction_manager import TransactionManager
from .abstract_gateway import AbstractEntityDTO, AbstractGateway
from ..base import Base


@dataclass
class Entity:
    identifier: str
    flags: Dict[str, bool]

    def create_transfer_entity(self, data: AbstractEntityDTO) -> TransferEntity:
        """Creates a transfer entity from the entity given some data."""
        return TransferEntity(self.identifier, self.flags, data)


@dataclass
class TransferEntity(Entity):
    data: AbstractEntityDTO

    def create_entity(self) -> Entity:
        """Creates a regular entity from the transfer entity."""
        return Entity(self.identifier, self.flags)

    def create_identifier_only_copy(self):
        """Creates a copy of the instance that only contains the data pertaining to the unique identifier."""
        # noinspection PyArgumentList
        return self.__class__(self.identifier, self.flags, self.data.create_identifier_only_copy())


@dataclass
class Repository:
    contents: Contents
    flags: FlagManagerFactory
    transaction: TransactionManager


class RepositoryFactory(Base):
    def __init__(self, gateway: AbstractGateway) -> None:
        self.gateway = gateway

    def __call__(self) -> Repository:
        """Creates a repository."""
        entities = {
            identifier: Entity(identifier, self.gateway.get_flags(identifier))
            for identifier in self.gateway.identifiers
        }
        return Repository(
            contents=Contents(entities, self.gateway),
            flags=FlagManagerFactory(entities, self.gateway),
            transaction=TransactionManager(entities, self.gateway),
        )
