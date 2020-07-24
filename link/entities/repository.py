from typing import Optional, Dict
from dataclasses import dataclass, field

from .contents import Contents
from .flag_manager import FlagManagerFactory
from .transaction_manager import TransactionManager
from .abstract_gateway import AbstractGateway


@dataclass
class Entity:
    identifier: str
    flags: Optional[Dict[str, bool]] = field(default_factory=dict)


@dataclass
class Repository:
    contents: Contents
    flags: FlagManagerFactory
    transaction: TransactionManager


class RepositoryFactory:
    def __init__(self, gateway: AbstractGateway, storage: Dict) -> None:
        self.gateway = gateway
        self.storage = storage

    def __call__(self) -> Repository:
        """Creates a repository."""
        entities = {
            identifier: Entity(identifier, self.gateway.get_flags(identifier))
            for identifier in self.gateway.identifiers
        }
        return Repository(
            contents=Contents(entities, self.gateway, self.storage),
            flags=FlagManagerFactory(entities, self.gateway),
            transaction=TransactionManager(entities, self.gateway),
        )
