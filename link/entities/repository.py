from typing import Optional, Dict, Any
from dataclasses import dataclass, field

from .contents import Contents
from .flag_manager import FlagManagerFactory
from .transaction_manager import TransactionManager
from .abstract_gateway import AbstractEntityGateway
from .representation import Base


@dataclass
class Entity:
    identifier: str
    flags: Optional[Dict[str, bool]] = field(default_factory=dict)
    data: Optional[Any] = field(default=None, repr=False, init=False)


@dataclass
class Repository:
    contents: Contents
    flags: FlagManagerFactory
    transaction: TransactionManager


class RepositoryFactory(Base):
    def __init__(self, gateway: AbstractEntityGateway) -> None:
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
