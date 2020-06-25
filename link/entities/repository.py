from typing import List, Optional, Iterator, ContextManager
from contextlib import contextmanager

from .address import Address
from .entity import Entity


class Repository:
    gateway = None
    entity_creator = None

    def __init__(self, address: Address) -> None:
        """Initializes Repository."""
        self.address = address
        self._entities = self.entity_creator.create_entities()
        self._backed_up_entities: Optional[List[Entity]] = None

    @property
    def identifiers(self):
        return [entity.identifier for entity in self.entities]

    @property
    def entities(self) -> List[Entity]:
        return self._entities

    def fetch(self, identifiers: List[str]) -> List[Entity]:
        self.gateway.fetch(identifiers)
        return [entity for entity in self.entities if entity.identifier in identifiers]

    def delete(self, identifiers: List[str]) -> None:
        self.gateway.delete(identifiers)
        self._entities = [entity for entity in self._entities if entity.identifier not in identifiers]

    def insert(self, entities: List[Entity]) -> None:
        self.gateway.insert([entity.identifier for entity in entities])
        self._entities += entities

    @property
    def in_transaction(self) -> bool:
        if self._backed_up_entities is None:
            return False
        return True

    def start_transaction(self) -> None:
        if self.in_transaction:
            raise RuntimeError("Can't start transaction while in transaction")
        self.gateway.start_transaction()
        self._backed_up_entities = self.entities

    def commit_transaction(self) -> None:
        if not self.in_transaction:
            raise RuntimeError("Can't commit transaction while not in transaction")
        self.gateway.commit_transaction()
        self._backed_up_entities = None

    def cancel_transaction(self) -> None:
        if not self.in_transaction:
            raise RuntimeError("Can't cancel transaction while not in transaction")
        self.gateway.cancel_transaction()
        self._entities = self._backed_up_entities
        self._backed_up_entities = None

    @contextmanager
    def transaction(self) -> ContextManager:
        self.start_transaction()
        try:
            yield
        except RuntimeError as exception:
            self.cancel_transaction()
            raise exception
        else:
            self.commit_transaction()

    def __contains__(self, identifier) -> bool:
        return identifier in self.identifiers

    def __len__(self) -> int:
        return len(self.identifiers)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({repr(self.address)})"

    def __iter__(self) -> Iterator:
        for identifier in self.identifiers:
            yield identifier
