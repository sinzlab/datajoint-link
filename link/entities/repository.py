from typing import List, Optional, Type, Iterator
from functools import wraps
from contextlib import contextmanager

from .address import Address
from .entity import Entity


def _check_identifiers(function):
    @wraps(function)
    def wrapper(self, identifiers):
        for identifier in identifiers:
            if identifier not in self:
                raise KeyError(identifier)
        return function(self, identifiers)

    return wrapper


class Repository:
    gateway = None
    entity_cls: Type[Entity] = None

    def __init__(self, address: Address) -> None:
        """Initializes Repository."""
        self.address = address
        self._identifiers: List[str] = self.gateway.get_identifiers()
        self._backed_up_identifiers: Optional[List[str]] = None

    def _create_entities(self, identifiers: List[str]) -> List[Entity]:
        return [self.entity_cls(self.address, i) for i in identifiers]

    @property
    def identifiers(self):
        return self._identifiers.copy()

    @property
    def entities(self) -> List[Entity]:
        return self._create_entities(self.identifiers)

    @_check_identifiers
    def fetch(self, identifiers: List[str]) -> List[Entity]:
        self.gateway.fetch(identifiers)
        entities = self._create_entities(identifiers)
        return entities

    @_check_identifiers
    def delete(self, identifiers: List[str]) -> None:
        self.gateway.delete(identifiers)
        for identifier in identifiers:
            del self._identifiers[self.identifiers.index(identifier)]

    def insert(self, entities: List[Entity]) -> None:
        for entity in entities:
            if entity.address != self.address:
                raise ValueError(
                    f"Entity address ('{entity.address}') does not match repository address ('{self.address}')"
                )
            if entity.identifier in self:
                raise ValueError(f"Entity with identifier '{entity.identifier}' is already in repository")
        self.gateway.insert([e.identifier for e in entities])
        for entity in entities:
            self._identifiers.append(entity.identifier)

    @property
    def in_transaction(self) -> bool:
        if self._backed_up_identifiers is None:
            return False
        return True

    def start_transaction(self) -> None:
        if self.in_transaction:
            raise RuntimeError("Can't start transaction while in transaction")
        self.gateway.start_transaction()
        self._backed_up_identifiers = self.identifiers

    def commit_transaction(self) -> None:
        if not self.in_transaction:
            raise RuntimeError("Can't commit transaction while not in transaction")
        self.gateway.commit_transaction()
        self._backed_up_identifiers = None

    def cancel_transaction(self) -> None:
        if not self.in_transaction:
            raise RuntimeError("Can't cancel transaction while not in transaction")
        self.gateway.cancel_transaction()
        self._identifiers = self._backed_up_identifiers
        self._backed_up_identifiers = None

    @contextmanager
    def transaction(self):
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
