from __future__ import annotations
from typing import TYPE_CHECKING, Dict, Iterator, Optional
from contextlib import contextmanager

from ..base import Base
from .abstract_gateway import AbstractGateway

if TYPE_CHECKING:
    from .repository import Entity


class TransactionManager(Base):
    def __init__(self, entities: Dict[str, Entity], gateway: AbstractGateway) -> None:
        self.entities = entities
        self.gateway = gateway
        self._entities_copy: Dict[str, Entity]

    @property
    def in_transaction(self) -> bool:
        """Returns "True" if the manager is in transaction, "False" otherwise."""
        return hasattr(self, "_entities_copy")

    def start(self) -> None:
        """Starts a transaction."""
        self.gateway.start_transaction()
        setattr(self, "_entities_copy", self.entities.copy())

    def commit(self) -> None:
        """Commits a transaction."""
        self.gateway.commit_transaction()
        delattr(self, "_entities_copy")

    def cancel(self) -> None:
        """Cancels a transaction."""
        self.entities.update(self._entities_copy)
        self.gateway.cancel_transaction()
        delattr(self, "_entities_copy")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Context manager for transactions."""
        self.start()
        try:
            yield
        except RuntimeError:
            self.cancel()
        else:
            self.commit()
