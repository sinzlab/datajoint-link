from __future__ import annotations
from typing import TYPE_CHECKING, Dict, ContextManager
from contextlib import contextmanager

from ..base import Base
from .abstract_gateway import AbstractGateway

if TYPE_CHECKING:
    from .repository import Entity


class TransactionManager(Base):
    def __init__(self, entities: Dict[str, Entity], gateway: AbstractGateway) -> None:
        self.entities = entities
        self.gateway = gateway
        self._entities_copy = None

    @property
    def in_transaction(self) -> bool:
        """Returns "True" if the manager is in transaction, "False" otherwise."""
        return self._entities_copy is not None

    def start(self) -> None:
        """Starts a transaction."""
        self.gateway.start_transaction()
        self._entities_copy = self.entities.copy()

    def commit(self) -> None:
        """Commits a transaction."""
        self.gateway.commit_transaction()
        self._entities_copy = None

    def cancel(self) -> None:
        """Cancels a transaction."""
        self.entities.update(self._entities_copy)
        self.gateway.cancel_transaction()
        self._entities_copy = None

    @contextmanager
    def transaction(self) -> ContextManager:
        """Context manager for transactions."""
        self.start()
        try:
            yield
        except RuntimeError:
            self.cancel()
        else:
            self.commit()
