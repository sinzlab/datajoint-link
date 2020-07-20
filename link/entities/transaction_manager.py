from typing import Dict, ContextManager
from contextlib import contextmanager

from .contents import Entity
from .gateway import AbstractGateway
from .representation import _represent


class TransactionManager:
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

    def __repr__(self) -> str:
        return _represent(self, ["entities", "gateway"])
