from __future__ import annotations
from typing import TYPE_CHECKING, List

from .repository import Repository

if TYPE_CHECKING:
    from .domain import Address, FlaggedEntity
    from .outbound import OutboundRepository


class LocalRepository(Repository):
    def __init__(self, address: Address, outbound_repo: OutboundRepository):
        super().__init__(address)
        self.outbound_repo = outbound_repo

    def delete(self, identifiers: List[str]) -> None:
        with self.transaction():
            super().delete(identifiers)
            self.outbound_repo.delete(identifiers)

    def insert(self, entities: List[FlaggedEntity]) -> None:
        for entity in entities:
            if entity.identifier not in self.outbound_repo:
                raise RuntimeError
        for entity in entities:
            if entity.deletion_requested:
                raise RuntimeError
        super().insert(entities)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.address}, {self.outbound_repo})"
