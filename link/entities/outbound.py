from __future__ import annotations
from typing import TYPE_CHECKING, List

from .repository import Repository

if TYPE_CHECKING:
    from .domain import Address
    from .local import LocalRepository


class OutboundRepository(Repository):
    def __init__(self, address: Address, local_repo: LocalRepository) -> None:
        super().__init__(address)
        self.local_repo = local_repo

    def delete(self, identifiers: List[str]) -> None:
        for index, identifier in enumerate(identifiers):
            if identifier in self.local_repo:
                raise RuntimeError(f"Can't delete entity that is present in local repository. ID: {identifier}")
            if self[identifier].deletion_requested:
                self[identifier].deletion_approved = True
                del identifiers[index]
        super().delete(identifiers)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.address}, {self.local_repo})"
