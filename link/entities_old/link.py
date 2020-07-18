from __future__ import annotations
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from .local import LocalRepository
    from .outbound import OutboundRepository


class Link:
    def __init__(self, local_repo: LocalRepository, outbound_repo: OutboundRepository) -> None:
        self.local_repo = local_repo
        self.outbound_repo = outbound_repo
        self.local_repo.link = self
        self.outbound_repo.link = self

    def present_in_local_repo(self, identifier: str) -> bool:
        return identifier in self.local_repo

    def not_present_in_outbound_repo(self, identifier: str) -> bool:
        return identifier not in self.outbound_repo

    def delete_in_outbound_repo(self, identifiers: List[str]) -> None:
        self.outbound_repo.delete(identifiers)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.local_repo}, {self.outbound_repo})"
