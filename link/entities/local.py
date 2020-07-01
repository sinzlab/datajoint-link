from __future__ import annotations
from typing import TYPE_CHECKING, List, Optional

from .repository import Repository

if TYPE_CHECKING:
    from .entity import FlaggedEntity
    from .link import Link


class LocalRepository(Repository):
    def __init__(self):
        super().__init__()
        self.link: Optional[Link] = None

    def delete(self, identifiers: List[str]) -> None:
        with self.transaction():
            super().delete(identifiers)
            self.link.delete_in_outbound_repo(identifiers)

    def insert(self, entities: List[FlaggedEntity]) -> None:
        for entity in entities:
            if self.link.not_present_in_outbound_repo(entity.identifier):
                raise RuntimeError(
                    f"Can't insert entity that is not present in outbound repository. ID: {entity.identifier}"
                )
            if entity.deletion_requested:
                raise RuntimeError(f"Can't insert entity that had its deletion requested. ID: {entity.identifier}")
        super().insert(entities)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.address})"
