from __future__ import annotations
from typing import TYPE_CHECKING, List, Optional

from .repository import NonSourceRepository

if TYPE_CHECKING:
    from .link import Link
    from ..adapters.gateway import AbstractOutboundGateway


class OutboundRepository(NonSourceRepository):
    gateway: AbstractOutboundGateway

    def __init__(self) -> None:
        super().__init__()
        self.link: Optional[Link] = None

    def delete(self, identifiers: List[str]) -> None:
        for identifier in identifiers:
            if self.link.present_in_local_repo(identifier):
                raise RuntimeError(f"Can't delete entity that is present in local repository. ID: {identifier}")
        deletion_requested = []
        for index, identifier in enumerate(identifiers):
            if self[identifier].deletion_requested:
                self[identifier].deletion_approved = True
                del identifiers[index]
                deletion_requested.append(identifier)
        self.gateway.approve_deletion(deletion_requested)
        super().delete(identifiers)
