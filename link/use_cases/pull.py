from typing import List

from .base import UseCase
from ..entities.local import LocalRepository
from ..entities.outbound import OutboundRepository
from ..entities.repository import SourceRepository


class Pull(UseCase):
    local_repo_cls = LocalRepository
    outbound_repo_cls = OutboundRepository
    source_repo_cls = SourceRepository

    def execute(self, identifiers: List[str]) -> None:
        entities = self.source_repo_cls().fetch(identifiers)
        self.outbound_repo_cls().insert(entities)
        self.local_repo_cls().insert(entities)
