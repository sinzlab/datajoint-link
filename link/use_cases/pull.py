from __future__ import annotations
from typing import TYPE_CHECKING, List

from .base import UseCase

if TYPE_CHECKING:
    from . import RepositoryLink


class Pull(UseCase):
    def execute(self, repo_link: RepositoryLink, identifiers: List[str]) -> None:
        """Pulls the entities specified by the provided identifiers if they were not already pulled."""
        valid_identifiers = [identifier for identifier in identifiers if identifier not in repo_link.local.contents]
        entities = [repo_link.source.contents[identifier] for identifier in valid_identifiers]
        with repo_link.outbound.transaction.transaction(), repo_link.local.transaction.transaction():
            for entity in entities:
                repo_link.outbound.contents[entity.identifier] = entity
                repo_link.local.contents[entity.identifier] = entity
