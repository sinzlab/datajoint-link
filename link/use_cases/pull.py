from __future__ import annotations
from typing import TYPE_CHECKING, List

from .base import UseCase

if TYPE_CHECKING:
    from . import RepositoryLink


class PullUseCase(UseCase):
    def execute(self, repo_link: RepositoryLink, identifiers: List[str]) -> None:
        """Pulls the entities specified by the provided identifiers if they were not already pulled."""
        valid_identifiers = [identifier for identifier in identifiers if identifier not in repo_link.outbound]
        entities = [repo_link.source[identifier] for identifier in valid_identifiers]
        with repo_link.outbound.transaction(), repo_link.local.transaction():
            for entity in entities:
                repo_link.outbound[entity.identifier] = entity.create_identifier_only_copy()
                repo_link.local[entity.identifier] = entity
