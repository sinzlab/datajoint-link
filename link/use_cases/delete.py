from __future__ import annotations
from typing import TYPE_CHECKING, List, Tuple, Set

from .base import UseCase

if TYPE_CHECKING:
    from . import RepositoryLink


class DeleteUseCase(UseCase):
    def execute(self, repo_link: RepositoryLink, identifiers: List[str]) -> None:
        """Executes logic associated with the deletion of entities from the local repository."""
        deletion_requested, deletion_not_requested = self._group_by_deletion_requested(repo_link, identifiers)
        self._approve_deletion(repo_link, deletion_requested)
        self._delete_from_outbound(repo_link, deletion_not_requested)
        self._delete_from_local(repo_link, identifiers)

    @staticmethod
    def _group_by_deletion_requested(repo_link: RepositoryLink, identifiers: List[str]) -> Tuple[Set[str], Set[str]]:
        deletion_requested = {i for i in identifiers if repo_link.outbound.flags[i]["deletion_requested"]}
        deletion_not_requested = set(identifiers) - deletion_requested
        return deletion_requested, deletion_not_requested

    @staticmethod
    def _approve_deletion(repo_link: RepositoryLink, deletion_requested: Set[str]) -> None:
        for identifier in deletion_requested:
            repo_link.outbound.flags[identifier]["deletion_approved"] = True

    @staticmethod
    def _delete_from_outbound(repo_link: RepositoryLink, deletion_not_requested: Set[str]) -> None:
        for identifier in deletion_not_requested:
            del repo_link.outbound[identifier]

    @staticmethod
    def _delete_from_local(repo_link: RepositoryLink, identifiers: List[str]) -> None:
        for identifier in identifiers:
            del repo_link.local[identifier]
