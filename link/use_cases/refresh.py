from . import RepositoryLink
from .base import UseCase


class Refresh(UseCase):
    def execute(self, repo_link: RepositoryLink) -> None:
        """Refreshes the deletion requested flags in the local table."""
        deletion_requested = {i for i in repo_link.outbound if repo_link.outbound.flags[i]["deletion_requested"]}
        to_be_enabled = {i for i in deletion_requested if not repo_link.local.flags[i]["deletion_requested"]}
        for identifier in to_be_enabled:
            repo_link.local.flags[identifier]["deletion_requested"] = True
