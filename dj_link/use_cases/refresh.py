from __future__ import annotations
from typing import TYPE_CHECKING, Set
from dataclasses import dataclass

from .base import AbstractRequestModel, AbstractResponseModel, AbstractUseCase

if TYPE_CHECKING:
    from . import RepositoryLink


@dataclass
class RefreshRequestModel(AbstractRequestModel):
    """Request model for the refresh use-case."""


@dataclass
class RefreshResponseModel(AbstractResponseModel):
    """Response model for the refresh use-case."""

    refreshed: Set[str]

    @property
    def n_refreshed(self) -> int:
        return len(self.refreshed)


class RefreshUseCase(AbstractUseCase[RefreshRequestModel]):
    response_model_cls = RefreshResponseModel

    def execute(self, repo_link: RepositoryLink, request_model: RefreshRequestModel) -> RefreshResponseModel:
        """Refreshes the deletion requested flags in the local table."""
        deletion_requested = {i for i in repo_link.outbound if repo_link.outbound.flags[i]["deletion_requested"]}
        to_be_enabled = {i for i in deletion_requested if not repo_link.local.flags[i]["deletion_requested"]}
        for identifier in to_be_enabled:
            repo_link.local.flags[identifier]["deletion_requested"] = True
        # noinspection PyArgumentList
        return self.response_model_cls(refreshed=to_be_enabled)
