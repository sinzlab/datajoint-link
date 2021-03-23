"""Contains code pertaining to the refresh use-case."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Set

from .base import AbstractRequestModel, AbstractResponseModel, AbstractUseCase

if TYPE_CHECKING:
    from . import RepositoryLink

LOGGER = logging.getLogger(__name__)


@dataclass
class RefreshRequestModel(AbstractRequestModel):
    """Request model for the refresh use-case."""


@dataclass
class RefreshResponseModel(AbstractResponseModel):
    """Response model for the refresh use-case."""

    refreshed: Set[str]

    @property
    def n_refreshed(self) -> int:
        """Return the number of entities that were refreshed."""
        return len(self.refreshed)


class RefreshUseCase(
    AbstractUseCase[RefreshRequestModel]
):  # pylint: disable=unsubscriptable-object,too-few-public-methods
    """Use-case that refreshes entities in the local table."""

    name = "refresh"
    response_model_cls = RefreshResponseModel

    def execute(self, repo_link: RepositoryLink, _: RefreshRequestModel) -> RefreshResponseModel:
        """Refresh the deletion requested flags in the local table."""
        deletion_requested = {i for i in repo_link.outbound if repo_link.outbound.flags[i]["deletion_requested"]}
        to_be_enabled = {i for i in deletion_requested if not repo_link.local.flags[i]["deletion_requested"]}
        for identifier in to_be_enabled:
            repo_link.local.flags[identifier]["deletion_requested"] = True
            LOGGER.info(f"Enabled 'deletion_requested' flag of entity with identifier {identifier} in local table")
        # noinspection PyArgumentList
        return self.response_model_cls(refreshed=to_be_enabled)
