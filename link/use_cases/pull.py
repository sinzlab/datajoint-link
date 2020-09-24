from __future__ import annotations
from typing import TYPE_CHECKING, List, Set
from dataclasses import dataclass

from .base import AbstractRequestModel, AbstractResponseModel, AbstractUseCase

if TYPE_CHECKING:
    from . import RepositoryLink


@dataclass
class PullRequestModel(AbstractRequestModel):
    """Request model for pull use-case."""

    identifiers: List[str]


@dataclass
class PullResponseModel(AbstractResponseModel):
    """Response model for the pull use-case."""

    requested: Set[str]
    valid: Set[str]
    invalid: Set[str]

    @property
    def n_requested(self) -> int:
        return len(self.requested)

    @property
    def n_valid(self) -> int:
        return len(self.valid)

    @property
    def n_invalid(self) -> int:
        return len(self.invalid)


class PullUseCase(AbstractUseCase[PullRequestModel]):
    response_model_cls = PullResponseModel

    def execute(self, repo_link: RepositoryLink, request_model: PullRequestModel) -> PullResponseModel:
        """Pulls the entities specified by the provided identifiers if they were not already pulled."""
        valid_identifiers = [i for i in request_model.identifiers if i not in repo_link.outbound]
        entities = [repo_link.source[identifier] for identifier in valid_identifiers]
        with repo_link.outbound.transaction(), repo_link.local.transaction():
            for entity in entities:
                repo_link.outbound[entity.identifier] = entity.create_identifier_only_copy()
                repo_link.local[entity.identifier] = entity
        # noinspection PyArgumentList
        return self.response_model_cls(
            requested=set(request_model.identifiers),
            valid=set(valid_identifiers),
            invalid={i for i in request_model.identifiers if i not in valid_identifiers},
        )
