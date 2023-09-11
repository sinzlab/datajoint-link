"""Contains the presenter class and related classes/functions."""

from typing import Callable, Iterable

from dj_link.use_cases.use_cases import (
    DeleteResponseModel,
    ListIdleEntitiesResponseModel,
    ProcessResponseModel,
    PullResponseModel,
)

from .custom_types import PrimaryKey
from .identification import IdentificationTranslator


class DJPresenter:
    """DataJoint-specific presenter."""

    def __init__(
        self,
        translator: IdentificationTranslator,
        *,
        update_idle_entities_list: Callable[[Iterable[PrimaryKey]], None],
    ) -> None:
        """Initialize the presenter."""
        self._translator = translator
        self._update_idle_entities_list = update_idle_entities_list

    def pull(self, response: PullResponseModel) -> None:
        """Present information about a finished pull use-case."""

    def delete(self, response: DeleteResponseModel) -> None:
        """Present information about a finished delete use-case."""

    def process(self, response: ProcessResponseModel) -> None:
        """Present information about a finished process use-case."""

    def update_idle_entities_list(self, response: ListIdleEntitiesResponseModel) -> None:
        """Update the list of idle entities."""
        self._update_idle_entities_list(
            self._translator.to_primary_key(identifier) for identifier in response.identifiers
        )
