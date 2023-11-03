"""Logic associated with presenting information about finished use-cases."""
from __future__ import annotations

from typing import Callable, Iterable

from link.domain import events

from .custom_types import PrimaryKey
from .identification import IdentificationTranslator


def create_idle_entities_updater(
    translator: IdentificationTranslator, update: Callable[[Iterable[PrimaryKey]], None]
) -> Callable[[events.IdleEntitiesListed], None]:
    """Create a callable that when called updates the list of idle entities."""

    def update_idle_entities(response: events.IdleEntitiesListed) -> None:
        update(translator.to_primary_key(identifier) for identifier in response.identifiers)

    return update_idle_entities
