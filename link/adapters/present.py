"""Logic associated with presenting information about finished use-cases."""
from __future__ import annotations

from typing import Callable

from link.domain import events

from .identification import IdentificationTranslator


def create_state_change_logger(
    translator: IdentificationTranslator, log: Callable[[str], None]
) -> Callable[[events.StateChanged], None]:
    """Create a logger that logs state changes of entities."""

    def log_state_change(state_change: events.StateChanged) -> None:
        context = {
            "identifier": translator.to_primary_key(state_change.identifier),
            "operation": state_change.operation.name,
            "transition": {
                "old": state_change.transition.current.__name__,
                "new": state_change.transition.new.__name__,
            },
            "command": state_change.command.name,
        }
        log(f"Entity state changed {context}")

    return log_state_change
