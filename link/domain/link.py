"""Contains the link class."""
from __future__ import annotations

from collections import deque
from typing import Iterable

from .custom_types import Identifier
from .state import STATE_MAP, Components, Entity, PersistentState, Processes


def create_entity(
    identifier: Identifier, *, components: Iterable[Components], is_tainted: bool, process: Processes
) -> Entity:
    """Create an entity."""
    presence = frozenset(components)
    persistent_state = PersistentState(presence, is_tainted=is_tainted, has_process=process is not Processes.NONE)
    state = STATE_MAP[persistent_state]
    return Entity(
        identifier,
        state=state,
        current_process=process,
        is_tainted=is_tainted,
        events=deque(),
    )
