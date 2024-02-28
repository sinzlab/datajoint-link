from __future__ import annotations

from typing import TypedDict

from link.domain.state import Components, Processes


class EntityConfig(TypedDict):
    components: list[Components]
    is_tainted: bool
    process: Processes
