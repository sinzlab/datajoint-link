from __future__ import annotations

import pytest

from link.domain.link import create_entity
from link.domain.state import Components, Processes, State, states
from tests.assignments import create_identifier

from .types import EntityConfig


@pytest.mark.parametrize(
    ("entity_config", "state"),
    [
        ({"components": [Components.SOURCE], "is_tainted": False, "process": Processes.NONE}, states.Unshared),
        (
            {"components": [Components.SOURCE, Components.OUTBOUND], "is_tainted": False, "process": Processes.PULL},
            states.Activated,
        ),
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": False,
                "process": Processes.PULL,
            },
            states.Received,
        ),
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": False,
                "process": Processes.NONE,
            },
            states.Shared,
        ),
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": True,
                "process": Processes.NONE,
            },
            states.Tainted,
        ),
        (
            {"components": [Components.SOURCE, Components.OUTBOUND], "is_tainted": True, "process": Processes.NONE},
            states.Deprecated,
        ),
        (
            {"components": [Components.SOURCE, Components.OUTBOUND], "is_tainted": True, "process": Processes.PULL},
            states.Activated,
        ),
        (
            {
                "components": [Components.SOURCE, Components.OUTBOUND, Components.LOCAL],
                "is_tainted": True,
                "process": Processes.PULL,
            },
            states.Received,
        ),
    ],
)
def test_entity_creation(entity_config: EntityConfig, state: type[State]) -> None:
    entity = create_entity(create_identifier("1"), **entity_config)
    expected = (state, entity_config["is_tainted"], entity_config["process"])
    assert (entity.state, entity.is_tainted, entity.current_process) == expected
