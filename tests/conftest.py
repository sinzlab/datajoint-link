from __future__ import annotations

from typing import Iterable, Protocol, Union

import pytest


class IdentifierCreator(Protocol):
    def __call__(self, spec: Union[int, Iterable[int]]) -> list[str]:
        ...


@pytest.fixture
def create_identifiers() -> IdentifierCreator:
    def _create_identifiers(spec: Union[int, Iterable[int]]) -> list[str]:
        if isinstance(spec, int):
            indexes = list(range(spec))
        elif isinstance(spec, Iterable):
            indexes = list(spec)
        else:
            raise RuntimeError("Invalid type for 'spec'")
        return ["identifier" + str(i) for i in indexes]

    return _create_identifiers
