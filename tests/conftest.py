from typing import Iterable

import pytest


@pytest.fixture
def create_identifiers():
    def _create_identifiers(spec):
        if isinstance(spec, int):
            indexes = range(spec)
        elif isinstance(spec, Iterable):
            indexes = spec
        else:
            raise RuntimeError("Invalid type for 'spec'")
        return ["identifier" + str(i) for i in indexes]

    return _create_identifiers
