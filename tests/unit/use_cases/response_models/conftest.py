import pytest


@pytest.fixture
def requested(create_identifiers):
    return set(create_identifiers(10))
