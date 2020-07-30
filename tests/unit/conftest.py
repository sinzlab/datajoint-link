import pytest


@pytest.fixture
def n_identifiers():
    return 10


@pytest.fixture
def identifiers(n_identifiers):
    return ["ID" + str(i) for i in range(n_identifiers)]
