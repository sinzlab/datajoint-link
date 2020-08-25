import pytest


@pytest.fixture
def requested():
    return {"identifier" + str(i) for i in range(10)}
