import pytest

from link.use_cases.pull import PullResponseModel


@pytest.fixture
def valid():
    return {"identifier" + str(i) for i in range(5)}


@pytest.fixture
def invalid():
    return {"identifier" + str(i) for i in range(5, 10)}


@pytest.fixture
def model(requested, valid, invalid):
    return PullResponseModel(requested=requested, valid=valid, invalid=invalid)


@pytest.mark.parametrize(
    "name,length", [("requested", 10), ("valid", 5), ("invalid", 5)],
)
def test_n_property(model, name, length):
    assert getattr(model, "n_" + name) == length
