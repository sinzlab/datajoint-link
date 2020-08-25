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


def test_n_requested_property(model, requested):
    assert model.n_requested == len(requested)


def test_n_valid_property(model, valid):
    assert model.n_valid == len(valid)


def test_n_invalid_property(model, invalid):
    assert model.n_invalid == len(invalid)
