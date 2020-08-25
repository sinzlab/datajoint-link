from dataclasses import is_dataclass

import pytest

from link.use_cases.base import ResponseModel
from link.use_cases.pull import PullResponseModel


def test_if_dataclass():
    assert is_dataclass(PullResponseModel)


def test_if_subclass_of_response_model():
    assert issubclass(PullResponseModel, ResponseModel)


@pytest.fixture
def requested():
    return {"identifier" + str(i) for i in range(10)}


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
