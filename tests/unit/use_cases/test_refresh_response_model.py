from dataclasses import is_dataclass

import pytest

from link.use_cases.base import ResponseModel
from link.use_cases.refresh import RefreshResponseModel


def test_if_dataclass():
    assert is_dataclass(RefreshResponseModel)


def test_if_subclass_of_response_model():
    assert issubclass(RefreshResponseModel, ResponseModel)


@pytest.fixture
def refreshed():
    return ["identifiers" + str(i) for i in range(10)]


@pytest.fixture
def model(refreshed):
    return RefreshResponseModel(refreshed=refreshed)


def test_n_refreshed_property(model, refreshed):
    assert model.n_refreshed == len(refreshed)
