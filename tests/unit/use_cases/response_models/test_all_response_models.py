from dataclasses import is_dataclass

import pytest

from link.use_cases.base import ResponseModel
from link.use_cases.delete import DeleteResponseModel
from link.use_cases.refresh import RefreshResponseModel
from link.use_cases.pull import PullResponseModel


RESPONSE_MODELS = [DeleteResponseModel, RefreshResponseModel, PullResponseModel]


@pytest.fixture(params=RESPONSE_MODELS)
def model_cls(request):
    return request.param


def test_if_dataclass(model_cls):
    assert is_dataclass(model_cls)


def test_if_subclass_of_response_model(model_cls):
    assert issubclass(model_cls, ResponseModel)
