from dataclasses import is_dataclass

import pytest

from dj_link.use_cases import RESPONSE_MODELS
from dj_link.use_cases.base import AbstractResponseModel


@pytest.fixture(params=RESPONSE_MODELS.values())
def model_cls(request):
    return request.param


def test_if_dataclass(model_cls):
    assert is_dataclass(model_cls)


def test_if_subclass_of_response_model(model_cls):
    assert issubclass(model_cls, AbstractResponseModel)
