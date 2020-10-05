import pytest

from dj_link.use_cases import RESPONSE_MODELS, USE_CASES
from dj_link.use_cases.base import AbstractUseCase


@pytest.fixture(params=USE_CASES)
def use_case_name(request):
    return request.param


@pytest.fixture
def use_case_cls(use_case_name):
    return USE_CASES[use_case_name]


def test_if_subclass_of_use_case(use_case_cls):
    assert issubclass(use_case_cls, AbstractUseCase)


@pytest.fixture
def response_model_cls(use_case_name):
    return RESPONSE_MODELS[use_case_name]


def test_of_response_model_class_is_correct(use_case_cls, response_model_cls):
    assert use_case_cls.response_model_cls is response_model_cls
