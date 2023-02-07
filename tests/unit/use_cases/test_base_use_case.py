from abc import ABC
from functools import partial
from unittest.mock import MagicMock, create_autospec

import pytest

from dj_link.base import Base
from dj_link.use_cases import RepositoryLink, base


def test_if_request_model_is_subclass_of_abc():
    assert issubclass(base.AbstractRequestModel, ABC)


def test_if_response_model_is_subclass_of_abc():
    assert issubclass(base.AbstractResponseModel, ABC)


@pytest.fixture
def repo_link_factory_spy():
    return MagicMock(name="repo_link_factory_spy", return_value="link")


@pytest.fixture
def output_port_spy():
    return MagicMock(name="output_port_spy")


@pytest.fixture
def use_case(repo_link_factory_spy, output_port_spy):
    class UseCase(base.AbstractUseCase):
        name = "test"

        def execute(self, repo_link: RepositoryLink, *args, **kwargs):
            pass

    UseCase.execute = MagicMock(name=UseCase.__name__ + ".execute", return_value="output")
    return UseCase(repo_link_factory_spy, output_port_spy)


def test_if_subclass_of_base():
    assert issubclass(base.AbstractUseCase, Base)


class TestInit:
    def test_if_repo_link_factory_is_stored_as_instance_attribute(self, use_case, repo_link_factory_spy):
        assert use_case.repo_link_factory is repo_link_factory_spy

    def test_if_output_port_is_stored_as_instance_attribute(self, use_case, output_port_spy):
        assert use_case.output_port is output_port_spy


class TestCall:
    @pytest.fixture
    def dummy_request_model(self):
        return create_autospec(base.AbstractRequestModel, instance=True)

    @pytest.fixture
    def call(self, use_case, dummy_request_model):
        use_case(dummy_request_model)

    @pytest.mark.usefixtures("call")
    def test_if_repo_link_factory_is_called_correctly(self, repo_link_factory_spy):
        repo_link_factory_spy.assert_called_once_with()

    @pytest.mark.usefixtures("call")
    def test_if_call_to_execute_method_is_correct(self, use_case, dummy_request_model):
        use_case.execute.assert_called_once_with("link", dummy_request_model)

    @pytest.mark.usefixtures("call")
    def test_if_call_to_output_port_is_correct(self, output_port_spy):
        output_port_spy.assert_called_once_with("output")

    def test_if_logged_messages_are_correct(self, is_correct_log, use_case, dummy_request_model):
        messages = [
            "Executing test use-case...",
            "Finished executing test use-case!",
        ]
        assert is_correct_log(base.LOGGER, partial(use_case, dummy_request_model), messages)
