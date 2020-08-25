from unittest.mock import MagicMock
from abc import ABC

import pytest

from link.use_cases import RepositoryLink, base
from link.base import Base


def test_if_response_model_is_subclass_of_abc():
    assert issubclass(base.ResponseModel, ABC)


@pytest.fixture
def repo_link_factory_spy():
    return MagicMock(name="repo_link_factory_spy", return_value="link")


@pytest.fixture
def output_port_spy():
    return MagicMock(name="output_port_spy")


@pytest.fixture
def use_case(repo_link_factory_spy, output_port_spy):
    class UseCase(base.UseCase):
        def execute(self, repo_link: RepositoryLink, *args, **kwargs):
            pass

    UseCase.execute = MagicMock(name=UseCase.__name__ + ".execute", return_value="output")
    return UseCase(repo_link_factory_spy, output_port_spy)


def test_if_subclass_of_base():
    assert issubclass(base.UseCase, Base)


class TestInit:
    def test_if_repo_link_factory_is_stored_as_instance_attribute(self, use_case, repo_link_factory_spy):
        assert use_case.repo_link_factory is repo_link_factory_spy

    def test_if_output_port_is_stored_as_instance_attribute(self, use_case, output_port_spy):
        assert use_case.output_port is output_port_spy


class TestCall:
    def test_if_repo_link_factory_is_called_correctly(self, use_case, repo_link_factory_spy):
        use_case()
        repo_link_factory_spy.assert_called_once_with()

    def test_if_call_to_execute_method_is_correct(self, use_case):
        use_case("arg", kwarg="kwarg")
        use_case.execute.assert_called_once_with("link", "arg", kwarg="kwarg")

    def test_if_call_to_output_port_is_correct(self, use_case, output_port_spy):
        use_case()
        output_port_spy.assert_called_once_with("output")
