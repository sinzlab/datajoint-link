from unittest.mock import MagicMock

import pytest

from link.use_cases import RepositoryLink, base


@pytest.fixture
def repo_link_factory_spy():
    name = "repo_link_factory_spy"
    link_factory_spy = MagicMock(name=name, return_value="link")
    link_factory_spy.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return link_factory_spy


@pytest.fixture
def output_port_spy():
    name = "output_port_spy"
    output_port_spy = MagicMock(name=name)
    output_port_spy.__repr__ = MagicMock(name=name + ".__repr", return_value=name)
    return output_port_spy


@pytest.fixture
def use_case(repo_link_factory_spy, output_port_spy):
    class UseCase(base.UseCase):
        def execute(self, repo_link: RepositoryLink, *args, **kwargs):
            pass

    UseCase.__qualname__ = UseCase.__name__
    UseCase.execute = MagicMock(name=UseCase.__name__ + ".execute", return_value="output")
    return UseCase(repo_link_factory_spy, output_port_spy)


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


def test_repr(use_case):
    assert repr(use_case) == "UseCase(repo_link_factory=repo_link_factory_spy, output_port=output_port_spy)"
