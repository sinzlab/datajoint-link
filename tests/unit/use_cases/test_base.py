from unittest.mock import MagicMock, call

import pytest

from link.use_cases import base


@pytest.fixture
def output_port():
    name = "output_port"
    output_port = MagicMock(name=name)
    output_port.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return output_port


@pytest.fixture
def execute_call():
    return call("arg1", "arg2", kwarg1="kwarg1", kwarg2="kwarg2")


class TestInitializationUseCase:
    @pytest.fixture
    def use_case(self, output_port):
        class UseCase(base.UseCase):
            execute = MagicMock(name="execute", return_value="executed!")
            __qualname__ = "UseCase"

        return UseCase(output_port)

    def test_if_output_port_is_stored_as_instance_attribute_when_initializing(self, use_case, output_port):
        assert use_case.output_port is output_port

    def test_if_execute_method_is_correctly_called_when_calling_use_case(self, use_case, execute_call):
        use_case(*execute_call.args, **execute_call.kwargs)
        use_case.execute.assert_called_once_with(*execute_call.args, **execute_call.kwargs)

    def test_if_output_port_is_correctly_called_when_calling_use_case(self, use_case, output_port):
        use_case()
        output_port.assert_called_once_with("executed!")

    def test_repr(self, use_case):
        assert repr(use_case) == "UseCase(output_port)"
