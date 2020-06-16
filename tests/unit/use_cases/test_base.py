from unittest.mock import MagicMock, call

import pytest

from link.use_cases import base


@pytest.fixture
def output_port():
    output_port = MagicMock(name="output_port")
    output_port.__repr__ = MagicMock(name="output_port.__repr__", return_value="output_port")
    return output_port


@pytest.fixture
def execute_call():
    return call("arg1", "arg2", kwarg1="kwarg1", kwarg2="kwarg2")


class TestInitializationUseCase:
    @pytest.fixture
    def use_case(self, output_port):
        class UseCase(base.InitializationUseCase):
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


class TestUseCase:
    @pytest.fixture
    def use_case_cls(self):
        class UseCase(base.UseCase):
            initialize_local = MagicMock(name="initialize_local")
            initialize_source = MagicMock(name="initialize_source")
            execute = MagicMock(name="execute")
            requires_local = requires_source = False

        return UseCase

    @pytest.fixture
    def use_case(self, use_case_cls, output_port):
        return use_case_cls(output_port)

    def test_if_initialize_local_is_none_by_default(self):
        assert base.UseCase.initialize_local is None

    def test_if_initialize_source_is_none_by_default(self):
        assert base.UseCase.initialize_source is None

    def test_if_local_side_is_initialized_if_required_when_calling_use_case(self, use_case):
        use_case.requires_local = True
        use_case()
        use_case.initialize_local.assert_called_once_with()
        use_case.initialize_source.assert_not_called()

    def test_if_source_side_is_initialized_if_required_when_calling_use_case(self, use_case):
        use_case.requires_source = True
        use_case()
        use_case.initialize_source.assert_called_once_with()
        use_case.initialize_local.assert_not_called()

    def test_if_both_sides_are_initialized_if_required_when_calling_use_case(self, use_case):
        use_case.requires_local = True
        use_case.requires_source = True
        use_case()
        use_case.initialize_local.assert_called_once_with()
        use_case.initialize_source.assert_called_once_with()

    def test_if_execute_method_of_super_class_is_correctly_called_when_calling_use_case(
        self, use_case_cls, output_port, execute_call
    ):
        class FakeInitializationUseCase(base.InitializationUseCase):
            execute = MagicMock(name="execute")
            requires_local = requires_source = False

        class UseCase(base.UseCase, FakeInitializationUseCase):
            requires_local = requires_source = None

        UseCase(output_port)(*execute_call.args, **execute_call.kwargs)
        FakeInitializationUseCase.execute.assert_called_once_with(*execute_call.args, **execute_call.kwargs)
