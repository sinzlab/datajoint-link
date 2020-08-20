from unittest.mock import create_autospec

import pytest

from link.adapters.datajoint.gateway import DataJointGateway
from link.use_cases import USE_CASES
from link.adapters.datajoint import local_table


@pytest.fixture
def restriction():
    return "restriction"


def test_if_pull_use_case_is_none():
    assert local_table.LocalTableController.pull_use_case is None


def test_if_delete_use_case_is_none():
    assert local_table.LocalTableController.delete_use_case is None


def test_if_source_gateway_is_none():
    assert local_table.LocalTableController.source_gateway is None


def test_if_local_gateway_is_none():
    assert local_table.LocalTableController.local_gateway is None


@pytest.fixture
def pull_use_case_spy():
    return create_autospec(USE_CASES["pull"], instance=True)


@pytest.fixture
def delete_use_case_spy():
    return create_autospec(USE_CASES["delete"], instance=True)


@pytest.fixture
def source_gateway_spy(identifiers):
    source_gateway = create_autospec(DataJointGateway, instance=True)
    source_gateway.get_identifiers_in_restriction.return_value = identifiers
    return source_gateway


@pytest.fixture
def local_gateway_spy(identifiers):
    local_gateway = create_autospec(DataJointGateway, instance=True)
    local_gateway.get_identifiers_in_restriction.return_value = identifiers
    return local_gateway


@pytest.fixture
def controller_cls(pull_use_case_spy, delete_use_case_spy, source_gateway_spy, local_gateway_spy):
    class LocalTableController(local_table.LocalTableController):
        pass

    LocalTableController.__qualname__ = LocalTableController.__name__
    LocalTableController.pull_use_case = pull_use_case_spy
    LocalTableController.delete_use_case = delete_use_case_spy
    LocalTableController.source_gateway = source_gateway_spy
    LocalTableController.local_gateway = local_gateway_spy
    return LocalTableController


@pytest.fixture
def controller(controller_cls):
    return controller_cls()


class TestPull:
    def test_if_restriction_is_converted_into_identifiers(self, source_gateway_spy, controller, restriction):
        controller.pull(restriction)
        source_gateway_spy.get_identifiers_in_restriction.assert_called_once_with(restriction)

    def test_if_pull_use_case_is_called_with_identifiers(self, identifiers, pull_use_case_spy, controller, restriction):
        controller.pull(restriction)
        pull_use_case_spy.assert_called_once_with(identifiers)


class TestDelete:
    def test_if_restriction_is_converted_into_identifiers(self, local_gateway_spy, controller, restriction):
        controller.delete(restriction)
        local_gateway_spy.get_identifiers_in_restriction.assert_called_once_with(restriction)

    def test_if_delete_use_case_is_called_with_identifiers(
        self, identifiers, delete_use_case_spy, controller, restriction
    ):
        controller.delete(restriction)
        delete_use_case_spy.assert_called_once_with(identifiers)


def test_repr(controller):
    assert repr(controller) == "LocalTableController()"
