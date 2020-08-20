from unittest.mock import create_autospec

import pytest

from link.base import Base
from link.adapters.datajoint.gateway import DataJointGateway
from link.use_cases import USE_CASES
from link.adapters.datajoint import local_table


@pytest.fixture
def restriction():
    return "restriction"


def test_if_subclass_of_base():
    assert issubclass(local_table.LocalTableController, Base)


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
def controller(pull_use_case_spy, delete_use_case_spy, source_gateway_spy, local_gateway_spy):
    class LocalTableController(local_table.LocalTableController):
        pass

    return LocalTableController(pull_use_case_spy, delete_use_case_spy, source_gateway_spy, local_gateway_spy)


class TestInit:
    def test_if_pull_use_case_is_stored_as_instance_attribute(self, controller, pull_use_case_spy):
        assert controller.pull_use_case is pull_use_case_spy

    def test_if_delete_use_case_is_stored_as_instance_attribute(self, controller, delete_use_case_spy):
        assert controller.delete_use_case is delete_use_case_spy

    def test_if_source_gateway_is_stored_as_instance_attribute(self, controller, source_gateway_spy):
        assert controller.source_gateway is source_gateway_spy

    def test_if_local_gateway_is_stored_as_instance_attribute(self, controller, local_gateway_spy):
        assert controller.local_gateway is local_gateway_spy


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
