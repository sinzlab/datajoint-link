from unittest.mock import create_autospec

import pytest

from link.base import Base
from link.adapters.datajoint.gateway import DataJointGateway
from link.use_cases import USE_CASES
from link.adapters.datajoint import local_table


def test_if_subclass_of_base():
    assert issubclass(local_table.LocalTableController, Base)


@pytest.fixture
def restriction():
    return "restriction"


@pytest.fixture
def use_case_spies():
    return {n: create_autospec(USE_CASES[n], instance=True) for n in ["pull", "delete"]}


@pytest.fixture
def gateway_spies(identifiers):
    spies = {}
    for name in ["source", "local"]:
        spy = create_autospec(DataJointGateway, instance=True)
        spy.get_identifiers_in_restriction.return_value = identifiers
        spies[name] = spy
    return spies


@pytest.fixture
def controller(use_case_spies, gateway_spies):
    class LocalTableController(local_table.LocalTableController):
        pass

    return LocalTableController(
        use_case_spies["pull"], use_case_spies["delete"], gateway_spies["source"], gateway_spies["local"]
    )


class TestInit:
    def test_if_pull_use_case_is_stored_as_instance_attribute(self, controller, use_case_spies):
        assert controller.pull_use_case is use_case_spies["pull"]

    def test_if_delete_use_case_is_stored_as_instance_attribute(self, controller, use_case_spies):
        assert controller.delete_use_case is use_case_spies["delete"]

    def test_if_source_gateway_is_stored_as_instance_attribute(self, controller, gateway_spies):
        assert controller.source_gateway is gateway_spies["source"]

    def test_if_local_gateway_is_stored_as_instance_attribute(self, controller, gateway_spies):
        assert controller.local_gateway is gateway_spies["local"]


class TestMethod:
    @pytest.fixture(params=["pull", "delete"])
    def method_name(self, request):
        return request.param

    @pytest.fixture(autouse=True)
    def execute(self, controller, method_name, restriction):
        getattr(controller, method_name)(restriction)

    @pytest.fixture
    def gateway_spy(self, gateway_spies, method_name):
        return {"pull": gateway_spies["source"], "delete": gateway_spies["local"]}[method_name]

    @pytest.fixture
    def use_case_spy(self, use_case_spies, method_name):
        return use_case_spies[method_name]

    def test_if_restriction_is_converted_into_identifiers(self, gateway_spy, restriction):
        gateway_spy.get_identifiers_in_restriction.assert_called_once_with(restriction)

    def test_if_use_case_is_called_with_identifiers(self, identifiers, use_case_spy):
        use_case_spy.assert_called_once_with(identifiers)
