from unittest.mock import MagicMock, create_autospec

import pytest

from link.base import Base
from link.adapters.datajoint.gateway import DataJointGateway
from link.use_cases import REQUEST_MODELS, USE_CASES, RepositoryLinkFactory
from link.adapters.datajoint import local_table


def test_if_subclass_of_base():
    assert issubclass(OriginalController, Base)


@pytest.fixture
def restriction():
    return "restriction"


@pytest.fixture
def dummy_repo_link_factory():
    return create_autospec(RepositoryLinkFactory, instance=True)


@pytest.fixture
def dummy_output_port():
    return MagicMock(name="dummy_output_port")


@pytest.fixture
def use_case_spies(dummy_repo_link_factory, dummy_output_port):
    return {n: MagicMock(USE_CASES[n](dummy_repo_link_factory, dummy_output_port)) for n in USE_CASES}


@pytest.fixture
def request_model_cls_spies():
    return {n: MagicMock(REQUEST_MODELS[n]) for n in USE_CASES}


@pytest.fixture
def gateway_spies(identifiers):
    spies = {}
    for name in ["source", "local"]:
        spy = create_autospec(DataJointGateway, instance=True)
        spy.get_identifiers_in_restriction.return_value = identifiers
        spies[name] = spy
    return spies


@pytest.fixture
def controller(use_case_spies, request_model_cls_spies, gateway_spies):
    class LocalTableController(local_table.LocalTableController):
        pass

    return LocalTableController(use_case_spies, request_model_cls_spies, gateway_spies)


class TestInit:
    def test_if_use_cases_are_stored_as_instance_attribute(self, controller, use_case_spies):
        assert controller.use_cases is use_case_spies

    def test_if_request_model_classes_are_stored_as_instance_attribute(self, controller, request_model_cls_spies):
        assert controller.request_model_classes is request_model_cls_spies

    def test_if_gateways_are_stored_as_instance_attribute(self, controller, gateway_spies):
        assert controller.gateways is gateway_spies


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
    def request_model_cls_spy(self, request_model_cls_spies, method_name):
        return request_model_cls_spies[method_name]

    @pytest.fixture
    def use_case_spy(self, use_case_spies, method_name):
        return use_case_spies[method_name]

    def test_if_restriction_is_converted_into_identifiers(self, gateway_spy, restriction):
        gateway_spy.get_identifiers_in_restriction.assert_called_once_with(restriction)

    def test_if_initialization_of_request_model_is_correct(self, request_model_cls_spy, identifiers):
        request_model_cls_spy.assert_called_once_with(identifiers)

    def test_if_use_case_is_called_with_request_model(self, use_case_spy, request_model_cls_spy):
        use_case_spy.assert_called_once_with(request_model_cls_spy.return_value)


class TestRefresh:
    def test_if_initialization_of_request_model_is_correct(self, controller, request_model_cls_spies):
        controller.refresh()
        request_model_cls_spies["refresh"].assert_called_once_with()

    def test_if_use_case_is_called_with_request_model(self, controller, use_case_spies, request_model_cls_spies):
        controller.refresh()
        use_case_spies["refresh"].assert_called_once_with(request_model_cls_spies["refresh"].return_value)
