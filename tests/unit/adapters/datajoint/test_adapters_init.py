from unittest.mock import MagicMock

import pytest

from dj_link.adapters.datajoint import AbstractTableFacadeLink, DataJointGatewayLink, initialize_adapters
from dj_link.adapters.datajoint.gateway import DataJointGateway
from dj_link.adapters.datajoint.identification import IdentificationTranslator
from dj_link.adapters.datajoint.presenter import Presenter, ViewModel
from dj_link.base import Base
from dj_link.globals import REPOSITORY_NAMES


@pytest.fixture(params=REPOSITORY_NAMES)
def repo_type(request):
    return request.param


class TestDataJointGatewayLink:
    @pytest.fixture()
    def gateway_stubs(self):
        gateway_stubs = {}
        for repo_type in REPOSITORY_NAMES:
            gateway_stub = MagicMock(name=repo_type + "_gateway_stub", spec=DataJointGateway)
            gateway_stubs[repo_type] = gateway_stub
        return gateway_stubs

    @pytest.fixture()
    def gateway_link(self, gateway_stubs):
        return DataJointGatewayLink(**{kind: gateway for kind, gateway in gateway_stubs.items()})

    def test_if_subclass_of_base(self):
        assert issubclass(DataJointGatewayLink, Base)

    def test_if_gateway_is_stored_as_instance_attribute(self, repo_type, gateway_link, gateway_stubs):
        assert getattr(gateway_link, repo_type) is gateway_stubs[repo_type]


class TestInitializeAdapters:
    @pytest.fixture()
    def table_facade_link_stub(self):
        return MagicMock(name="table_facade_link_stub", spec=AbstractTableFacadeLink)

    @pytest.fixture()
    def initialize_adapters_return_value(self, table_facade_link_stub):
        return initialize_adapters(table_facade_link_stub)

    @pytest.fixture()
    def gateway_link(self, initialize_adapters_return_value):
        return initialize_adapters_return_value[0]

    @pytest.fixture()
    def view_model(self, initialize_adapters_return_value):
        return initialize_adapters_return_value[1]

    @pytest.fixture()
    def presenter(self, initialize_adapters_return_value):
        return initialize_adapters_return_value[2]

    def test_if_gateway_link_is_returned(self, gateway_link):
        assert isinstance(gateway_link, DataJointGatewayLink)

    def test_if_gateway_in_link_is_datajoint_gateway(self, repo_type, gateway_link):
        assert isinstance(getattr(gateway_link, repo_type), DataJointGateway)

    def test_if_gateways_are_associated_with_correct_table_facade(
        self, repo_type, gateway_link, table_facade_link_stub
    ):
        assert getattr(gateway_link, repo_type).table_facade is getattr(table_facade_link_stub, repo_type)

    def test_if_translators_of_gateways_are_identification_translators(self, repo_type, gateway_link):
        assert isinstance(getattr(gateway_link, repo_type).translator, IdentificationTranslator)

    def test_if_the_same_translator_is_used_in_all_gateways(self, gateway_link):
        assert (
            gateway_link.source.translator is gateway_link.outbound.translator
            and gateway_link.outbound.translator is gateway_link.local.translator
        )

    def test_if_view_model_is_returned(self, view_model):
        assert isinstance(view_model, ViewModel)

    def test_if_presenter_is_returned(self, presenter):
        assert isinstance(presenter, Presenter)

    def test_if_view_model_of_presenter_is_correct(self, view_model, presenter):
        assert view_model is presenter.view_model
