from unittest.mock import MagicMock

import pytest

from link.adapters.datajoint import AbstractTableFacadeLink, DataJointGatewayLink, initialize
from link.adapters.datajoint.gateway import DataJointGateway
from link.adapters.datajoint.identification import IdentificationTranslator
from link.base import Base


@pytest.fixture(params=["source", "outbound", "local"])
def kind(request):
    return request.param


class TestDataJointGatewayLink:
    @pytest.fixture
    def gateway_stubs(self):
        gateway_stubs = {}
        for kind in ("source", "outbound", "local"):
            gateway_stub = MagicMock(name=kind + "_gateway_stub", spec=DataJointGateway)
            gateway_stubs[kind] = gateway_stub
        return gateway_stubs

    @pytest.fixture
    def gateway_link(self, gateway_stubs):
        return DataJointGatewayLink(**{kind: gateway for kind, gateway in gateway_stubs.items()})

    def test_if_subclass_of_base(self):
        assert issubclass(DataJointGatewayLink, Base)

    def test_if_gateway_is_stored_as_instance_attribute(self, kind, gateway_link, gateway_stubs):
        assert getattr(gateway_link, kind) is gateway_stubs[kind]


class TestInitialize:
    @pytest.fixture
    def table_facade_link_stub(self):
        return MagicMock(name="table_facade_link_stub", spec=AbstractTableFacadeLink)

    @pytest.fixture
    def gateway_link(self, table_facade_link_stub):
        return initialize(table_facade_link_stub)

    def test_if_gateway_link_is_returned(self, gateway_link):
        assert isinstance(gateway_link, DataJointGatewayLink)

    def test_if_gateway_in_link_is_datajoint_gateway(self, kind, gateway_link):
        assert isinstance(getattr(gateway_link, kind), DataJointGateway)

    def test_if_gateways_are_associated_with_correct_table_facade(self, kind, gateway_link, table_facade_link_stub):
        assert getattr(gateway_link, kind).table_facade is getattr(table_facade_link_stub, kind)

    def test_if_translators_of_gateways_are_identification_translators(self, kind, gateway_link):
        assert isinstance(getattr(gateway_link, kind).translator, IdentificationTranslator)

    def test_if_translators_are_associated_with_correct_table_facade(self, kind, gateway_link, table_facade_link_stub):
        assert getattr(gateway_link, kind).translator.table_facade is getattr(table_facade_link_stub, kind)
