import pytest

from link.adapters.dj_gateway import LocalGateway, OutboundGateway
from link.adapters.gateway import AbstractOutboundGateway


@pytest.fixture
def gateway_cls():
    return OutboundGateway


def test_if_subclass_of_non_source_gateway():
    assert issubclass(OutboundGateway, LocalGateway)


def test_if_subclass_of_abstract_outbound_gateway():
    assert issubclass(OutboundGateway, AbstractOutboundGateway)


def test_deletion_approved_identifiers(gateway, identifiers):
    assert gateway.deletion_approved_identifiers == [identifiers[0]]


def test_if_deletion_is_approved_in_gateway(table_proxy, gateway, primary_keys, identifiers):
    gateway.approve_deletion(identifiers)
    table_proxy.approve_deletion.assert_called_once_with(primary_keys)
