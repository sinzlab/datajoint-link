import pytest

from link.adapters.dj_gateway import SourceGateway
from link.adapters.gateway import AbstractSourceGateway


def test_if_subclass_of_abstract_source_gateway():
    assert issubclass(SourceGateway, AbstractSourceGateway)


@pytest.fixture
def gateway_cls():
    return SourceGateway


def test_if_table_proxy_is_stored_as_instance_attribute(gateway, table_proxy):
    assert gateway.table_proxy is table_proxy


def test_identifiers_property(gateway, identifiers):
    assert gateway.identifiers == identifiers


class TestGetIdentifiersInRestriction:
    def test_if_primary_keys_in_restriction_are_fetched(self, gateway, table_proxy, restriction):
        gateway.get_identifiers_in_restriction(restriction)
        table_proxy.get_primary_keys_in_restriction.assert_called_once_with(restriction)

    def test_if_identifiers_in_restriction_are_returned(self, gateway, identifiers, restriction):
        assert gateway.get_identifiers_in_restriction(restriction) == identifiers


class TestFetch:
    def test_if_data_is_fetched_from_table_proxy(self, primary_keys, table_proxy, gateway, identifiers):
        gateway.fetch(identifiers)
        table_proxy.fetch.assert_called_once_with(primary_keys)

    def test_if_data_is_returned(self, primary_keys, gateway, identifiers, data):
        assert gateway.fetch(identifiers) == data


def test_repr(gateway):
    assert repr(gateway) == "SourceGateway(table_proxy)"
