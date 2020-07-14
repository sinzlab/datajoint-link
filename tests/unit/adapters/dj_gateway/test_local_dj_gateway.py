import pytest

from link.adapters.dj_gateway import SourceGateway, LocalGateway
from link.adapters.gateway import AbstractLocalGateway


def test_if_subclass_of_source_gateway():
    assert issubclass(LocalGateway, SourceGateway)


def test_if_subclass_of_abstract_local_gateway():
    assert issubclass(LocalGateway, AbstractLocalGateway)


@pytest.fixture
def gateway_cls():
    return LocalGateway


def test_deletion_requested_identifiers(gateway, identifiers):
    assert gateway.deletion_requested_identifiers == [identifiers[0], identifiers[1]]


def test_delete(primary_keys, table_proxy, gateway, identifiers):
    gateway.delete(identifiers)
    table_proxy.delete.assert_called_once_with(primary_keys)


def test_insert(primary_keys, table_proxy, gateway, identifiers):
    gateway.insert(identifiers)
    table_proxy.insert.assert_called_once_with(primary_keys)


def test_if_transaction_is_started_in_table_proxy(table_proxy, gateway):
    gateway.start_transaction()
    table_proxy.start_transaction.assert_called_once_with()


def test_if_transaction_is_committed_in_table_proxy(table_proxy, gateway):
    gateway.commit_transaction()
    table_proxy.commit_transaction.assert_called_once_with()


def test_if_transaction_is_cancelled_in_table_proxy(table_proxy, gateway):
    gateway.cancel_transaction()
    table_proxy.cancel_transaction.assert_called_once_with()
