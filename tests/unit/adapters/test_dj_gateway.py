from unittest.mock import MagicMock

import pytest

from link.adapters import dj_gateway
from link.adapters import gateway as abstract_gateway


@pytest.fixture
def primary_keys():
    return [dict(a=1, b=2.5, c="Hello world!"), dict(a=5, b=5.12, c="Goodbye world!"), dict(a=12, b=14.64, c="Yo!")]


@pytest.fixture
def table(primary_keys):
    return MagicMock(name="table", primary_keys=primary_keys)


@pytest.fixture
def gateway(gateway_cls, table):
    return gateway_cls(table)


@pytest.fixture
def identifiers():
    return [
        "53eece2e8abc80f75d06f0e08da5da9a88210116",
        "9fde7aa1c88f25abbbc39d184352acf451dff348",
        "e41a74018879ec8fe0dc0d402a58234a9cfc9789",
    ]


class TestGateway:
    def test_if_subclass_of_abstract_gateway(self):
        assert issubclass(dj_gateway.Gateway, abstract_gateway.AbstractGateway)

    @pytest.fixture
    def gateway_cls(self):
        class Gateway(dj_gateway.Gateway):
            pass

        return Gateway

    def test_if_table_is_stored_as_instance_attribute(self, gateway, table):
        assert gateway.table is table

    def test_identifiers_property(self, gateway, identifiers):
        assert gateway.identifiers == identifiers

    def test_fetch(self, primary_keys, table, gateway, identifiers):
        gateway.fetch(identifiers)
        table.fetch.assert_called_once_with(primary_keys)

    def test_delete(self, primary_keys, table, gateway, identifiers):
        gateway.delete(identifiers)
        table.delete.assert_called_once_with(primary_keys)

    def test_insert(self, primary_keys, table, gateway, identifiers):
        gateway.insert(identifiers)
        table.insert.assert_called_once_with(primary_keys)

    def test_if_transaction_is_started_in_table(self, table, gateway):
        gateway.start_transaction()
        table.start_transaction.assert_called_once_with()

    def test_if_transaction_is_committed_in_table(self, table, gateway):
        gateway.commit_transaction()
        table.commit_transaction.assert_called_once_with()

    def test_if_transaction_is_cancelled_in_table(self, table, gateway):
        gateway.cancel_transaction()
        table.cancel_transaction.assert_called_once_with()


class TestFlaggedGateway:
    def test_if_subclass_of_gateway(self):
        assert issubclass(dj_gateway.FlaggedGateway, dj_gateway.Gateway)

    def test_if_subclass_of_abstract_flagged_gateway(self):
        assert issubclass(dj_gateway.FlaggedGateway, abstract_gateway.AbstractFlaggedGateway)

    @pytest.fixture
    def gateway_cls(self):
        class FlaggedGateway(dj_gateway.FlaggedGateway):
            pass

        return FlaggedGateway

    @pytest.fixture
    def table(self, table, primary_keys):
        table.deletion_requested = [primary_keys[0], primary_keys[1]]
        table.deletion_approved = [primary_keys[0]]
        return table

    def test_deletion_requested_flags(self, gateway, identifiers):
        assert gateway.deletion_requested_flags == [identifiers[0], identifiers[1]]

    def test_deletion_approved_flags(self, gateway, identifiers):
        assert gateway.deletion_approved_flags == [identifiers[0]]


class TestSourceGateway:
    def test_if_subclass_of_flagged_gateway(self):
        assert issubclass(dj_gateway.SourceGateway, dj_gateway.FlaggedGateway)

    def test_if_subclass_of_abstract_source_gateway(self):
        assert issubclass(dj_gateway.SourceGateway, abstract_gateway.AbstractSourceGateway)


class TestOutboundGateway:
    def test_if_subclass_of_flagged_gateway(self):
        assert issubclass(dj_gateway.OutboundGateway, dj_gateway.FlaggedGateway)

    def test_if_subclass_of_abstract_outbound_gateway(self):
        assert issubclass(dj_gateway.OutboundGateway, abstract_gateway.AbstractOutboundGateway)


class TestLocalGateway:
    def test_if_subclass_of_flagged_gateway(self):
        assert issubclass(dj_gateway.LocalGateway, dj_gateway.FlaggedGateway)

    def test_if_subclass_of_abstract_local_gateway(self):
        assert issubclass(dj_gateway.LocalGateway, abstract_gateway.AbstractLocalGateway)
