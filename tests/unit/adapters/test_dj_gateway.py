from unittest.mock import MagicMock

import pytest

from link.adapters import dj_gateway
from link.adapters import gateway as abstract_gateway


@pytest.fixture
def primary_attr_names():
    return ["p0", "p1", "p2"]


@pytest.fixture
def primary_attr_data():
    return [(1, 2.5, "Hello world!"), (5, 5.12, "Goodbye world!"), (12, 14.64, "Yo!")]


@pytest.fixture
def secondary_attr_names():
    return ["s0", "s1"]


@pytest.fixture
def secondary_attr_data():
    return [(4.5, 12), (7.51, 64), (5.123, 34)]


@pytest.fixture
def primary_keys(primary_attr_names, primary_attr_data):
    primary_keys = []
    for data in primary_attr_data:
        primary_keys.append({k: v for k, v in zip(primary_attr_names, data)})
    return primary_keys


@pytest.fixture
def entities(primary_keys, secondary_attr_names, secondary_attr_data):
    entities = []
    for primary_key, data in zip(primary_keys, secondary_attr_data):
        entity = {k: v for k, v in zip(secondary_attr_names, data)}
        entity.update(primary_key)
        entities.append(entity)
    return entities


@pytest.fixture
def table(primary_attr_names, primary_keys, entities):
    table = MagicMock(name="table", primary_attr_names=primary_attr_names, primary_keys=primary_keys)
    table.fetch.return_value = entities
    return table


@pytest.fixture
def identifiers():
    return [
        "edbaaf579279b704bd776d22a35b47a7477702de",
        "25d0f885e46404fa3774b32f614e086c4afa7f4f",
        "c8a5fac2306aab1cc2d12459c575bde80c403a02",
    ]


@pytest.fixture
def data(identifiers, secondary_attr_names, secondary_attr_data):
    return {
        identifier: {k: v for k, v in zip(secondary_attr_names, data)}
        for identifier, data in zip(identifiers, secondary_attr_data)
    }


@pytest.fixture
def gateway(gateway_cls, table):
    return gateway_cls(table)


class TestReadOnlyGateway:
    def test_if_subclass_of_abstract_read_only_gateway(self):
        assert issubclass(dj_gateway.ReadOnlyGateway, abstract_gateway.AbstractReadOnlyGateway)

    @pytest.fixture
    def gateway_cls(self):
        class Gateway(dj_gateway.ReadOnlyGateway):
            pass

        return Gateway

    def test_if_table_is_stored_as_instance_attribute(self, gateway, table):
        assert gateway.table is table

    def test_identifiers_property(self, gateway, identifiers):
        assert gateway.identifiers == identifiers

    def test_if_data_is_fetched_from_table(self, primary_keys, table, gateway, identifiers):
        gateway.fetch(identifiers)
        table.fetch.assert_called_once_with(primary_keys)

    def test_if_data_is_returned(self, primary_keys, gateway, identifiers, data):
        assert gateway.fetch(identifiers) == data


class TestGateway:
    def test_if_subclass_of_read_only_gateway(self):
        assert issubclass(dj_gateway.Gateway, dj_gateway.ReadOnlyGateway)

    def test_if_subclass_of_abstract_gateway(self):
        assert issubclass(dj_gateway.Gateway, abstract_gateway.AbstractGateway)

    @pytest.fixture
    def gateway_cls(self):
        class FlaggedGateway(dj_gateway.Gateway):
            pass

        return FlaggedGateway

    @pytest.fixture
    def table(self, table, primary_keys):
        table.deletion_requested = [primary_keys[0], primary_keys[1]]
        table.deletion_approved = [primary_keys[0]]
        return table

    def test_deletion_requested_identifiers(self, gateway, identifiers):
        assert gateway.deletion_requested_identifiers == [identifiers[0], identifiers[1]]

    def test_deletion_approved_identifiers(self, gateway, identifiers):
        assert gateway.deletion_approved_identifiers == [identifiers[0]]

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


class TestSourceGateway:
    def test_if_subclass_of_read_only_gateway(self):
        assert issubclass(dj_gateway.SourceGateway, dj_gateway.ReadOnlyGateway)

    def test_if_subclass_of_abstract_source_gateway(self):
        assert issubclass(dj_gateway.SourceGateway, abstract_gateway.AbstractSourceGateway)


class TestOutboundGateway:
    @pytest.fixture
    def gateway_cls(self):
        return dj_gateway.OutboundGateway

    def test_if_subclass_of_gateway(self):
        assert issubclass(dj_gateway.OutboundGateway, dj_gateway.Gateway)

    def test_if_subclass_of_abstract_outbound_gateway(self):
        assert issubclass(dj_gateway.OutboundGateway, abstract_gateway.AbstractOutboundGateway)

    def test_if_deletion_is_approved_in_gateway(self, table, gateway, primary_keys, identifiers):
        gateway.approve_deletion(identifiers)
        table.approve_deletion.assert_called_once_with(primary_keys)


class TestLocalGateway:
    def test_if_subclass_of_gateway(self):
        assert issubclass(dj_gateway.LocalGateway, dj_gateway.Gateway)

    def test_if_subclass_of_abstract_local_gateway(self):
        assert issubclass(dj_gateway.LocalGateway, abstract_gateway.AbstractLocalGateway)
