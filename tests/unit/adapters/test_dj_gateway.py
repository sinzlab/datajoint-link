from unittest.mock import MagicMock

import pytest

from link.adapters import dj_gateway
from link.adapters import gateway as abstract_gateway


@pytest.fixture
def n_attrs():
    return 5


@pytest.fixture
def n_entities():
    return 6


@pytest.fixture
def n_primary_attrs():
    return 3


@pytest.fixture
def attrs(n_attrs):
    return ["a" + str(i) for i in range(n_attrs)]


@pytest.fixture
def primary_attrs(n_primary_attrs, attrs):
    return attrs[:n_primary_attrs]


@pytest.fixture
def secondary_attrs(n_primary_attrs, attrs):
    return attrs[n_primary_attrs:]


@pytest.fixture
def attr_values(n_attrs, n_entities):
    return [["v" + str(i) + str(j) for j in range(n_attrs)] for i in range(n_entities)]


@pytest.fixture
def primary_attr_values(n_primary_attrs, attr_values):
    return [entity_attr_values[:n_primary_attrs] for entity_attr_values in attr_values]


@pytest.fixture
def secondary_attr_values(n_primary_attrs, attr_values):
    return [entity_attr_values[n_primary_attrs:] for entity_attr_values in attr_values]


@pytest.fixture
def primary_keys(primary_attrs, primary_attr_values):
    return [
        {pa: pav for pa, pav in zip(primary_attrs, primary_entity_attr_values)}
        for primary_entity_attr_values in primary_attr_values
    ]


@pytest.fixture
def entities(attrs, attr_values):
    return [{a: av for a, av in zip(attrs, entity_attr_values)} for entity_attr_values in attr_values]


@pytest.fixture
def table(primary_attrs, primary_keys, entities):
    name = "table"
    table = MagicMock(
        name=name,
        primary_attr_names=primary_attrs,
        primary_keys=primary_keys,
        deletion_requested=[primary_keys[0], primary_keys[1]],
        deletion_approved=[primary_keys[0]],
    )
    table.fetch.return_value = entities
    table.__repr__ = MagicMock(name=name + "__repr__", return_value=name)
    return table


@pytest.fixture
def identifiers():
    return [
        "62aad6b1b90f0613ac14b3ed0f5ecbf1c3cca448",
        "2d78c5aafa6200eb909bfc7b4b5b8f07284ad734",
        "e359f33515accad6b2e967135ee713cd17a200c9",
        "f62ac0bf9e4f661e617b935c76076bdfb5845cf3",
        "9f1d3a454a02283d83d2da2b02ce8950fb683d14",
        "f355683595377472c79473009e2cef9259254359",
    ]


@pytest.fixture
def data(identifiers, secondary_attrs, secondary_attr_values):
    return {
        identifier: {k: v for k, v in zip(secondary_attrs, secondary_entity_attr_values)}
        for identifier, secondary_entity_attr_values in zip(identifiers, secondary_attr_values)
    }


@pytest.fixture
def gateway(gateway_cls, table):
    return gateway_cls(table)


class TestReadOnlyGateway:
    def test_if_subclass_of_abstract_read_only_gateway(self):
        assert issubclass(dj_gateway.ReadOnlyGateway, abstract_gateway.AbstractSourceGateway)

    @pytest.fixture
    def gateway_cls(self):
        return dj_gateway.ReadOnlyGateway

    def test_if_table_is_stored_as_instance_attribute(self, gateway, table):
        assert gateway.table is table

    def test_identifiers_property(self, gateway, identifiers):
        assert gateway.identifiers == identifiers

    def test_if_data_is_fetched_from_table(self, primary_keys, table, gateway, identifiers):
        gateway.fetch(identifiers)
        table.fetch.assert_called_once_with(primary_keys)

    def test_if_data_is_returned(self, primary_keys, gateway, identifiers, data):
        assert gateway.fetch(identifiers) == data

    def test_repr(self, gateway):
        assert repr(gateway) == "ReadOnlyGateway(table)"


class TestGateway:
    def test_if_subclass_of_read_only_gateway(self):
        assert issubclass(dj_gateway.Gateway, dj_gateway.ReadOnlyGateway)

    def test_if_subclass_of_abstract_gateway(self):
        assert issubclass(dj_gateway.Gateway, abstract_gateway.AbstractNonSourceGateway)

    @pytest.fixture
    def gateway_cls(self):
        return dj_gateway.Gateway

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
