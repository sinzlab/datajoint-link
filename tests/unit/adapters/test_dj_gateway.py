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
def table_proxy(primary_attrs, primary_keys, entities):
    name = "table_proxy"
    table_proxy = MagicMock(
        name=name,
        primary_attr_names=primary_attrs,
        primary_keys=primary_keys,
        deletion_requested=[primary_keys[0], primary_keys[1]],
        deletion_approved=[primary_keys[0]],
    )
    table_proxy.get_primary_keys_in_restriction.return_value = primary_keys
    table_proxy.fetch.return_value = entities
    table_proxy.__repr__ = MagicMock(name=name + "__repr__", return_value=name)
    return table_proxy


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
def gateway(gateway_cls, table_proxy):
    return gateway_cls(table_proxy)


class TestSourceGateway:
    def test_if_subclass_of_abstract_source_gateway(self):
        assert issubclass(dj_gateway.SourceGateway, abstract_gateway.AbstractSourceGateway)

    @pytest.fixture
    def gateway_cls(self):
        return dj_gateway.SourceGateway

    @pytest.fixture
    def restriction(self):
        return "restriction"

    def test_if_table_proxy_is_stored_as_instance_attribute(self, gateway, table_proxy):
        assert gateway.table_proxy is table_proxy

    def test_identifiers_property(self, gateway, identifiers):
        assert gateway.identifiers == identifiers

    def test_if_primary_keys_in_restriction_are_fetched_when_getting_identifiers_in_restriction(
        self, gateway, table_proxy, restriction
    ):
        gateway.get_identifiers_in_restriction(restriction)
        table_proxy.get_primary_keys_in_restriction.assert_called_once_with(restriction)

    def test_if_identifiers_in_restriction_are_returned(self, gateway, identifiers, restriction):
        assert gateway.get_identifiers_in_restriction(restriction) == identifiers

    def test_if_data_is_fetched_from_table_proxy(self, primary_keys, table_proxy, gateway, identifiers):
        gateway.fetch(identifiers)
        table_proxy.fetch.assert_called_once_with(primary_keys)

    def test_if_data_is_returned(self, primary_keys, gateway, identifiers, data):
        assert gateway.fetch(identifiers) == data

    def test_repr(self, gateway):
        assert repr(gateway) == "SourceGateway(table_proxy)"


class TestNonSourceGateway:
    def test_if_subclass_of_source_gateway(self):
        assert issubclass(dj_gateway.NonSourceGateway, dj_gateway.SourceGateway)

    def test_if_subclass_of_abstract_non_source_gateway(self):
        assert issubclass(dj_gateway.NonSourceGateway, abstract_gateway.AbstractNonSourceGateway)

    @pytest.fixture
    def gateway_cls(self):
        return dj_gateway.NonSourceGateway

    def test_deletion_requested_identifiers(self, gateway, identifiers):
        assert gateway.deletion_requested_identifiers == [identifiers[0], identifiers[1]]

    def test_delete(self, primary_keys, table_proxy, gateway, identifiers):
        gateway.delete(identifiers)
        table_proxy.delete.assert_called_once_with(primary_keys)

    def test_insert(self, primary_keys, table_proxy, gateway, identifiers):
        gateway.insert(identifiers)
        table_proxy.insert.assert_called_once_with(primary_keys)

    def test_if_transaction_is_started_in_table_proxy(self, table_proxy, gateway):
        gateway.start_transaction()
        table_proxy.start_transaction.assert_called_once_with()

    def test_if_transaction_is_committed_in_table_proxy(self, table_proxy, gateway):
        gateway.commit_transaction()
        table_proxy.commit_transaction.assert_called_once_with()

    def test_if_transaction_is_cancelled_in_table_proxy(self, table_proxy, gateway):
        gateway.cancel_transaction()
        table_proxy.cancel_transaction.assert_called_once_with()


class TestOutboundGateway:
    @pytest.fixture
    def gateway_cls(self):
        return dj_gateway.OutboundGateway

    def test_if_subclass_of_non_source_gateway(self):
        assert issubclass(dj_gateway.OutboundGateway, dj_gateway.NonSourceGateway)

    def test_if_subclass_of_abstract_outbound_gateway(self):
        assert issubclass(dj_gateway.OutboundGateway, abstract_gateway.AbstractOutboundGateway)

    def test_deletion_approved_identifiers(self, gateway, identifiers):
        assert gateway.deletion_approved_identifiers == [identifiers[0]]

    def test_if_deletion_is_approved_in_gateway(self, table_proxy, gateway, primary_keys, identifiers):
        gateway.approve_deletion(identifiers)
        table_proxy.approve_deletion.assert_called_once_with(primary_keys)


class TestLocalGateway:
    def test_if_subclass_of_non_source_gateway(self):
        assert issubclass(dj_gateway.LocalGateway, dj_gateway.NonSourceGateway)

    def test_if_subclass_of_abstract_local_gateway(self):
        assert issubclass(dj_gateway.LocalGateway, abstract_gateway.AbstractLocalGateway)
