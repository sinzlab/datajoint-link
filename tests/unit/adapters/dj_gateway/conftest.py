from unittest.mock import MagicMock

import pytest


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
def entities(primary_keys):
    return [MagicMock(name="entity" + str(i), primary_key=primary_key) for i, primary_key in enumerate(primary_keys)]


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
def data(identifiers, entities):
    return {identifier: entity for identifier, entity in zip(identifiers, entities)}


@pytest.fixture
def gateway(gateway_cls, table_proxy):
    return gateway_cls(table_proxy)
