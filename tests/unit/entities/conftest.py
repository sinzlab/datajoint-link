from unittest.mock import MagicMock

import pytest

from link.entities.repository import Entity


@pytest.fixture
def identifiers():
    return ["identifier" + str(i) for i in range(10)]


@pytest.fixture
def identifier(identifiers):
    return identifiers[0]


@pytest.fixture
def entities(identifiers):
    return {identifier: Entity(identifier) for identifier in identifiers}


@pytest.fixture
def entity(identifier, entities):
    return entities[identifier]


@pytest.fixture
def data():
    return "data"


@pytest.fixture
def gateway_spy(data):
    name = "gateway_spy"
    gateway_spy = MagicMock(name=name)
    gateway_spy.fetch.return_value = data
    gateway_spy.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return gateway_spy
