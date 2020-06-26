from unittest.mock import MagicMock

import pytest

from link.entities.domain import Address


@pytest.fixture
def address():
    address = MagicMock(name="address")
    address.__repr__ = MagicMock(return_value="address", spec=Address)
    return address


@pytest.fixture
def identifiers():
    return ["ID" + str(i) for i in range(10)]


@pytest.fixture
def entities(identifiers):
    return [MagicMock(name="entity_" + identifier, identifier=identifier) for identifier in identifiers]
