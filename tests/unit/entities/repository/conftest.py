from unittest.mock import MagicMock

import pytest


@pytest.fixture
def data(identifiers):
    return {identifier: "data" + str(i) for i, identifier in enumerate(identifiers)}


@pytest.fixture
def gateway(gateway, data):
    gateway.fetch.return_value = data
    return gateway
