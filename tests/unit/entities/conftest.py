import pytest


@pytest.fixture
def address_cls():
    class Address:
        def __init__(self, name):
            self.name = name

        def __eq__(self, other):
            return self.name == other.name

        def __repr__(self):
            return self.name

    return Address


@pytest.fixture
def address(address_cls):
    return address_cls("address")


@pytest.fixture
def identifiers():
    return ["ID" + str(i) for i in range(10)]
