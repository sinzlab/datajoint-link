import pytest


@pytest.fixture
def n_identifiers():
    return 10


@pytest.fixture
def identifiers(n_identifiers):
    return ["ID" + str(i) for i in range(n_identifiers)]


@pytest.fixture
def entity_cls():
    class Entity:
        def __init__(self, identifier):
            self.identifier = identifier

        def __repr__(self):
            return f"{self.__class__.__qualname__}({self.identifier})"

    return Entity


@pytest.fixture
def entities(identifiers, entity_cls):
    return [entity_cls(identifier) for identifier in identifiers]
