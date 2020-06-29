import pytest


@pytest.fixture
def entity_cls(entity_cls):
    class FlaggedEntity(entity_cls):
        def __init__(self, identifier):
            super().__init__(identifier)
            self.deletion_requested = False
            self.deletion_approved = False

    return FlaggedEntity


@pytest.fixture
def new_selected_identifiers(new_identifiers, indexes):
    return [new_identifiers[index] for index in indexes]


@pytest.fixture
def request_deletion_of_present_entities(entities, selected_identifiers, request_deletion):
    request_deletion(selected_identifiers, entities)


@pytest.fixture
def request_deletion():
    def _request_deletion(identifiers, entities):
        for entity in entities:
            if entity.identifier in identifiers:
                entity.deletion_requested = True

    return _request_deletion


@pytest.fixture
def request_deletion_of_new_entities(new_entities, new_selected_identifiers, request_deletion):
    request_deletion(new_selected_identifiers, new_entities)
