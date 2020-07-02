import pytest

from link.entities import repository


def test_if_gateway_is_none_by_default():
    assert repository.ReadOnlyRepository.gateway is None


def test_if_entity_creator_is_none_by_default():
    assert repository.ReadOnlyRepository.entity_creator is None


@pytest.fixture
def repo_cls(gateway, entity_creator):
    class ReadOnlyRepository(repository.ReadOnlyRepository):
        __qualname__ = "ReadOnlyRepository"

    return ReadOnlyRepository


class TestInit:
    @pytest.mark.usefixtures("repo")
    def test_if_entity_creator_gets_called_correctly(self, entity_creator):
        entity_creator.create_entities.assert_called_once_with()


class TestIdentifiersProperty:
    def test_if_identifiers_are_returned(self, repo, identifiers):
        assert repo.identifiers == identifiers

    def test_if_identifiers_are_copy(self, repo, identifiers):
        del repo.identifiers[0]
        assert repo.identifiers == identifiers


class TestEntitiesProperty:
    def test_if_entities_are_returned(self, repo, entities):
        assert repo.entities == entities

    def test_if_entities_are_copy(self, repo, entities):
        del repo.entities[0]
        assert repo.entities == entities


class TestFetch:
    @pytest.fixture
    def fetched_entities(self, repo, selected_identifiers):
        return repo.fetch(selected_identifiers)

    @pytest.mark.usefixtures("fetched_entities")
    def test_if_entities_are_fetched_from_gateway(self, gateway, selected_identifiers):
        gateway.fetch.assert_called_once_with(selected_identifiers)

    def test_if_correct_entities_are_fetched(self, entities, indexes, fetched_entities):
        expected_entities = [entities[index] for index in indexes]
        assert fetched_entities == expected_entities


class TestContains:
    def test_if_not_in_is_true_if_entity_is_not_contained(self, repo):
        assert "ID999" not in repo

    def test_if_in_is_true_if_entity_is_contained(self, repo, identifiers):
        assert identifiers[0] in repo


def test_len(repo):
    assert len(repo) == 10


def test_repr(repo):
    assert repr(repo) == "ReadOnlyRepository()"


def test_iter(identifiers, repo):
    assert [i for i in repo] == identifiers


def test_getitem(identifiers, entities, repo):
    assert [repo[identifier] for identifier in identifiers] == entities
