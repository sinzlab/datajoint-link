from unittest.mock import MagicMock, call

import pytest

from link.entities import repository, source


@pytest.fixture
def fake_repository_cls(identifiers):
    class FakeRepository(repository.Repository):
        pass

    return FakeRepository


@pytest.fixture
def source_repo_cls(fake_repository_cls):
    class SourceRepository(source.SourceRepository, fake_repository_cls):
        __qualname__ = "SourceRepository"

    return SourceRepository


@pytest.fixture
def outbound_repo():
    name = "outbound_repo"
    outbound_repo = MagicMock(name=name)
    outbound_repo.__repr__ = MagicMock(name="outbound_repo.__init__", return_value=name)
    return outbound_repo


@pytest.fixture
def source_repo(source_repo_cls, address, outbound_repo):
    return source_repo_cls(address, outbound_repo)


class TestInit:
    @pytest.fixture
    def fake_repository_cls(self, fake_repository_cls):
        fake_repository_cls.__init__ = MagicMock(name="FakeRepository.__init__")
        return fake_repository_cls

    @pytest.mark.usefixtures("source_repo")
    def test_if_super_class_gets_initialized(self, fake_repository_cls, address):
        fake_repository_cls.__init__.assert_called_once_with(address)

    def test_if_outbound_repository_gets_stored_as_class_attribute(self, outbound_repo, source_repo):
        assert source_repo.outbound_repo is outbound_repo


class TestDelete:
    @pytest.fixture
    def fake_repository_cls(self, fake_repository_cls):
        fake_repository_cls.delete = MagicMock(name="FakeRepository.delete")
        return fake_repository_cls

    def test_if_presence_of_identifiers_in_outbound_repository_is_checked(
        self, identifiers, outbound_repo, source_repo
    ):
        source_repo.delete(identifiers)
        assert outbound_repo.__contains__.mock_calls == [call(i) for i in identifiers]

    @pytest.mark.parametrize("is_contained_index", [0, -1])
    def test_if_runtime_error_is_raised_if_identifier_is_in_outbound_repository(
        self, identifiers, outbound_repo, source_repo, is_contained_index
    ):
        is_contained = [False for _ in range(len(identifiers) - 1)]
        is_contained.insert(is_contained_index, True)
        outbound_repo.__contains__.return_value = is_contained
        with pytest.raises(RuntimeError):
            source_repo.delete(identifiers)

    def test_if_delete_of_super_class_gets_called(self, identifiers, fake_repository_cls, source_repo):
        source_repo.delete(identifiers)
        fake_repository_cls.delete.assert_called_once_with(identifiers)

    def test_if_value_error_is_raised_before_delete_of_super_class_is_called(
        self, identifiers, fake_repository_cls, outbound_repo, source_repo
    ):
        outbound_repo.__contains__.return_value = True
        try:
            source_repo.delete(identifiers)
        except RuntimeError:
            pass
        fake_repository_cls.delete.assert_not_called()


def test_repr(source_repo):
    assert repr(source_repo) == "SourceRepository(address, outbound_repo)"
