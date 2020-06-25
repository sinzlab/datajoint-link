from unittest.mock import MagicMock

import pytest

from link.entities import repository


def test_if_gateway_is_none_by_default():
    assert repository.Repository.gateway is None


def test_if_entity_creator_is_none_by_default():
    assert repository.Repository.entity_creator is None


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


@pytest.fixture
def gateway(identifiers):
    gateway = MagicMock(name="gateway")
    gateway.get_identifiers.return_value = identifiers.copy()
    return gateway


@pytest.fixture
def entity_cls():
    class EntityClass:
        def __init__(self, address, identifier):
            self.address = address
            self.identifier = identifier

        def __eq__(self, other):
            return self.address == other.address and self.identifier == other.identifier

    return EntityClass


@pytest.fixture
def entities(address, entity_cls, identifiers):
    return [entity_cls(address, i) for i in identifiers]


@pytest.fixture
def entity_creator(entities):
    entity_creator = MagicMock(name="entity_creator")
    entity_creator.create_entities.return_value = entities.copy()
    return entity_creator


@pytest.fixture
def repo_cls(gateway, entity_cls, entity_creator):
    class Repository(repository.Repository):
        __qualname__ = "Repository"

    Repository.gateway = gateway
    Repository.entity_creator = entity_creator
    return Repository


@pytest.fixture
def repo(repo_cls, address):
    return repo_cls(address)


class TestInit:
    def test_if_address_is_stored_as_instance_attribute(self, repo, address):
        assert repo.address is address

    @pytest.mark.usefixtures("repo")
    def test_if_entity_creator_gets_called_correctly(self, entity_creator):
        entity_creator.create_entities.assert_called_once_with()

    def test_if_in_transaction_is_false_by_default(self, repo):
        assert repo.in_transaction is False


class TestIdentifiersProperty:
    def test_if_identifiers_are_returned(self, repo, identifiers):
        assert repo.identifiers == identifiers

    def test_if_identifiers_are_copy(self, repo, identifiers):
        del repo.identifiers[0]
        assert repo.identifiers == identifiers


def test_entities_property(repo, entities):
    assert repo.entities == entities


@pytest.fixture
def indexes():
    return 0, 4


@pytest.fixture
def selected_identifiers(identifiers, indexes):
    return [identifiers[i] for i in indexes]


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


@pytest.fixture
def execute_while_ignoring_error(repo):
    def _execute_while_ignoring_error(method, arg, error):
        try:
            getattr(repo, method)(arg)
        except error:
            pass

    return _execute_while_ignoring_error


@pytest.fixture
def test_if_entities_are_not_processed_after_processing_failed_in_gateway(
    entities, repo, gateway, execute_while_ignoring_error
):
    def _test_if_entities_are_not_processed_after_processing_failed_in_gateway(method, arg, error):
        getattr(gateway, method).side_effect = RuntimeError
        execute_while_ignoring_error(method, arg, error)
        assert repo.entities == entities

    return _test_if_entities_are_not_processed_after_processing_failed_in_gateway


class TestDelete:
    @pytest.fixture
    def remaining_entities(self, entities, selected_identifiers):
        return [e for e in entities if e.identifier not in selected_identifiers]

    def test_if_entities_are_deleted_in_gateway(self, repo, gateway, selected_identifiers):
        repo.delete(selected_identifiers)
        gateway.delete.assert_called_once_with(selected_identifiers)

    def test_if_entities_are_not_deleted_after_deletion_failed_in_gateway(
        self, test_if_entities_are_not_processed_after_processing_failed_in_gateway, selected_identifiers
    ):
        test_if_entities_are_not_processed_after_processing_failed_in_gateway(
            "delete", selected_identifiers, RuntimeError
        )

    def test_if_correct_entities_are_deleted(self, repo, selected_identifiers, remaining_entities):
        repo.delete(selected_identifiers)
        assert repo.entities == remaining_entities


class TestInsert:
    @pytest.fixture
    def new_entities(self, address, entity_cls):
        return [entity_cls(address, "ID" + str(10 + i)) for i in range(3)]

    def test_if_entities_are_inserted_in_gateway(self, repo, gateway, new_entities):
        repo.insert(new_entities)
        gateway.insert.assert_called_once_with([e.identifier for e in new_entities])

    def test_if_entities_are_not_inserted_after_insertion_failed_in_gateway(
        self, test_if_entities_are_not_processed_after_processing_failed_in_gateway, new_entities
    ):
        test_if_entities_are_not_processed_after_processing_failed_in_gateway("insert", new_entities, RuntimeError)

    def test_if_entities_are_inserted(self, entities, repo, new_entities):
        repo.insert(new_entities)
        assert repo.entities == entities + new_entities


class TestContains:
    def test_if_not_in_is_true_if_entity_is_not_contained(self, repo):
        assert "ID999" not in repo

    def test_if_in_is_true_if_entity_is_contained(self, repo, identifiers):
        assert identifiers[0] in repo


@pytest.fixture
def start_transaction(repo):
    repo.start_transaction()


@pytest.fixture
def execute_with_faulty_gateway(repo, gateway):
    def _execute_with_faulty_gateway(method):
        getattr(gateway, method).side_effect = RuntimeError
        try:
            getattr(repo, method)()
        except RuntimeError:
            pass

    return _execute_with_faulty_gateway


class TestStartTransaction:
    @pytest.mark.usefixtures("start_transaction")
    def test_if_starting_transaction_while_in_transaction_raises_runtime_error(self, repo):
        with pytest.raises(RuntimeError):
            repo.start_transaction()

    @pytest.mark.usefixtures("start_transaction")
    def test_if_transaction_is_started_in_gateway(self, repo, gateway):
        gateway.start_transaction.assert_called_once_with()

    @pytest.mark.usefixtures("start_transaction")
    def test_if_transaction_is_not_started_in_gateway_if_repository_is_in_transaction(self, repo, gateway):
        with pytest.raises(RuntimeError):
            repo.start_transaction()
        gateway.start_transaction.assert_called_once_with()

    @pytest.mark.usefixtures("start_transaction")
    def test_if_repository_is_put_into_transaction(self, repo):
        assert repo.in_transaction is True

    def test_if_repository_is_not_put_into_transaction_if_starting_transaction_fails_in_gateway(
        self, repo, execute_with_faulty_gateway
    ):
        execute_with_faulty_gateway("start_transaction")
        assert repo.in_transaction is False


class TestStartTransactionWhenEmpty:
    @pytest.fixture
    def identifiers(self):
        return list()

    def test_if_repository_is_put_into_transaction_if_repository_is_empty(self, repo):
        repo.start_transaction()
        assert repo.in_transaction is True


class TestCommitTransaction:
    @pytest.fixture
    def commit_transaction(self, repo):
        repo.commit_transaction()

    def test_if_committing_while_not_in_transaction_raises_runtime_error(self, repo):
        with pytest.raises(RuntimeError):
            repo.commit_transaction()

    @pytest.mark.usefixtures("start_transaction", "commit_transaction")
    def test_if_transaction_is_committed_in_gateway(self, repo, gateway):
        gateway.commit_transaction.assert_called_once_with()

    def test_if_transaction_is_not_committed_in_gateway_if_repository_is_not_in_transaction(self, repo, gateway):
        with pytest.raises(RuntimeError):
            repo.commit_transaction()
        gateway.commit_transaction.assert_not_called()

    @pytest.mark.usefixtures("start_transaction", "commit_transaction")
    def test_if_repository_is_put_out_of_transaction(self, repo):
        assert repo.in_transaction is False

    @pytest.mark.usefixtures("start_transaction")
    def test_if_repository_is_not_put_out_of_transaction_if_committing_transaction_fails_in_gateway(
        self, repo, execute_with_faulty_gateway
    ):
        execute_with_faulty_gateway("commit_transaction")
        assert repo.in_transaction is True


class TestCancelTransaction:
    @pytest.fixture
    def cancel_transaction(self, repo):
        repo.cancel_transaction()

    def test_if_cancelling_while_not_in_transaction_raises_runtime_error(self, repo):
        with pytest.raises(RuntimeError):
            repo.cancel_transaction()

    @pytest.mark.usefixtures("start_transaction", "cancel_transaction")
    def test_if_transaction_is_cancelled_in_gateway(self, gateway):
        gateway.cancel_transaction.assert_called_once_with()

    def test_if_transaction_is_not_cancelled_in_gateway_if_repository_is_not_in_transaction(self, repo, gateway):
        with pytest.raises(RuntimeError):
            repo.cancel_transaction()
        gateway.cancel_transaction.assert_not_called()

    @pytest.mark.usefixtures("start_transaction", "cancel_transaction")
    def test_if_repository_is_put_out_of_transaction(self, repo):
        assert repo.in_transaction is False

    @pytest.mark.usefixtures("start_transaction")
    def test_if_repository_is_not_put_out_of_transaction_if_cancelling_transaction_fails_in_gateway(
        self, repo, execute_with_faulty_gateway
    ):
        execute_with_faulty_gateway("cancel_transaction")
        assert repo.in_transaction is True

    @pytest.mark.usefixtures("start_transaction")
    def test_if_changes_are_rolled_back_if_transaction_is_cancelled(self, repo, selected_identifiers, entities):
        repo.delete(selected_identifiers)
        repo.cancel_transaction()
        assert repo.entities == entities


class TestTransaction:
    def test_if_transaction_is_started(self, repo):
        with repo.transaction():
            assert repo.in_transaction is True

    def test_if_transaction_is_committed(self, repo):
        with repo.transaction():
            pass
        assert repo.in_transaction is False

    def test_if_transaction_is_cancelled_if_error_is_raised(self, repo, selected_identifiers, entities):
        try:
            with repo.transaction():
                repo.delete(selected_identifiers)
                raise RuntimeError
        except RuntimeError:
            pass
        assert repo.entities == entities

    def test_if_error_raised_during_transaction_is_reraised_after(self, repo):
        with pytest.raises(RuntimeError):
            with repo.transaction():
                raise RuntimeError


def test_len(repo):
    assert len(repo) == 10


def test_repr(repo):
    assert repr(repo) == "Repository(address)"


def test_iter(identifiers, repo):
    assert [i for i in repo] == identifiers
