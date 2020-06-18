from unittest.mock import MagicMock

import pytest

from link.entities.repository import Repository


@pytest.fixture
def address():
    return MagicMock(name="address")


@pytest.fixture
def identifiers():
    return ["ID" + str(i) for i in range(10)]


@pytest.fixture
def gateway(identifiers):
    gateway = MagicMock(name="gateway")
    gateway.get_identifiers.return_value = identifiers
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
def repository_cls(gateway, entity_cls):
    Repository.gateway = gateway
    Repository.entity_cls = entity_cls
    return Repository


@pytest.fixture
def repository(repository_cls, address):
    return repository_cls(address)


class TestInit:
    def test_if_address_is_stored_as_instance_attribute(self, repository, address):
        assert repository.address is address

    @pytest.mark.usefixtures("repository")
    def test_if_gateway_gets_called_correctly(self, gateway):
        gateway.get_identifiers.assert_called_once_with()

    def test_if_in_transaction_is_false_by_default(self, repository):
        assert repository.in_transaction is False


def test_entities_property(repository, entities):
    assert repository.entities == entities


class TestList:
    def test_if_correct_identifiers_are_returned(self, repository, identifiers):
        assert repository.list() == identifiers


@pytest.fixture
def indexes():
    return 0, 4


@pytest.fixture
def selected_identifiers(identifiers, indexes):
    return [identifiers[i] for i in indexes]


class TestFetch:
    @pytest.fixture
    def fetched_entities(self, repository, selected_identifiers):
        return repository.fetch(selected_identifiers)

    @pytest.mark.usefixtures("fetched_entities")
    def test_if_gateway_is_correctly_called(self, gateway, selected_identifiers):
        gateway.fetch.assert_called_once_with(selected_identifiers)

    def test_if_correct_entities_are_fetched(self, entities, indexes, fetched_entities):
        expected_entities = [entities[index] for index in indexes]
        assert fetched_entities == expected_entities

    def test_if_getting_non_existing_entity_raises_error(self, repository):
        with pytest.raises(KeyError):
            repository.fetch("ID999")

    def test_if_error_is_raised_before_gateway_is_called(self, repository, gateway):
        try:
            repository.fetch("ID999")
        except KeyError:
            gateway.fetch.assert_not_called()


class TestDelete:
    @pytest.fixture
    def remaining_identifiers(self, identifiers, selected_identifiers):
        return [i for i in identifiers if i not in selected_identifiers]

    def test_if_gateway_is_correctly_called(self, repository, gateway, selected_identifiers):
        repository.delete(selected_identifiers)
        gateway.delete.assert_called_once_with(selected_identifiers)

    def test_correct_entities_are_deleted(self, repository, selected_identifiers, remaining_identifiers):
        repository.delete(selected_identifiers)
        assert repository.list() == remaining_identifiers

    def test_if_deleting_non_existing_entity_raises_error(self, repository):
        with pytest.raises(KeyError):
            repository.delete("ID999")

    def test_if_error_is_raised_before_gateway_is_called(self, repository, gateway):
        try:
            repository.delete("ID999")
        except KeyError:
            gateway.delete.assert_not_called()

    def test_if_repository_is_rolled_back_if_delete_fails_in_gateway(
        self, repository, identifiers, gateway, selected_identifiers
    ):
        gateway.delete.side_effect = RuntimeError
        repository.delete(selected_identifiers)
        assert repository.list() == identifiers


class TestContains:
    def test_if_not_in_is_true_if_entity_is_not_contained(self, repository):
        assert "ID999" not in repository

    def test_if_in_is_true_if_entity_is_contained(self, repository, identifiers):
        assert identifiers[0] in repository


@pytest.fixture
def start_transaction(repository):
    repository.start_transaction()


@pytest.fixture
def execute_with_faulty_gateway(repository, gateway):
    def _execute_with_faulty_gateway(method):
        getattr(gateway, method).side_effect = RuntimeError
        try:
            getattr(repository, method)()
        except RuntimeError:
            pass

    return _execute_with_faulty_gateway


class TestStartTransaction:
    @pytest.mark.usefixtures("start_transaction")
    def test_if_starting_transaction_while_in_transaction_raises_runtime_error(self, repository):
        with pytest.raises(RuntimeError):
            repository.start_transaction()

    @pytest.mark.usefixtures("start_transaction")
    def test_if_transaction_is_started_in_gateway(self, repository, gateway):
        gateway.start_transaction.assert_called_once_with()

    @pytest.mark.usefixtures("start_transaction")
    def test_if_transaction_is_not_started_in_gateway_if_repository_is_in_transaction(self, repository, gateway):
        with pytest.raises(RuntimeError):
            repository.start_transaction()
        gateway.start_transaction.assert_called_once_with()

    @pytest.mark.usefixtures("start_transaction")
    def test_if_repository_is_put_into_transaction(self, repository):
        assert repository.in_transaction is True

    def test_if_repository_is_not_put_into_transaction_if_starting_transaction_fails_in_gateway(
        self, repository, execute_with_faulty_gateway
    ):
        execute_with_faulty_gateway("start_transaction")
        assert repository.in_transaction is False


class TestCommitTransaction:
    @pytest.fixture
    def commit_transaction(self, repository):
        repository.commit_transaction()

    def test_if_committing_while_not_in_transaction_raises_runtime_error(self, repository):
        with pytest.raises(RuntimeError):
            repository.commit_transaction()

    @pytest.mark.usefixtures("start_transaction", "commit_transaction")
    def test_if_transaction_is_committed_in_gateway(self, repository, gateway):
        gateway.commit_transaction.assert_called_once_with()

    def test_if_transaction_is_not_committed_in_gateway_if_repository_is_not_in_transaction(self, repository, gateway):
        with pytest.raises(RuntimeError):
            repository.commit_transaction()
        gateway.commit_transaction.assert_not_called()

    @pytest.mark.usefixtures("start_transaction", "commit_transaction")
    def test_if_repository_is_put_out_of_transaction(self, repository):
        assert repository.in_transaction is False

    @pytest.mark.usefixtures("start_transaction")
    def test_if_repository_is_not_put_out_of_transaction_if_committing_transaction_fails_in_gateway(
        self, repository, execute_with_faulty_gateway
    ):
        execute_with_faulty_gateway("commit_transaction")
        assert repository.in_transaction is True


class TestCancelTransaction:
    @pytest.fixture
    def cancel_transaction(self, repository):
        repository.cancel_transaction()

    def test_if_cancelling_while_not_in_transaction_raises_runtime_error(self, repository):
        with pytest.raises(RuntimeError):
            repository.cancel_transaction()

    @pytest.mark.usefixtures("start_transaction", "cancel_transaction")
    def test_if_transaction_is_cancelled_in_gateway(self, repository, gateway):
        gateway.cancel_transaction.assert_called_once_with()

    def test_if_transaction_is_not_cancelled_in_gateway_if_repository_is_not_in_transaction(self, repository, gateway):
        with pytest.raises(RuntimeError):
            repository.cancel_transaction()
        gateway.cancel_transaction.assert_not_called()

    @pytest.mark.usefixtures("start_transaction", "cancel_transaction")
    def test_if_repository_is_put_out_of_transaction(self, repository):
        assert repository.in_transaction is False

    @pytest.mark.usefixtures("start_transaction")
    def test_if_repository_is_not_put_out_of_transaction_if_cancelling_transaction_fails_in_gateway(
        self, repository, execute_with_faulty_gateway
    ):
        execute_with_faulty_gateway("cancel_transaction")
        assert repository.in_transaction is True

    @pytest.mark.usefixtures("start_transaction")
    def test_if_changes_are_rolled_back_if_transaction_is_cancelled(self, repository, selected_identifiers, entities):
        repository.delete(selected_identifiers)
        repository.cancel_transaction()
        assert repository.entities == entities
