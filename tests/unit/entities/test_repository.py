from unittest.mock import MagicMock

import pytest

from link.entities.repository import Repository


def test_if_gateway_is_none_by_default():
    assert Repository.gateway is None


def test_if_entity_cls_is_none_by_default():
    assert Repository.entity_cls is None


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


class TestIdentifiersProperty:
    def test_if_identifiers_are_returned(self, repository, identifiers):
        assert repository.identifiers == identifiers

    def test_if_identifiers_are_copy(self, repository, identifiers):
        del repository.identifiers[0]
        assert repository.identifiers == identifiers


def test_entities_property(repository, entities):
    assert repository.entities == entities


@pytest.fixture
def indexes():
    return 0, 4


@pytest.fixture
def selected_identifiers(identifiers, indexes):
    return [identifiers[i] for i in indexes]


@pytest.fixture
def execute_while_ignoring_error(repository):
    def _execute_while_ignoring_error(method, arg, error):
        try:
            getattr(repository, method)(arg)
        except error:
            pass

    return _execute_while_ignoring_error


@pytest.fixture
def test_if_error_is_raised_before_gateway_is_called(repository, gateway, execute_while_ignoring_error):
    def _test_if_error_is_raised_before_gateway_is_called(method, arg, error):
        execute_while_ignoring_error(method, arg, error)
        getattr(gateway, method).assert_not_called()

    return _test_if_error_is_raised_before_gateway_is_called


class TestFetch:
    @pytest.fixture
    def fetched_entities(self, repository, selected_identifiers):
        return repository.fetch(selected_identifiers)

    def test_if_trying_to_get_non_existing_entity_raises_key_error(self, repository):
        with pytest.raises(KeyError):
            repository.fetch(["ID999"])

    def test_if_key_error_is_raised_before_gateway_is_called(self, test_if_error_is_raised_before_gateway_is_called):
        test_if_error_is_raised_before_gateway_is_called("fetch", ["ID999"], KeyError)

    @pytest.mark.usefixtures("fetched_entities")
    def test_if_entities_are_fetched_from_gateway(self, gateway, selected_identifiers):
        gateway.fetch.assert_called_once_with(selected_identifiers)

    def test_if_correct_entities_are_fetched(self, entities, indexes, fetched_entities):
        expected_entities = [entities[index] for index in indexes]
        assert fetched_entities == expected_entities


@pytest.fixture
def test_if_entities_are_not_processed_after_processing_failed_in_gateway(
    entities, repository, gateway, execute_while_ignoring_error
):
    def _test_if_entities_are_not_processed_after_processing_failed_in_gateway(method, arg, error):
        getattr(gateway, method).side_effect = RuntimeError
        execute_while_ignoring_error(method, arg, error)
        assert repository.entities == entities

    return _test_if_entities_are_not_processed_after_processing_failed_in_gateway


class TestDelete:
    @pytest.fixture
    def remaining_entities(self, entities, selected_identifiers):
        return [e for e in entities if e.identifier not in selected_identifiers]

    def test_if_trying_to_delete_non_existing_entity_raises_key_error(self, repository):
        with pytest.raises(KeyError):
            repository.delete(["ID999"])

    def test_if_key_error_is_raised_before_gateway_is_called(self, test_if_error_is_raised_before_gateway_is_called):
        test_if_error_is_raised_before_gateway_is_called("delete", ["ID999"], KeyError)

    def test_if_entities_are_deleted_in_gateway(self, repository, gateway, selected_identifiers):
        repository.delete(selected_identifiers)
        gateway.delete.assert_called_once_with(selected_identifiers)

    def test_if_entities_are_not_deleted_after_deletion_failed_in_gateway(
        self, test_if_entities_are_not_processed_after_processing_failed_in_gateway, selected_identifiers
    ):
        test_if_entities_are_not_processed_after_processing_failed_in_gateway(
            "delete", selected_identifiers, RuntimeError
        )

    def test_if_correct_entities_are_deleted(self, repository, selected_identifiers, remaining_entities):
        repository.delete(selected_identifiers)
        assert repository.entities == remaining_entities


class TestInsert:
    @pytest.fixture
    def invalid_address(self, address_cls):
        return address_cls("invalid_address")

    @pytest.fixture
    def new_entities(self, address, entity_cls):
        return [entity_cls(address, "ID" + str(10 + i)) for i in range(3)]

    @pytest.fixture(params=list(range(3)))
    def invalid_entity_index(self, request):
        return request.param

    @pytest.fixture
    def invalidate_address(self, invalid_address, new_entities, invalid_entity_index):
        new_entities[invalid_entity_index].address = invalid_address

    @pytest.fixture
    def invalidate_identifier(self, identifiers, new_entities, invalid_entity_index):
        new_entities[invalid_entity_index].identifier = identifiers[0]

    @pytest.mark.usefixtures("invalidate_address")
    def test_if_trying_to_insert_entity_with_invalid_address_raises_value_error(self, repository, new_entities):
        with pytest.raises(ValueError):
            repository.insert(new_entities)

    @pytest.mark.usefixtures("invalidate_identifier")
    def test_if_trying_to_insert_already_existing_entity_raises_value_error(self, repository, new_entities):
        with pytest.raises(ValueError):
            repository.insert(new_entities)

    @pytest.mark.usefixtures("invalidate_identifier")
    def test_if_value_error_is_raised_before_gateway_is_called(
        self, test_if_error_is_raised_before_gateway_is_called, new_entities
    ):
        test_if_error_is_raised_before_gateway_is_called("insert", new_entities, ValueError)

    def test_if_entities_are_inserted_in_gateway(self, repository, gateway, new_entities):
        repository.insert(new_entities)
        gateway.insert.assert_called_once_with([e.identifier for e in new_entities])

    def test_if_entities_are_not_inserted_after_insertion_failed_in_gateway(
        self, test_if_entities_are_not_processed_after_processing_failed_in_gateway, new_entities
    ):
        test_if_entities_are_not_processed_after_processing_failed_in_gateway("insert", new_entities, RuntimeError)

    def test_if_entities_are_inserted(self, entities, repository, new_entities):
        repository.insert(new_entities)
        assert repository.entities == entities + new_entities


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


class TestStartTransactionWhenEmpty:
    @pytest.fixture
    def identifiers(self):
        return list()

    def test_if_repository_is_put_into_transaction_if_repository_is_empty(self, repository):
        repository.start_transaction()
        assert repository.in_transaction is True


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


class TestTransaction:
    def test_if_transaction_is_started(self, repository):
        with repository.transaction():
            assert repository.in_transaction is True

    def test_if_transaction_is_committed(self, repository):
        with repository.transaction():
            pass
        assert repository.in_transaction is False

    def test_if_transaction_is_cancelled_if_error_is_raised(self, repository, selected_identifiers, entities):
        try:
            with repository.transaction():
                repository.delete(selected_identifiers)
                raise RuntimeError
        except RuntimeError:
            pass
        assert repository.entities == entities

    def test_if_error_raised_during_transaction_is_reraised_after(self, repository):
        with pytest.raises(RuntimeError):
            with repository.transaction():
                raise RuntimeError


def test_len(repository):
    assert len(repository) == 10


def test_repr(repository):
    assert repr(repository) == "Repository(address)"


def test_iter(identifiers, repository):
    assert [i for i in repository] == identifiers
