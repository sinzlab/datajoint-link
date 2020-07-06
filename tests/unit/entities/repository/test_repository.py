import pytest

from link.entities import repository


def test_if_subclass_of_read_only_repository():
    assert issubclass(repository.Repository, repository.ReadOnlyRepository)


@pytest.fixture
def repo_cls(gateway, entity_creator):
    class Repository(repository.Repository):
        __qualname__ = "Repository"

    return Repository


class TestInit:
    def test_if_in_transaction_is_false_by_default(self, repo):
        assert repo.in_transaction is False


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
    def test_if_data_is_retrieved_from_storage(self, repo, new_identifiers, new_entities, storage):
        repo.insert(new_entities)
        storage.retrieve.assert_called_once_with(new_identifiers)

    def test_if_data_is_inserted_in_gateway(self, repo, gateway, new_entities, new_data):
        repo.insert(new_entities)
        gateway.insert.assert_called_once_with(new_data)

    def test_if_entities_are_not_inserted_after_insertion_failed_in_gateway(
        self, test_if_entities_are_not_processed_after_processing_failed_in_gateway, new_entities
    ):
        test_if_entities_are_not_processed_after_processing_failed_in_gateway("insert", new_entities, RuntimeError)

    def test_if_entities_are_inserted(self, entities, repo, new_entities):
        repo.insert(new_entities)
        assert repo.entities == entities + new_entities


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

    @pytest.fixture
    def deletion_requested_indexes(self):
        return list()

    @pytest.fixture
    def deletion_approved_indexes(self):
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
