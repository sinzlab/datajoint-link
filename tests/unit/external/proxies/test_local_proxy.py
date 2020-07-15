from unittest.mock import call

import pytest

from link.external.proxies import SourceTableProxy, LocalTableProxy


def test_if_subclass_of_source_table_proxy():
    assert issubclass(LocalTableProxy, SourceTableProxy)


@pytest.fixture
def proxy_cls():
    return LocalTableProxy


class TestDeletionRequestedProperty:
    def test_if_table_is_instantiated(self, table_factory, proxy):
        _ = proxy.deletion_requested
        table_factory.assert_called_once_with()

    def test_if_primary_keys_of_deletion_requested_entities_are_fetched_correctly(self, table, proxy):
        _ = proxy.deletion_requested
        table.DeletionRequested.fetch.assert_called_once_with(as_dict=True)

    def test_if_primary_keys_of_deletion_requested_entities_are_returned(self, primary_keys, proxy):
        assert proxy.deletion_requested == primary_keys


class TestDelete:
    @pytest.fixture
    def delete(self, primary_keys, proxy):
        proxy.delete(primary_keys)

    @pytest.mark.usefixtures("delete")
    def test_if_table_is_instantiated_when_deleting_entities(self, table_factory):
        table_factory.assert_called_once_with()

    @pytest.mark.usefixtures("delete")
    def test_if_table_is_restricted_when_deleting_entities(self, primary_keys, table):
        table.__and__.assert_called_once_with(primary_keys)

    @pytest.mark.usefixtures("delete")
    def test_if_entities_are_correctly_deleted_from_restricted_table(self, primary_keys, table):
        table.__and__.return_value.delete.assert_called_once_with()


class TestInsert:
    def test_if_table_is_instantiated_when_inserting_entities(self, n_entities, entities, table_factory, proxy):
        proxy.insert(entities)
        assert table_factory.call_args_list == [call() for _ in range(n_entities)]

    def test_if_entities_are_correctly_inserted(self, entities, master_entities, table_factory, proxy):
        proxy.insert(entities)
        assert table_factory.return_value.insert1.call_args_list == [call(entity) for entity in master_entities]

    def test_if_part_entities_are_inserted_into_part_tables(self, entities, parts, part_entities, proxy):
        proxy.insert(entities)
        for part_name, part_entities in part_entities.items():
            assert parts[part_name].insert.call_args_list == [call(part_entity) for part_entity in part_entities]


class TestStartTransaction:
    def test_if_table_is_instantiated_when_starting_transaction(self, table_factory, proxy):
        proxy.start_transaction()
        table_factory.assert_called_once_with()

    def test_if_transaction_is_started_in_table(self, table, proxy):
        proxy.start_transaction()
        table.connection.start_transaction.assert_called_once_with()


class TestCommitTransaction:
    def test_if_table_is_instantiated_when_committing_transaction(self, table_factory, proxy):
        proxy.commit_transaction()
        table_factory.assert_called_once_with()

    def test_if_transaction_is_committed_in_table(self, table, proxy):
        proxy.commit_transaction()
        table.connection.commit_transaction.assert_called_once_with()


class TestCancelTransaction:
    def test_if_table_is_instantiated_when_cancelling_transaction(self, table_factory, proxy):
        proxy.cancel_transaction()
        table_factory.assert_called_once_with()

    def test_if_transaction_is_cancelled_in_table(self, table, proxy):
        proxy.cancel_transaction()
        table.connection.cancel_transaction.assert_called_once_with()
