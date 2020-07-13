from unittest.mock import MagicMock

import pytest

from link.external import proxies


@pytest.fixture
def primary_attr_names():
    return ["pa0", "pa1", "pa2"]


@pytest.fixture
def primary_keys():
    return ["pk0", "pk1", "pk2"]


@pytest.fixture
def entities():
    return ["entity0", "entity1", "entity2"]


@pytest.fixture
def table(primary_attr_names, primary_keys, entities):
    name = "table"
    table = MagicMock(name=name)
    table.heading.primary_key = primary_attr_names
    table.return_value.proj.return_value.fetch.return_value = primary_keys
    table.return_value.proj.return_value.__and__.return_value.fetch.return_value = primary_keys
    table.return_value.__and__.return_value.fetch.return_value = entities
    table.return_value.DeletionRequested.fetch.return_value = primary_keys
    table.return_value.DeletionApproved.fetch.return_value = primary_keys
    table.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return table


@pytest.fixture
def proxy(proxy_cls, table):
    return proxy_cls(table)


class TestSourceTableProxy:
    @pytest.fixture
    def proxy_cls(self):
        return proxies.SourceTableProxy

    @pytest.fixture
    def fetch(self, primary_keys, proxy):
        proxy.fetch(primary_keys)

    @pytest.fixture
    def restriction(self):
        return "restriction"

    def test_if_table_is_stored_as_instance_attribute(self, table, proxy):
        assert proxy.table is table

    def test_if_table_is_instantiated_when_accessing_primary_keys_property(self, table, proxy):
        _ = proxy.primary_keys
        table.assert_called_once_with()

    def test_if_table_is_projected_to_primary_keys_when_accessing_primary_keys_property(self, table, proxy):
        _ = proxy.primary_keys
        table.return_value.proj.assert_called_once_with()

    def test_if_fetch_is_called_correctly_when_accessing_primary_keys_property(self, table, proxy):
        _ = proxy.primary_keys
        table.return_value.proj.return_value.fetch.assert_called_once_with(as_dict=True)

    def test_if_primary_keys_property_returns_correct_value(self, primary_keys, proxy):
        assert proxy.primary_keys == primary_keys

    def test_if_table_is_instantiated_when_getting_primary_keys_in_restriction(self, table, proxy, restriction):
        proxy.get_primary_keys_in_restriction(restriction)
        table.assert_called_once_with()

    def test_if_table_is_projected_to_primary_keys_when_getting_primary_keys_in_restriction(
        self, table, proxy, restriction
    ):
        proxy.get_primary_keys_in_restriction(restriction)
        table.return_value.proj.assert_called_once_with()

    def test_if_projected_table_is_restricted_when_getting_primary_keys_in_restriction(self, table, proxy, restriction):
        proxy.get_primary_keys_in_restriction(restriction)
        table.return_value.proj.return_value.__and__.assert_called_once_with(restriction)

    def test_if_fetch_on_restricted_table_is_called_correctly_when_getting_primary_keys_in_restriction(
        self, table, proxy, restriction
    ):
        proxy.get_primary_keys_in_restriction(restriction)
        table.return_value.proj.return_value.__and__.return_value.fetch.assert_called_once_with(as_dict=True)

    def test_if_correct_primary_keys_are_returned_when_getting_primary_keys_in_restriction(
        self, primary_keys, table, proxy, restriction
    ):
        assert proxy.get_primary_keys_in_restriction(restriction) == primary_keys

    @pytest.mark.usefixtures("fetch")
    def test_if_table_is_instantiated_when_fetching_entities(self, primary_keys, table, proxy):
        table.assert_called_once_with()

    @pytest.mark.usefixtures("fetch")
    def test_if_table_is_restricted_when_fetching_entities(self, primary_keys, table, proxy):
        table.return_value.__and__.assert_called_once_with(primary_keys)

    @pytest.mark.usefixtures("fetch")
    def test_if_entities_are_correctly_fetched_from_restricted_table(self, primary_keys, table, proxy):
        table.return_value.__and__.return_value.fetch.assert_called_once_with(as_dict=True)

    def test_if_entities_are_returned_when_fetched(self, primary_keys, entities, proxy):
        assert proxy.fetch(primary_keys) == entities

    def test_repr(self, proxy):
        assert repr(proxy) == "SourceTableProxy(table)"


class TestNonSourceTableProxy:
    def test_if_subclass_of_source_table_proxy(self):
        assert issubclass(proxies.NonSourceTableProxy, proxies.SourceTableProxy)

    @pytest.fixture
    def proxy_cls(self):
        return proxies.NonSourceTableProxy

    @pytest.fixture
    def delete(self, primary_keys, proxy):
        proxy.delete(primary_keys)

    def test_if_table_is_instantiated_when_fetching_deletion_requested_entities(self, table, proxy):
        _ = proxy.deletion_requested
        table.assert_called_once_with()

    def test_if_primary_keys_of_deletion_requested_entities_are_fetched_correctly(self, table, proxy):
        _ = proxy.deletion_requested
        table.return_value.DeletionRequested.fetch.assert_called_once_with(as_dict=True)

    def test_if_primary_keys_of_deletion_requested_entities_are_returned(self, primary_keys, proxy):
        assert proxy.deletion_requested == primary_keys

    @pytest.mark.usefixtures("delete")
    def test_if_table_is_instantiated_when_deleting_entities(self, table):
        table.assert_called_once_with()

    @pytest.mark.usefixtures("delete")
    def test_if_table_is_restricted_when_deleting_entities(self, primary_keys, table):
        table.return_value.__and__.assert_called_once_with(primary_keys)

    @pytest.mark.usefixtures("delete")
    def test_if_entities_are_correctly_deleted_from_restricted_table(self, primary_keys, table):
        table.return_value.__and__.return_value.delete.assert_called_once_with()

    def test_if_table_is_instantiated_when_inserting_entities(self, entities, table, proxy):
        proxy.insert(entities)
        table.assert_called_once_with()

    def test_if_entities_are_correctly_inserted(self, entities, table, proxy):
        proxy.insert(entities)
        table.return_value.insert.assert_called_once_with(entities)

    def test_if_table_is_instantiated_when_starting_transaction(self, table, proxy):
        proxy.start_transaction()
        table.assert_called_once_with()

    def test_if_transaction_is_started_in_table(self, table, proxy):
        proxy.start_transaction()
        table.return_value.connection.start_transaction.assert_called_once_with()

    def test_if_table_is_instantiated_when_committing_transaction(self, table, proxy):
        proxy.commit_transaction()
        table.assert_called_once_with()

    def test_if_transaction_is_committed_in_table(self, table, proxy):
        proxy.commit_transaction()
        table.return_value.connection.commit_transaction.assert_called_once_with()

    def test_if_table_is_instantiated_when_cancelling_transaction(self, table, proxy):
        proxy.cancel_transaction()
        table.assert_called_once_with()

    def test_if_transaction_is_cancelled_in_table(self, table, proxy):
        proxy.cancel_transaction()
        table.return_value.connection.cancel_transaction.assert_called_once_with()


class TestOutboundTableProxy:
    def test_if_subclass_of_non_source_table_proxy(self):
        assert issubclass(proxies.OutboundTableProxy, proxies.NonSourceTableProxy)

    @pytest.fixture
    def proxy_cls(self):
        return proxies.OutboundTableProxy

    def test_if_table_is_instantiated_when_fetching_deletion_approved_entities(self, table, proxy):
        _ = proxy.deletion_approved
        table.assert_called_once_with()

    def test_if_primary_keys_of_deletion_approved_entities_are_fetched_correctly(self, table, proxy):
        _ = proxy.deletion_approved
        table.return_value.DeletionApproved.fetch.assert_called_once_with(as_dict=True)

    def test_if_primary_keys_of_deletion_approved_entities_are_returned(self, primary_keys, proxy):
        assert proxy.deletion_approved == primary_keys

    def test_if_table_is_instantiated_when_approving_deletion(self, primary_keys, table, proxy):
        proxy.approve_deletion(primary_keys)
        table.assert_called_once_with()

    def test_if_primary_keys_are_inserted_into_deletion_approved_part_table(self, primary_keys, table, proxy):
        proxy.approve_deletion(primary_keys)
        table.return_value.DeletionApproved.insert.assert_called_once_with(primary_keys)


def test_if_local_table_proxy_is_subclass_of_non_source_table_proxy():
    assert issubclass(proxies.LocalTableProxy, proxies.NonSourceTableProxy)
