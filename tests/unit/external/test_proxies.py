from unittest.mock import MagicMock
from string import ascii_uppercase

import pytest

from link.external import proxies


@pytest.fixture
def primary_attr_names():
    return ["pa0", "pa1", "pa2"]


@pytest.fixture
def n_entities():
    return 3


@pytest.fixture
def primary_keys(n_entities):
    return ["pk" + str(i) for i in range(n_entities)]


@pytest.fixture
def main_entities(n_entities):
    return ["Main_entity" + str(i) for i in range(n_entities)]


@pytest.fixture
def part_names(n_entities):
    return ["Part" + ascii_uppercase[i] for i in range(n_entities)]


@pytest.fixture
def part_entities(n_entities, part_names):
    return [[name + "_entity" + str(i) for i in range(n_entities)] for name in part_names]


@pytest.fixture
def entities(main_entities, part_names, part_entities):
    return dict(main=main_entities, parts={name: entities for name, entities in zip(part_names, part_entities)},)


@pytest.fixture
def parts(part_names, part_entities):
    parts = dict()
    for name, entities in zip(part_names, part_entities):
        part = MagicMock(name=name)
        part.__and__.return_value.fetch.return_value = entities
        parts[name] = part
    return parts


@pytest.fixture
def table(primary_attr_names, primary_keys, main_entities):
    table = MagicMock(name="table")
    table.heading.primary_key = primary_attr_names
    table.proj.return_value.fetch.return_value = primary_keys
    table.proj.return_value.__and__.return_value.fetch.return_value = primary_keys
    table.__and__.return_value.fetch.return_value = main_entities
    table.DeletionRequested.fetch.return_value = primary_keys
    table.DeletionApproved.fetch.return_value = primary_keys
    return table


@pytest.fixture
def table_factory(primary_attr_names, parts, table):
    name = "table_factory"
    table_factory = MagicMock(name=name, return_value=table)
    table_factory.parts = parts
    table_factory.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return table_factory


@pytest.fixture
def proxy(proxy_cls, table_factory):
    return proxy_cls(table_factory)


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

    def test_if_table_factory_is_stored_as_instance_attribute(self, table_factory, proxy):
        assert proxy.table_factory is table_factory

    def test_if_table_is_instantiated_when_accessing_primary_attr_names(self, table_factory, proxy):
        _ = proxy.primary_attr_names
        table_factory.assert_called_once_with()

    def test_if_primary_attr_names_are_returned(self, primary_attr_names, proxy):
        assert proxy.primary_attr_names == primary_attr_names

    def test_if_table_is_instantiated_when_accessing_primary_keys_property(self, table_factory, proxy):
        _ = proxy.primary_keys
        table_factory.assert_called_once_with()

    def test_if_table_is_projected_to_primary_keys_when_accessing_primary_keys_property(self, table, proxy):
        _ = proxy.primary_keys
        table.proj.assert_called_once_with()

    def test_if_fetch_is_called_correctly_when_accessing_primary_keys_property(self, table, proxy):
        _ = proxy.primary_keys
        table.proj.return_value.fetch.assert_called_once_with(as_dict=True)

    def test_if_primary_keys_property_returns_correct_value(self, primary_keys, proxy):
        assert proxy.primary_keys == primary_keys

    def test_if_table_is_instantiated_when_getting_primary_keys_in_restriction(self, table_factory, proxy, restriction):
        proxy.get_primary_keys_in_restriction(restriction)
        table_factory.assert_called_once_with()

    def test_if_table_is_projected_to_primary_keys_when_getting_primary_keys_in_restriction(
        self, table, proxy, restriction
    ):
        proxy.get_primary_keys_in_restriction(restriction)
        table.proj.assert_called_once_with()

    def test_if_projected_table_is_restricted_when_getting_primary_keys_in_restriction(self, table, proxy, restriction):
        proxy.get_primary_keys_in_restriction(restriction)
        table.proj.return_value.__and__.assert_called_once_with(restriction)

    def test_if_fetch_on_restricted_table_is_called_correctly_when_getting_primary_keys_in_restriction(
        self, table, proxy, restriction
    ):
        proxy.get_primary_keys_in_restriction(restriction)
        table.proj.return_value.__and__.return_value.fetch.assert_called_once_with(as_dict=True)

    def test_if_correct_primary_keys_are_returned_when_getting_primary_keys_in_restriction(
        self, primary_keys, table, proxy, restriction
    ):
        assert proxy.get_primary_keys_in_restriction(restriction) == primary_keys

    @pytest.mark.usefixtures("fetch")
    def test_if_table_is_instantiated_when_fetching_entities(self, table_factory):
        table_factory.assert_called_once_with()

    @pytest.mark.usefixtures("fetch")
    def test_if_table_is_restricted_when_fetching_entities(self, primary_keys, table):
        table.__and__.assert_called_once_with(primary_keys)

    @pytest.mark.usefixtures("fetch")
    def test_if_entities_are_correctly_fetched_from_restricted_table(self, table):
        table.__and__.return_value.fetch.assert_called_once_with(as_dict=True)

    @pytest.mark.usefixtures("fetch")
    def test_if_part_tables_are_restricted_when_fetching_entities(self, primary_keys, parts):
        for part in parts.values():
            part.__and__.assert_called_once_with(primary_keys)

    @pytest.mark.usefixtures("fetch")
    def test_if_part_entities_are_correctly_fetched_from_restricted_part_tables(self, parts):
        for part in parts.values():
            part.__and__.return_value.fetch.assert_called_once_with(as_dict=True)

    def test_if_entities_are_returned_when_fetched(self, primary_keys, entities, proxy):
        assert proxy.fetch(primary_keys) == entities

    def test_repr(self, proxy):
        assert repr(proxy) == "SourceTableProxy(table_factory)"


class TestLocalTableProxy:
    def test_if_subclass_of_source_table_proxy(self):
        assert issubclass(proxies.LocalTableProxy, proxies.SourceTableProxy)

    @pytest.fixture
    def proxy_cls(self):
        return proxies.LocalTableProxy

    @pytest.fixture
    def delete(self, primary_keys, proxy):
        proxy.delete(primary_keys)

    def test_if_table_is_instantiated_when_fetching_deletion_requested_entities(self, table_factory, proxy):
        _ = proxy.deletion_requested
        table_factory.assert_called_once_with()

    def test_if_primary_keys_of_deletion_requested_entities_are_fetched_correctly(self, table, proxy):
        _ = proxy.deletion_requested
        table.DeletionRequested.fetch.assert_called_once_with(as_dict=True)

    def test_if_primary_keys_of_deletion_requested_entities_are_returned(self, primary_keys, proxy):
        assert proxy.deletion_requested == primary_keys

    @pytest.mark.usefixtures("delete")
    def test_if_table_is_instantiated_when_deleting_entities(self, table_factory):
        table_factory.assert_called_once_with()

    @pytest.mark.usefixtures("delete")
    def test_if_table_is_restricted_when_deleting_entities(self, primary_keys, table):
        table.__and__.assert_called_once_with(primary_keys)

    @pytest.mark.usefixtures("delete")
    def test_if_entities_are_correctly_deleted_from_restricted_table(self, primary_keys, table):
        table.__and__.return_value.delete.assert_called_once_with()

    def test_if_table_is_instantiated_when_inserting_entities(self, entities, table_factory, proxy):
        proxy.insert(entities)
        table_factory.assert_called_once_with()

    def test_if_entities_are_correctly_inserted(self, entities, main_entities, table_factory, proxy):
        proxy.insert(entities)
        table_factory.return_value.insert.assert_called_once_with(main_entities)

    def test_if_part_entities_are_inserted_into_part_tables(self, entities, parts, part_entities, proxy):
        proxy.insert(entities)
        for part, part_entities in zip(parts.values(), part_entities):
            part.insert.assert_called_once_with(part_entities)

    def test_if_table_is_instantiated_when_starting_transaction(self, table_factory, proxy):
        proxy.start_transaction()
        table_factory.assert_called_once_with()

    def test_if_transaction_is_started_in_table(self, table, proxy):
        proxy.start_transaction()
        table.connection.start_transaction.assert_called_once_with()

    def test_if_table_is_instantiated_when_committing_transaction(self, table_factory, proxy):
        proxy.commit_transaction()
        table_factory.assert_called_once_with()

    def test_if_transaction_is_committed_in_table(self, table, proxy):
        proxy.commit_transaction()
        table.connection.commit_transaction.assert_called_once_with()

    def test_if_table_is_instantiated_when_cancelling_transaction(self, table_factory, proxy):
        proxy.cancel_transaction()
        table_factory.assert_called_once_with()

    def test_if_transaction_is_cancelled_in_table(self, table, proxy):
        proxy.cancel_transaction()
        table.connection.cancel_transaction.assert_called_once_with()


class TestOutboundTableProxy:
    def test_if_subclass_of_non_source_table_proxy(self):
        assert issubclass(proxies.OutboundTableProxy, proxies.LocalTableProxy)

    @pytest.fixture
    def proxy_cls(self):
        return proxies.OutboundTableProxy

    def test_if_table_is_instantiated_when_fetching_deletion_approved_entities(self, table_factory, proxy):
        _ = proxy.deletion_approved
        table_factory.assert_called_once_with()

    def test_if_primary_keys_of_deletion_approved_entities_are_fetched_correctly(self, table, proxy):
        _ = proxy.deletion_approved
        table.DeletionApproved.fetch.assert_called_once_with(as_dict=True)

    def test_if_primary_keys_of_deletion_approved_entities_are_returned(self, primary_keys, proxy):
        assert proxy.deletion_approved == primary_keys

    def test_if_table_is_instantiated_when_approving_deletion(self, primary_keys, table_factory, proxy):
        proxy.approve_deletion(primary_keys)
        table_factory.assert_called_once_with()

    def test_if_primary_keys_are_inserted_into_deletion_approved_part_table(self, primary_keys, table, proxy):
        proxy.approve_deletion(primary_keys)
        table.DeletionApproved.insert.assert_called_once_with(primary_keys)
