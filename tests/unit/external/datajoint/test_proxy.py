from unittest.mock import MagicMock

import pytest
from datajoint import Part

from link.external.datajoint.proxy import TableProxy


@pytest.fixture
def flag_table_names():
    return ["MyFlag", "MyOtherFlag"]


@pytest.fixture
def is_present_in_flag_table(flag_table_names):
    return {flag_table_name: is_present for flag_table_name, is_present in zip(flag_table_names, [False, True])}


@pytest.fixture
def table_spy(flag_table_names, is_present_in_flag_table):
    table_spy = MagicMock(name="table_spy", flag_table_names=flag_table_names)
    table_spy.proj.return_value.fetch.return_value = "primary_keys"
    table_spy.__and__.return_value.fetch1.return_value = "master_entity"
    for flag_table_name, is_present in is_present_in_flag_table.items():
        getattr(table_spy, flag_table_name).__and__.return_value.__contains__.return_value = is_present
    return table_spy


@pytest.fixture
def part_table_names():
    return ["MyPart", "MyOtherPart"]


@pytest.fixture
def part_table_entities(part_table_names):
    return {name: name + "_entities" for name in part_table_names}


@pytest.fixture
def part_table_spies(part_table_entities):
    part_table_spies = dict()
    for name, entities in part_table_entities.items():
        spy = MagicMock(name=name + "Spy", spec=Part)
        spy.__and__.return_value.fetch.return_value = entities
        part_table_spies[name] = spy
    return part_table_spies


@pytest.fixture
def table_factory_spy(table_spy, part_table_spies):
    name = "table_factory_spy"
    table_factory_spy = MagicMock(name=name, return_value=table_spy, parts=part_table_spies)
    table_factory_spy.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return table_factory_spy


@pytest.fixture
def download_path():
    return "download_path"


@pytest.fixture
def table_proxy(table_factory_spy, download_path):
    return TableProxy(table_factory_spy, download_path)


@pytest.fixture
def primary_key():
    return "primary_key"


def test_if_table_factory_is_stored_as_instance_attribute(table_proxy, table_factory_spy):
    assert table_proxy.table_factory is table_factory_spy


def test_if_download_path_is_stored_as_instance_attribute(table_proxy, download_path):
    assert table_proxy.download_path == download_path


class TestPrimaryKeysProperty:
    @pytest.fixture(autouse=True)
    def primary_keys(self, table_proxy):
        return table_proxy.primary_keys

    def test_if_call_to_table_factory_is_correct(self, table_factory_spy):
        table_factory_spy.assert_called_once_with()

    def test_if_table_is_projected_to_primary_keys(self, table_spy):
        table_spy.proj.assert_called_once_with()

    def test_if_primary_keys_are_fetched_from_projected_table(self, table_spy):
        table_spy.proj.return_value.fetch.assert_called_once_with(as_dict=True)

    def test_if_primary_keys_are_returned(self, primary_keys):
        assert primary_keys == "primary_keys"


class TestGetFlags:
    @pytest.fixture(autouse=True)
    def flags(self, table_proxy, primary_key):
        return table_proxy.get_flags(primary_key)

    def test_if_call_to_table_factory_is_correct(self, table_factory_spy):
        table_factory_spy.assert_called_once_with()

    def test_if_flag_tables_are_restricted(self, table_spy, primary_key, flag_table_names):
        for name in flag_table_names:
            getattr(table_spy, name).__and__.assert_called_once_with(primary_key)

    def test_if_presence_of_primary_key_in_restricted_flag_tables_is_checked(
        self, table_spy, primary_key, flag_table_names
    ):
        for name in flag_table_names:
            getattr(table_spy, name).__and__.return_value.__contains__.assert_called_once_with(primary_key)

    def test_if_returned_flags_are_correct(self, is_present_in_flag_table, flags):
        assert flags == is_present_in_flag_table


class TestFetchMaster:
    @pytest.fixture(autouse=True)
    def master_entity(self, table_proxy, primary_key):
        return table_proxy.fetch_master(primary_key)

    def test_if_call_to_table_factory_is_correct(self, table_factory_spy):
        table_factory_spy.assert_called_once_with()

    def test_if_table_is_restricted(self, table_spy, primary_key):
        table_spy.__and__.assert_called_once_with(primary_key)

    def test_if_master_entity_is_fetched(self, table_spy, download_path):
        table_spy.__and__.return_value.fetch1.assert_called_once_with(download_path=download_path)

    def test_if_master_entity_is_returned(self, master_entity):
        assert master_entity == "master_entity"


class TestFetchParts:
    @pytest.fixture(autouse=True)
    def returned_part_table_entities(self, table_proxy, primary_key):
        return table_proxy.fetch_parts(primary_key)

    def test_if_part_tables_are_restricted(self, part_table_spies, primary_key):
        for part in part_table_spies.values():
            part.__and__.assert_called_once_with(primary_key)

    def test_if_part_entities_are_fetched_from_part_tables(self, part_table_spies, download_path):
        for part in part_table_spies.values():
            part.__and__.return_value.fetch.assert_called_once_with(as_dict=True, download_path=download_path)

    def test_if_part_entities_are_returned(self, returned_part_table_entities, part_table_entities):
        assert returned_part_table_entities == part_table_entities


class TestInsertMaster:
    @pytest.fixture
    def master_entity(self):
        return "master_entity"

    @pytest.fixture(autouse=True)
    def insert_master(self, table_proxy, master_entity):
        table_proxy.insert_master(master_entity)

    def test_if_call_to_table_factory_is_correct(self, table_factory_spy):
        table_factory_spy.assert_called_once_with()

    def test_if_master_entity_is_inserted(self, table_spy, master_entity):
        table_spy.insert1.assert_called_once_with(master_entity)


class TestInsertParts:
    @pytest.fixture(autouse=True)
    def insert_parts(self, table_proxy, part_table_entities):
        table_proxy.insert_parts(part_table_entities)

    def test_if_part_entities_are_inserted(self, part_table_spies, part_table_entities):
        for name, part in part_table_spies.items():
            part.insert.assert_called_once_with(part_table_entities[name])


class TestDeleteMaster:
    @pytest.fixture(autouse=True)
    def delete_master(self, table_proxy, primary_key):
        table_proxy.delete_master(primary_key)

    def test_if_call_to_table_factory_is_correct(self, table_factory_spy):
        table_factory_spy.assert_called_once_with()

    def test_if_table_is_restricted(self, table_spy, primary_key):
        table_spy.__and__.assert_called_once_with(primary_key)

    def test_if_master_table_entity_is_deleted(self, table_spy):
        table_spy.__and__.return_value.delete_quick.assert_called_once_with()


class TestDeleteParts:
    @pytest.fixture(autouse=True)
    def delete_parts(self, table_proxy, primary_key):
        table_proxy.delete_parts(primary_key)

    def test_if_part_tables_are_restricted(self, part_table_spies, primary_key):
        for part in part_table_spies.values():
            part.__and__.assert_called_once_with(primary_key)

    def test_if_part_table_entities_are_deleted(self, part_table_spies):
        for part in part_table_spies.values():
            part.__and__.return_value.delete_quick.assert_called_once_with()


@pytest.fixture
def part_table_name(part_table_names):
    return part_table_names[0]


@pytest.fixture
def part_table_spy(part_table_spies, part_table_name):
    return part_table_spies[part_table_name]


def test_if_flag_is_enabled(table_proxy, part_table_spy, primary_key, part_table_name):
    table_proxy.enable_flag(primary_key, part_table_name)
    part_table_spy.insert1.assert_called_once_with(primary_key)


class TestDisableFlag:
    @pytest.fixture(autouse=True)
    def enable_flag(self, table_proxy, primary_key, part_table_name):
        table_proxy.disable_flag(primary_key, part_table_name)

    def test_if_part_table_is_restricted(self, part_table_spy, primary_key):
        part_table_spy.__and__.assert_called_once_with(primary_key)

    def test_if_flag_is_deleted(self, part_table_spy):
        part_table_spy.__and__.return_value.delete_quick.assert_called_once_with()


class TestStartTransaction:
    @pytest.fixture(autouse=True)
    def start_transaction(self, table_proxy):
        table_proxy.start_transaction()

    def test_if_call_to_table_factory_is_correct(self, table_factory_spy):
        table_factory_spy.assert_called_once_with()

    def test_if_transaction_is_started(self, table_spy):
        table_spy.connection.start_transaction.assert_called_once_with()


class TestCommitTransaction:
    @pytest.fixture(autouse=True)
    def commit_transaction(self, table_proxy):
        table_proxy.commit_transaction()

    def test_if_call_to_table_factory_is_correct(self, table_factory_spy):
        table_factory_spy.assert_called_once_with()

    def test_if_transaction_is_committed(self, table_spy):
        table_spy.connection.commit_transaction.assert_called_once_with()


class TestCancelTransaction:
    @pytest.fixture(autouse=True)
    def cancel_transaction(self, table_proxy):
        table_proxy.cancel_transaction()

    def test_if_call_to_table_factory_is_correct(self, table_factory_spy):
        table_factory_spy.assert_called_once_with()

    def test_if_transaction_is_cancelled(self, table_spy):
        table_spy.connection.cancel_transaction.assert_called_once_with()


def test_repr(table_proxy):
    assert repr(table_proxy) == "TableProxy(table_factory=table_factory_spy, download_path='download_path')"
