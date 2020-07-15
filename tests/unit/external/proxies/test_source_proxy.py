from unittest.mock import MagicMock, call

import pytest

from link.external.proxies import SourceTableProxy
from link.external.entity import EntityPacketCreator


@pytest.fixture
def proxy_cls():
    return SourceTableProxy


def test_if_entity_packet_creator_class_attribute_is_none_by_default():
    assert SourceTableProxy.entity_packet_creator is None


def test_if_table_factory_is_stored_as_instance_attribute(table_factory, proxy):
    assert proxy.table_factory is table_factory


def test_if_download_path_is_stored_as_instance_attribute(download_path, proxy):
    assert proxy.download_path == download_path


class TestPrimaryKeysProperty:
    def test_if_table_is_instantiated(self, table_factory, proxy):
        _ = proxy.primary_keys
        table_factory.assert_called_once_with()

    def test_if_table_is_projected_to_primary_keys(self, table, proxy):
        _ = proxy.primary_keys
        table.proj.assert_called_once_with()

    def test_if_fetch_is_called_correctly(self, table, proxy):
        _ = proxy.primary_keys
        table.proj.return_value.fetch.assert_called_once_with(as_dict=True)

    def test_if_primary_keys_property_returns_correct_value(self, primary_keys, proxy):
        assert proxy.primary_keys == primary_keys


class TestGetPrimaryKeysInRestriction:
    @pytest.fixture
    def restriction(self):
        return "restriction"

    def test_if_table_is_instantiated(self, table_factory, proxy, restriction):
        proxy.get_primary_keys_in_restriction(restriction)
        table_factory.assert_called_once_with()

    def test_if_table_is_projected_to_primary_keys(self, table, proxy, restriction):
        proxy.get_primary_keys_in_restriction(restriction)
        table.proj.assert_called_once_with()

    def test_if_projected_table_is_restricted_when(self, table, proxy, restriction):
        proxy.get_primary_keys_in_restriction(restriction)
        table.proj.return_value.__and__.assert_called_once_with(restriction)

    def test_if_fetch_on_restricted_table_is_called_correctly(self, table, proxy, restriction):
        proxy.get_primary_keys_in_restriction(restriction)
        table.proj.return_value.__and__.return_value.fetch.assert_called_once_with(as_dict=True)

    def test_if_correct_primary_keys_are_returned_when_getting_primary_keys_in_restriction(
        self, primary_keys, table, proxy, restriction
    ):
        assert proxy.get_primary_keys_in_restriction(restriction) == primary_keys


class TestFetch:
    @pytest.fixture
    def entity_packet_creator(self):
        return MagicMock(name="entity_packet_creator", spec=EntityPacketCreator)

    @pytest.fixture
    def proxy_cls(self, entity_packet_creator):
        SourceTableProxy.entity_packet_creator = entity_packet_creator
        return SourceTableProxy

    @pytest.fixture
    def fetch(self, primary_keys, proxy):
        proxy.fetch(primary_keys)

    @pytest.mark.usefixtures("fetch")
    def test_if_table_is_instantiated(self, n_entities, table_factory):
        assert table_factory.call_args_list == [call() for _ in range(n_entities + 1)]

    @pytest.mark.usefixtures("fetch")
    def test_if_table_is_restricted_when_fetching_entities(self, primary_keys, table):
        assert table.__and__.call_args_list == [call(key) for key in primary_keys]

    @pytest.mark.usefixtures("fetch")
    def test_if_entities_are_correctly_fetched_from_restricted_table(self, n_entities, table, download_path):
        assert table.__and__.return_value.fetch1.call_args_list == [
            call(download_path=download_path) for _ in range(n_entities)
        ]

    @pytest.mark.usefixtures("fetch")
    def test_if_part_tables_are_restricted_when_fetching_entities(self, primary_keys, parts):
        for part in parts.values():
            assert part.__and__.call_args_list == [call(key) for key in primary_keys]

    @pytest.mark.usefixtures("fetch")
    def test_if_part_entities_are_correctly_fetched_from_restricted_part_tables(self, n_entities, parts, download_path):
        for part in parts.values():
            assert part.__and__.return_value.fetch.call_args_list == [
                call(download_path=download_path, as_dict=True) for _ in range(n_entities)
            ]

    @pytest.mark.usefixtures("fetch")
    def test_if_entity_packet_creator_is_called_correctly(
        self, n_entities, primary_attr_names, entity_packet_creator, master_entities, part_entities
    ):
        entity_packet_creator.create.assert_called_once_with(
            primary_attrs=primary_attr_names,
            master_entities=master_entities,
            part_entities=[{name: entities[i] for name, entities in part_entities.items()} for i in range(n_entities)],
        )

    def test_if_entity_packet_is_returned(self, primary_keys, proxy, entity_packet_creator):
        assert proxy.fetch(primary_keys) is entity_packet_creator.create()


def test_repr(proxy):
    assert repr(proxy) == "SourceTableProxy(table_factory)"
