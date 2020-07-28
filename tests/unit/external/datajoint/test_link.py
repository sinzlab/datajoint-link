from unittest.mock import MagicMock, call

import pytest
import datajoint as dj

from link.external.datajoint.dj_helpers import replace_stores
from link.external.datajoint.link import Link, pull
from link.external.datajoint.factory import SpawnTableConfig, CreateTableConfig


@pytest.fixture
def schema_cls_stub():
    class SchemaStub:
        def __init__(self, schema_name, connection):
            self.database = schema_name
            self.connection = connection

        def __repr__(self):
            return self.database

    return SchemaStub


@pytest.fixture
def local_schema_stub(schema_cls_stub):
    return schema_cls_stub("local_schema", "local_connection")


@pytest.fixture
def source_schema_stub(schema_cls_stub):
    return schema_cls_stub("source_schema", "source_connection")


@pytest.fixture
def stores():
    return dict(source_store="local_store")


@pytest.fixture
def schema_cls_spy():
    return MagicMock(name="schema_cls_spy")


@pytest.fixture
def replace_stores_spy():
    return MagicMock(name="replace_stores_spy", return_value="replaced_heading")


@pytest.fixture
def source_table_cls_stub():
    source_table_stub = MagicMock(name="source_table_cls_stub")
    source_table_stub.return_value.heading.__str__.return_value = "source_master_heading"
    return source_table_stub


@pytest.fixture
def source_part_stubs():
    source_part_stubs = dict()
    for name in ["A", "B", "C"]:
        source_part_stub = MagicMock(name="SourcePart" + name + "Stub")
        source_part_stub.return_value.heading.__str__.return_value = "source_part_" + name.lower() + "_heading"
        source_part_stubs["Part" + name] = source_part_stub
    return source_part_stubs


@pytest.fixture
def table_cls_factory_spies(source_table_cls_stub, source_part_stubs):
    table_cls_factory_spies = {
        kind: MagicMock(name=kind + "_dummy_table_cls_factory") for kind in ("source", "outbound", "local")
    }
    table_cls_factory_spies["source"].return_value = source_table_cls_stub
    table_cls_factory_spies["source"].part_tables = source_part_stubs
    table_cls_factory_spies["local"].return_value = "local_table_cls"
    return table_cls_factory_spies


@pytest.fixture
def dummy_local_table_controller():
    return MagicMock(name="local_table_controller")


@pytest.fixture
def link(
    local_schema_stub,
    source_schema_stub,
    stores,
    table_cls_factory_spies,
    schema_cls_spy,
    replace_stores_spy,
    dummy_local_table_controller,
):
    link = Link(local_schema_stub, source_schema_stub, stores=stores)
    link._table_cls_factories = table_cls_factory_spies
    link._schema_cls = schema_cls_spy
    link._replace_stores_func = replace_stores_spy
    link._local_table_controller = dummy_local_table_controller
    return link


@pytest.fixture
def table_name():
    return "table"


@pytest.fixture
def dummy_table_cls(table_name):
    return MagicMock(name="dummy_table_cls", __name__=table_name)


def test_if_table_factories_class_attribute_is_none():
    assert Link._table_cls_factories is None


def test_if_schema_class_class_attribute_is_datajoint_schema_class():
    assert Link._schema_cls is dj.schema


def test_if_replace_stores_func_class_attribute_is_replace_stores():
    assert Link._replace_stores_func is replace_stores


def test_if_local_table_controller_class_attribute_is_none():
    assert Link._local_table_controller is None


class TestInit:
    def test_if_local_schema_is_stored_as_instance_attribute(self, link, local_schema_stub):
        assert link.local_schema is local_schema_stub

    def test_if_source_schema_is_stored_as_instance_attribute(self, link, source_schema_stub):
        assert link.source_schema is source_schema_stub

    def test_if_stores_is_stored_as_instance_attribute(self, link, stores):
        assert link.stores is stores


@pytest.fixture
def linked_table(link, dummy_table_cls):
    return link(dummy_table_cls)


@pytest.mark.usefixtures("linked_table")
class TestCallWithoutInitialSetup:
    def test_if_spawn_table_config_attribute_on_source_table_cls_factory_is_set(
        self, table_cls_factory_spies, source_schema_stub, table_name
    ):
        assert table_cls_factory_spies["source"].spawn_table_config == SpawnTableConfig(source_schema_stub, table_name)

    def test_if_call_to_schema_class_is_correct(self, source_schema_stub, schema_cls_spy):
        schema_cls_spy.assert_called_once_with("datajoint_outbound__" + source_schema_stub.database)

    def test_if_spawn_table_config_attribute_on_outbound_table_cls_factory_is_set(
        self, table_cls_factory_spies, source_schema_stub, table_name, schema_cls_spy
    ):
        assert table_cls_factory_spies["outbound"].spawn_table_config == SpawnTableConfig(
            schema_cls_spy.return_value,
            table_name + "Outbound",
            flag_table_names=["DeletionRequested", "DeletionApproved"],
        )

    def test_if_spawn_table_config_attribute_on_local_table_cls_factory_is_set(
        self, table_cls_factory_spies, local_schema_stub, table_name, dummy_local_table_controller
    ):
        assert table_cls_factory_spies["local"].spawn_table_config == SpawnTableConfig(
            local_schema_stub,
            table_name,
            dict(controller=dummy_local_table_controller, pull=pull),
            ["DeletionRequested"],
        )

    def test_if_call_to_local_table_cls_factory_is_correct(self, table_cls_factory_spies):
        table_cls_factory_spies["local"].assert_called_once_with()

    def test_if_local_table_class_is_returned(self, linked_table):
        assert linked_table is "local_table_cls"


@pytest.fixture
def initial_setup_required(table_cls_factory_spies):
    table_cls_factory_spies["local"].side_effect = [RuntimeError, "local_table_cls"]


@pytest.mark.usefixtures("initial_setup_required", "linked_table")
class TestCallWithInitialSetup:
    def test_if_source_table_cls_factory_is_called(self, table_cls_factory_spies):
        table_cls_factory_spies["source"].assert_called_once_with()

    def test_if_source_table_is_added_to_table_class_attributes_of_spawn_table_config_in_outbound_table_cls_factory(
        self, table_cls_factory_spies, source_table_cls_stub
    ):
        assert (
            table_cls_factory_spies["outbound"].spawn_table_config.table_cls_attrs["source_table"]
            is source_table_cls_stub
        )

    def test_if_create_table_config_on_outbound_table_cls_factory_is_set(self, table_cls_factory_spies):
        assert table_cls_factory_spies["outbound"].create_table_config == CreateTableConfig("-> self.source_table")

    def test_if_outbound_table_cls_factory_is_called(self, table_cls_factory_spies):
        table_cls_factory_spies["outbound"].assert_called_once_with()

    def test_if_calls_to_replace_stores_func_are_correct(self, replace_stores_spy, stores):
        assert replace_stores_spy.call_args_list == [
            call("source_master_heading", stores),
            call("source_part_a_heading", stores),
            call("source_part_b_heading", stores),
            call("source_part_c_heading", stores),
        ]

    def test_if_create_table_config_on_local_table_cls_factory_is_set(self, table_cls_factory_spies):
        assert table_cls_factory_spies["local"].create_table_config == CreateTableConfig(
            "replaced_heading", dict(PartA="replaced_heading", PartB="replaced_heading", PartC="replaced_heading")
        )

    def test_if_calls_to_local_table_cls_factory_are_correct(self, table_cls_factory_spies):
        assert table_cls_factory_spies["local"].call_args_list == [call(), call()]

    def test_if_local_table_class_is_returned(self, linked_table):
        assert linked_table is "local_table_cls"


def test_repr(link, stores):
    assert repr(link) == f"Link(local_schema=local_schema, source_schema=source_schema, stores={stores})"


class TestPull:
    @pytest.fixture
    def fake_local_table(self):
        class FakeLocalTable:
            controller = MagicMock(name="controller_spy")

        setattr(FakeLocalTable, "pull", pull)
        return FakeLocalTable()

    def test_if_call_to_controller_is_correct(self, fake_local_table):
        fake_local_table.pull("restriction1", "restriction2")
        fake_local_table.controller.pull.assert_called_once_with(("restriction1", "restriction2"))
