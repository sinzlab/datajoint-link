import os
from unittest.mock import MagicMock, call

import pytest
from datajoint import Lookup, Schema

from link.base import Base
from link.frameworks.datajoint.dj_helpers import replace_stores
from link.frameworks.datajoint.link import Link, LocalTableMixin
from link.frameworks.datajoint.factory import TableFactoryConfig, TableFactory


@pytest.fixture
def schema_cls_stub():
    class SchemaStub:
        def __init__(self, schema_name, connection):
            self.database = schema_name
            self.connection = connection

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
    source_part_stubs = {}
    for name in ["A", "B", "C"]:
        source_part_stub = MagicMock(name="SourcePart" + name + "Stub")
        source_part_stub.return_value.heading.__str__.return_value = "source_part_" + name.lower() + "_heading"
        source_part_stubs["Part" + name] = source_part_stub
    return source_part_stubs


@pytest.fixture
def table_cls_factory_spies(source_table_cls_stub, source_part_stubs):
    table_cls_factory_spies = {
        kind: MagicMock(name=kind + "_dummy_table_cls_factory", spec=TableFactory)
        for kind in ("source", "outbound", "local")
    }
    table_cls_factory_spies["source"].return_value = source_table_cls_stub
    table_cls_factory_spies["source"].part_tables = source_part_stubs
    table_cls_factory_spies["outbound"].table_cls_attrs = {}
    table_cls_factory_spies["local"].return_value = "local_table_cls"
    return table_cls_factory_spies


@pytest.fixture
def dummy_base_table_cls():
    return MagicMock(name="dummy_base_table_cls")


@pytest.fixture
def link(
    local_schema_stub,
    source_schema_stub,
    stores,
    table_cls_factory_spies,
    schema_cls_spy,
    replace_stores_spy,
    dummy_base_table_cls,
):
    link = Link(local_schema_stub, source_schema_stub, stores=stores)
    link._table_cls_factories = table_cls_factory_spies
    link._schema_cls = schema_cls_spy
    link._replace_stores_func = replace_stores_spy
    link._base_table_cls = dummy_base_table_cls
    return link


@pytest.fixture
def table_name():
    return "table"


@pytest.fixture
def dummy_cls(table_name):
    return MagicMock(name="dummy_cls", __name__=table_name)


def test_if_link_is_subclass_of_base():
    assert issubclass(Link, Base)


def test_if_schema_class_class_attribute_is_datajoint_schema_class():
    assert Link._schema_cls is Schema


def test_if_replace_stores_func_class_attribute_is_replace_stores():
    assert Link._replace_stores_func is replace_stores


def test_if_base_table_class_is_lookup_table():
    assert Link._base_table_cls is Lookup


class TestInit:
    def test_if_local_schema_is_stored_as_instance_attribute(self, link, local_schema_stub):
        assert link.local_schema is local_schema_stub

    def test_if_source_schema_is_stored_as_instance_attribute(self, link, source_schema_stub):
        assert link.source_schema is source_schema_stub

    def test_if_stores_is_stored_as_instance_attribute(self, link, stores):
        assert link.stores is stores

    def test_if_stores_is_empty_dict_if_not_provided(self, local_schema_stub, source_schema_stub):
        assert Link(local_schema_stub, source_schema_stub).stores == dict()


@pytest.fixture
def prepare_env():
    os.environ["LINK_OUTBOUND"] = "outbound_schema"


@pytest.fixture
def linked_table(link, dummy_cls):
    return link(dummy_cls)


@pytest.fixture
def basic_outbound_config(table_name, schema_cls_spy):
    return dict(
        schema=schema_cls_spy.return_value,
        table_name=table_name + "Outbound",
        flag_table_names=["DeletionRequested", "DeletionApproved"],
    )


@pytest.fixture
def basic_local_config(local_schema_stub, table_name):
    return dict(
        schema=local_schema_stub,
        table_name=table_name,
        table_bases=(LocalTableMixin,),
        flag_table_names=["DeletionRequested"],
    )


@pytest.mark.usefixtures("prepare_env", "linked_table")
class TestCallWithoutInitialSetup:
    def test_if_configuration_of_source_table_cls_factory_is_correct(
        self, table_cls_factory_spies, source_schema_stub, table_name
    ):
        assert table_cls_factory_spies["source"].config == TableFactoryConfig(source_schema_stub, table_name)

    def test_if_call_to_schema_class_is_correct(self, source_schema_stub, schema_cls_spy):
        schema_cls_spy.assert_called_once_with("outbound_schema", connection=source_schema_stub.connection)

    def test_if_configuration_of_outbound_table_cls_factory_is_correct(
        self, table_cls_factory_spies, basic_outbound_config
    ):
        assert table_cls_factory_spies["outbound"].config == TableFactoryConfig(**basic_outbound_config)

    def test_if_configuration_of_local_table_cls_factory_is_correct(self, table_cls_factory_spies, basic_local_config):
        assert table_cls_factory_spies["local"].config == TableFactoryConfig(**basic_local_config)

    def test_if_call_to_local_table_cls_factory_is_correct(self, table_cls_factory_spies):
        table_cls_factory_spies["local"].assert_called_once_with()

    def test_if_local_table_class_is_returned(self, linked_table):
        assert linked_table == "local_table_cls"


@pytest.fixture
def initial_setup_required(table_cls_factory_spies):
    table_cls_factory_spies["local"].side_effect = [RuntimeError, "local_table_cls"]


@pytest.mark.usefixtures("initial_setup_required", "linked_table")
class TestCallWithInitialSetup:
    def test_if_source_table_cls_factory_is_called(self, table_cls_factory_spies):
        assert table_cls_factory_spies["source"].call_args_list == [call(), call()]

    def test_if_configuration_of_outbound_table_cls_factory_is_correct(
        self, table_cls_factory_spies, basic_outbound_config, dummy_base_table_cls, source_table_cls_stub
    ):
        assert table_cls_factory_spies["outbound"].config == TableFactoryConfig(
            **basic_outbound_config,
            table_cls=dummy_base_table_cls,
            table_cls_attrs=dict(source_table=source_table_cls_stub),
            table_definition="-> self.source_table",
        )

    def test_if_outbound_table_cls_factory_is_called(self, table_cls_factory_spies):
        table_cls_factory_spies["outbound"].assert_called_once_with()

    def test_if_calls_to_replace_stores_func_are_correct(self, replace_stores_spy, stores):
        assert replace_stores_spy.call_args_list == [
            call("source_master_heading", stores),
            call("source_part_a_heading", stores),
            call("source_part_b_heading", stores),
            call("source_part_c_heading", stores),
        ]

    def test_if_configuration_of_local_table_cls_factory_is_correct(
        self, table_cls_factory_spies, basic_local_config, dummy_base_table_cls
    ):
        assert table_cls_factory_spies["local"].config == TableFactoryConfig(
            **basic_local_config,
            table_cls=dummy_base_table_cls,
            table_definition="replaced_heading",
            part_table_definitions=dict(PartA="replaced_heading", PartB="replaced_heading", PartC="replaced_heading"),
        )

    def test_if_calls_to_local_table_cls_factory_are_correct(self, table_cls_factory_spies):
        assert table_cls_factory_spies["local"].call_args_list == [call(), call()]

    def test_if_local_table_class_is_returned(self, linked_table):
        assert linked_table is "local_table_cls"
