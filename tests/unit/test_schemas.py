from unittest.mock import MagicMock
from typing import Type
import os

import pytest
from datajoint.connection import Connection
from datajoint.schemas import Schema
from datajoint.table import Table

from link import schemas


def test_if_schema_cls_is_correct():
    assert schemas.LazySchema._schema_cls is Schema


@pytest.fixture
def schema_name():
    return "schema_name"


@pytest.fixture
def context():
    return dict()


@pytest.fixture
def connection():
    return MagicMock(name="connection", spec=Connection)


@pytest.fixture
def schema():
    return MagicMock(name="schema", spec=Schema, some_attribute="some_value")


@pytest.fixture
def schema_cls(schema, connection):
    schema_cls = MagicMock(name="schema_cls", spec=Type[Schema], return_value=schema)
    schema_cls.return_value.connection = connection
    return schema_cls


@pytest.fixture
def lazy_schema_cls(schema_cls):
    class LazySchema(schemas.LazySchema):
        pass

    LazySchema.__qualname__ = LazySchema.__name__
    LazySchema._schema_cls = schema_cls
    return LazySchema


class TestInit:
    def test_if_value_error_is_raised_if_initialized_with_connection_and_host(self, connection):
        with pytest.raises(ValueError):
            schemas.LazySchema(schema_name, connection=connection, host="host")

    def test_if_schema_name_is_stored_as_instance_attribute(self, lazy_schema_cls, schema_name):
        assert lazy_schema_cls(schema_name).database == schema_name

    def test_if_context_is_stored_as_instance_attribute_if_provided(self, lazy_schema_cls, schema_name, context):
        assert lazy_schema_cls(schema_name, context=context).context is context

    def test_if_context_is_none_if_not_provided(self, lazy_schema_cls, schema_name):
        assert lazy_schema_cls(schema_name).context is None

    def test_if_connection_is_stored_as_instance_attribute_if_provided(self, lazy_schema_cls, schema_name, connection):
        assert lazy_schema_cls(schema_name, connection=connection).connection is connection

    def test_if_connection_is_none_if_not_provided(self, lazy_schema_cls, schema_name):
        assert lazy_schema_cls(schema_name).connection is None

    def test_if_create_schema_is_stored_as_instance_attribute_if_provided(self, lazy_schema_cls, schema_name):
        assert lazy_schema_cls(schema_name, create_schema=False).create_schema is False

    def test_if_create_schema_is_true_if_not_provided(self, lazy_schema_cls, schema_name):
        assert lazy_schema_cls(schema_name).create_schema is True

    def test_if_create_tables_is_stored_as_instance_attribute_if_provided(self, lazy_schema_cls, schema_name):
        assert lazy_schema_cls(schema_name, create_tables=False).create_tables is False

    def test_if_create_tables_is_true_if_not_provided(self, lazy_schema_cls, schema_name):
        assert lazy_schema_cls(schema_name).create_tables is True

    def test_if_host_is_stored_as_instance_attribute_if_provided(self, lazy_schema_cls, schema_name):
        assert lazy_schema_cls(schema_name, host="host").host == "host"

    def test_if_host_is_none_if_not_provided(self, lazy_schema_cls, schema_name):
        assert lazy_schema_cls(schema_name).host is None


class TestConnectionProperty:
    def test_if_trying_to_set_connection_raises_runtime_error_if_host_is_set(
        self, lazy_schema_cls, schema_name, connection
    ):
        lazy_schema = lazy_schema_cls(schema_name, host="host")
        with pytest.raises(RuntimeError):
            lazy_schema.connection = connection

    def test_if_setter_sets_property(self, lazy_schema_cls, schema_name, connection):
        lazy_schema = lazy_schema_cls(schema_name)
        lazy_schema.connection = connection
        assert lazy_schema.connection is connection


class TestHostProperty:
    def test_if_trying_to_set_host_raises_runtime_error_if_connection_is_set(
        self, lazy_schema_cls, schema_name, connection
    ):
        lazy_schema = lazy_schema_cls(schema_name, connection=connection)
        with pytest.raises(RuntimeError):
            lazy_schema.host = "host"

    def test_if_setter_sets_property(self, lazy_schema_cls, schema_name):
        lazy_schema = lazy_schema_cls(schema_name)
        lazy_schema.host = "host"
        assert lazy_schema.host is "host"


class TestInitialize:
    @pytest.fixture
    def setup_env(self):
        os.environ.update(REMOTE_DJ_USER="user", REMOTE_DJ_PASS="pass")

    @pytest.fixture
    def conn_cls(self, connection):
        return MagicMock(name="conn_cls", spec=Type[Connection], return_value=connection)

    @pytest.fixture
    def lazy_schema_cls(self, lazy_schema_cls, conn_cls):
        lazy_schema_cls._conn_cls = conn_cls
        return lazy_schema_cls

    @pytest.mark.usefixtures("setup_env")
    def test_if_connection_is_correctly_initialized_if_host_is_provided(self, lazy_schema_cls, schema_name, conn_cls):
        lazy_schema_cls(schema_name, host="host").initialize()
        conn_cls.assert_called_once_with("host", "user", "pass")

    @pytest.mark.usefixtures("setup_env")
    def test_if_initialized_connection_is_stored_as_instance_attribute_if_host_is_provided(
        self, lazy_schema_cls, schema_name, connection
    ):
        lazy_schema = lazy_schema_cls(schema_name, host="host")
        lazy_schema.initialize()
        assert lazy_schema.connection is connection

    @pytest.mark.usefixtures("setup_env")
    def test_if_schema_is_correctly_initialized(self, lazy_schema_cls, schema_name, schema_cls):
        lazy_schema_cls(schema_name).initialize()
        schema_cls.assert_called_once_with(
            schema_name=schema_name, context=None, connection=None, create_schema=True, create_tables=True
        )

    def test_if_connection_of_regular_schema_is_stored_as_instance_attribute_if_no_connection_or_host_are_provided(
        self, lazy_schema_cls, schema_name, connection
    ):
        lazy_schema = lazy_schema_cls(schema_name)
        lazy_schema.initialize()
        assert lazy_schema.connection is connection

    def test_if_schema_is_not_initialized_again_if_initialize_is_called_twice(
        self, lazy_schema_cls, schema_name, schema_cls
    ):
        lazy_schema = lazy_schema_cls(schema_name)
        lazy_schema.initialize()
        lazy_schema.initialize()
        assert schema_cls.call_count == 1


@pytest.fixture
def lazy_schema(lazy_schema_cls, schema_name):
    return lazy_schema_cls(schema_name)


@pytest.fixture
def initialize_mock(lazy_schema):
    return MagicMock(name="initialize", wraps=lazy_schema.initialize)


@pytest.fixture
def lazy_schema_with_initialize_mock(lazy_schema, initialize_mock):
    lazy_schema.initialize = initialize_mock
    return lazy_schema


class TestSchemaProperty:
    def test_if_initialize_is_called_correctly(self, lazy_schema_with_initialize_mock, initialize_mock):
        _ = lazy_schema_with_initialize_mock.schema
        initialize_mock.assert_called_once_with()

    def test_if_schema_is_returned(self, lazy_schema, schema):
        assert lazy_schema.schema is schema


class TestGetAttr:
    def test_if_getattr_calls_initialize_correctly(self, lazy_schema_with_initialize_mock, initialize_mock):
        _ = lazy_schema_with_initialize_mock.some_attribute
        initialize_mock.assert_called_once_with()

    def test_if_getattr_returns_correct_value(self, lazy_schema):
        assert lazy_schema.some_attribute == "some_value"


class TestCall:
    @pytest.fixture
    def table_cls(self):
        return MagicMock(name="table_cls", spec=Type[Table])

    @pytest.fixture
    def processed_table_class(self):
        return MagicMock(name="processed_table_cls", spec=Type[Table])

    @pytest.fixture
    def schema(self, schema, processed_table_class):
        schema.return_value = processed_table_class
        return schema

    def test_if_initialize_is_correctly_called(self, lazy_schema_with_initialize_mock, initialize_mock, table_cls):
        lazy_schema_with_initialize_mock(table_cls)
        initialize_mock.assert_called_once_with()

    def test_if_call_calls_schema_correctly(self, lazy_schema, table_cls, schema):
        lazy_schema(table_cls)
        schema.assert_called_once_with(table_cls, context=None)

    def test_if_context_is_passed_if_provided(self, lazy_schema, context, table_cls, schema):
        lazy_schema(table_cls, context=context)
        schema.assert_called_once_with(table_cls, context=context)

    def test_if_call_returns_correct_value(self, lazy_schema, table_cls, processed_table_class):
        assert lazy_schema(table_cls) is processed_table_class


def test_if_repr_returns_correct_value(lazy_schema):
    assert repr(lazy_schema) == (
        "LazySchema(schema_name, context=None, connection=None, create_schema=True, create_tables=True, host=None)"
    )


class TestIsInitialized:
    def test_if_is_initialized_is_false_before_initializing(self, lazy_schema):
        assert lazy_schema.is_initialized is False

    def test_if_is_initialized_is_true_after_initializing(self, lazy_schema):
        lazy_schema.initialize()
        assert lazy_schema.is_initialized is True
