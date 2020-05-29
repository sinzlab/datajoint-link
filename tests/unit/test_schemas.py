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


def test_if_value_error_is_raised_if_initialized_with_connection_and_host(connection):
    with pytest.raises(ValueError):
        schemas.LazySchema(schema_name, connection=connection, host="host")


@pytest.fixture
def schema():
    return MagicMock(name="schema", spec=Schema, some_attribute="some_value")


@pytest.fixture
def schema_cls(schema):
    return MagicMock(name="schema_cls", spec=Type[Schema], return_value=schema)


@pytest.fixture
def lazy_schema_cls(schema_cls):
    schemas.LazySchema._schema_cls = schema_cls
    return schemas.LazySchema


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
    def test_if_connection_cls_is_correctly_initialized_if_host_is_provided(
        self, lazy_schema_cls, schema_name, conn_cls
    ):
        lazy_schema_cls(schema_name, host="host").initialize()
        conn_cls.assert_called_once_with("host", "user", "pass")

    @pytest.mark.usefixtures("setup_env")
    def test_if_connection_is_passed_to_schema_if_host_is_provided(
        self, lazy_schema_cls, schema_name, connection, schema_cls
    ):
        lazy_schema_cls(schema_name, host="host").initialize()
        schema_cls.assert_called_once_with(
            schema_name=schema_name, context=None, connection=connection, create_schema=True, create_tables=True
        )

    def test_if_schema_name_is_passed(self, lazy_schema_cls, schema_name, schema_cls):
        lazy_schema_cls(schema_name).initialize()
        schema_cls.assert_called_once_with(
            schema_name=schema_name, context=None, connection=None, create_schema=True, create_tables=True
        )

    def test_if_context_is_passed_if_provided(self, lazy_schema_cls, schema_name, context, schema_cls):
        lazy_schema_cls(schema_name, context=context).initialize()
        schema_cls.assert_called_once_with(
            schema_name=schema_name, context=context, connection=None, create_schema=True, create_tables=True
        )

    def test_if_connection_is_passed_if_provided(self, lazy_schema_cls, schema_name, connection, schema_cls):
        lazy_schema_cls(schema_name, connection=connection).initialize()
        schema_cls.assert_called_once_with(
            schema_name=schema_name, context=None, connection=connection, create_schema=True, create_tables=True
        )

    def test_if_create_schema_is_passed_if_provided(self, lazy_schema_cls, schema_name, schema_cls):
        lazy_schema_cls(schema_name, create_schema=False).initialize()
        schema_cls.assert_called_once_with(
            schema_name=schema_name, context=None, connection=None, create_schema=False, create_tables=True
        )

    def test_if_create_tables_is_passed_if_provided(self, lazy_schema_cls, schema_name, schema_cls):
        lazy_schema_cls(schema_name, create_tables=False).initialize()
        schema_cls.assert_called_once_with(
            schema_name=schema_name, context=None, connection=None, create_schema=True, create_tables=False
        )

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


class TestGetAttr:
    def test_if_getattr_calls_initialize_correctly(self, lazy_schema, initialize_mock):
        lazy_schema.initialize = initialize_mock
        _ = lazy_schema.some_attribute
        initialize_mock.assert_called_once_with()

    def test_if_getattr_returns_correct_value(self, lazy_schema_cls, schema_name):
        lazy_schema = lazy_schema_cls(schema_name)
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

    def test_if_initialize_is_correctly_called(self, lazy_schema, initialize_mock, table_cls):
        lazy_schema.initialize = initialize_mock
        lazy_schema(table_cls)
        initialize_mock.assert_called_once_with()

    def test_if_call_calls_schema_correctly(self, lazy_schema_cls, schema_name, table_cls, schema):
        lazy_schema = lazy_schema_cls(schema_name)
        lazy_schema(table_cls)
        schema.assert_called_once_with(table_cls, context=None)

    def test_if_context_is_passed_if_provided(self, lazy_schema_cls, schema_name, context, table_cls, schema):
        lazy_schema = lazy_schema_cls(schema_name)
        lazy_schema(table_cls, context=context)
        schema.assert_called_once_with(table_cls, context=context)

    def test_if_call_returns_correct_value(self, lazy_schema_cls, schema_name, table_cls, processed_table_class):
        lazy_schema = lazy_schema_cls(schema_name)
        assert lazy_schema(table_cls) is processed_table_class
