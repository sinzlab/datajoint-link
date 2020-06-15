from functools import partial
from unittest.mock import MagicMock

import pytest

from link.adapters.link import LinkController


def test_if_initialize_use_case_is_none():
    assert LinkController.initialize_use_case is None


@pytest.fixture
def controller_cls():
    return LinkController


@pytest.fixture
def local_schema():
    return MagicMock(name="local_schema")


@pytest.fixture
def source_schema():
    return MagicMock(name="source_schema")


@pytest.fixture
def partial_controller_cls(controller_cls, local_schema, source_schema):
    return partial(controller_cls, local_schema, source_schema)


class TestInit:
    def test_if_local_schema_is_stored_as_instance_attribute(self, partial_controller_cls, local_schema):
        assert partial_controller_cls().local_schema == local_schema

    def test_if_source_schema_is_stored_as_instance_attribute(self, partial_controller_cls, source_schema):
        assert partial_controller_cls().source_schema == source_schema

    def test_if_stores_attribute_is_none_if_not_provided(self, partial_controller_cls):
        assert partial_controller_cls().stores is None

    def test_if_stores_are_stored_as_instance_attribute_if_provided(self, partial_controller_cls):
        stores = dict(local_store="source_store")
        assert partial_controller_cls(stores=stores).stores == stores

    def test_if_table_cls_is_none(self, partial_controller_cls):
        assert partial_controller_cls().table_cls is None


class TestCall:
    @pytest.fixture
    def initialize_use_case(self):
        return MagicMock(name="initialize_use_case")

    @pytest.fixture
    def controller_cls(self, initialize_use_case):
        LinkController.initialize_use_case = initialize_use_case
        return LinkController

    @pytest.fixture
    def table_cls(self):
        return MagicMock(name="table_cls", __name__="Table")

    @pytest.fixture
    def local_schema(self, local_schema):
        local_schema.host = "local_host"
        local_schema.database = "local_schema"
        return local_schema

    @pytest.fixture
    def source_schema(self, source_schema):
        source_schema.host = "source_host"
        source_schema.database = "source_schema"
        return source_schema

    def test_if_table_cls_is_stored_as_instance_attribute(self, partial_controller_cls, table_cls):
        controller = partial_controller_cls()
        controller(table_cls)
        assert controller.table_cls is table_cls

    def test_if_initialize_use_case_is_correctly_called(self, partial_controller_cls, table_cls, initialize_use_case):
        partial_controller_cls()(table_cls)
        initialize_use_case.assert_called_once_with(
            "Table", "local_host", "local_schema", "source_host", "source_schema"
        )
