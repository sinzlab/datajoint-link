from unittest.mock import MagicMock
from functools import partial

import pytest

from link.external.link import Link


def test_if_controller_is_none():
    assert Link.controller is None


@pytest.fixture
def link_cls():
    return Link


@pytest.fixture
def local_schema():
    return MagicMock(name="local_schema")


@pytest.fixture
def source_schema():
    return MagicMock(name="source_schema")


@pytest.fixture
def partial_link_cls(link_cls, local_schema, source_schema):
    return partial(link_cls, local_schema, source_schema)


@pytest.fixture
def stores():
    return dict(local_store="source_store")


class TestInit:
    def test_if_local_schema_is_stored_as_instance_attribute(self, partial_link_cls, local_schema):
        assert partial_link_cls().local_schema == local_schema

    def test_if_source_schema_is_stored_as_instance_attribute(self, partial_link_cls, source_schema):
        assert partial_link_cls().source_schema == source_schema

    def test_if_stores_attribute_is_none_if_not_provided(self, partial_link_cls):
        assert partial_link_cls().stores is None

    def test_if_stores_are_stored_as_instance_attribute_if_provided(self, partial_link_cls, stores):
        assert partial_link_cls(stores=stores).stores == stores

    def test_if_table_cls_is_none(self, partial_link_cls):
        assert partial_link_cls().table_cls is None


class TestCall:
    @pytest.fixture
    def controller(self):
        return MagicMock(name="controller")

    @pytest.fixture
    def link_cls(self, controller):
        Link.controller = controller
        return Link

    @pytest.fixture
    def table_cls(self):
        return MagicMock(name="table_cls", __name__="Table")

    @pytest.fixture
    def local_schema(self, local_schema):
        local_schema.host = "local_host"
        local_schema.database = "local_database"
        return local_schema

    @pytest.fixture
    def source_schema(self, source_schema):
        source_schema.host = "source_host"
        source_schema.database = "source_database"
        return source_schema

    def test_if_controller_is_correctly_called(self, partial_link_cls, table_cls, controller):
        partial_link_cls()(table_cls)
        controller.initialize.assert_called_once_with(
            "Table", "local_host", "local_database", "source_host", "source_database"
        )

    def test_if_table_class_is_returned(self, partial_link_cls, table_cls):
        partial_link = partial_link_cls()
        partial_link.table_cls = table_cls
        assert partial_link(table_cls) is table_cls
