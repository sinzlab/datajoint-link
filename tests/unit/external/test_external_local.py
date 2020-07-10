from unittest.mock import MagicMock

import pytest

from link.external.outbound import OutboundTableFactory


@pytest.fixture
def factory_type():
    return "local"


@pytest.fixture
def factory_args(source_table_factory, created_table_cls):
    return [created_table_cls, source_table_factory]


@pytest.fixture
def source_table_factory():
    name = "source_table_factory"
    source_table_factory = MagicMock(name=name)
    source_table_factory.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return source_table_factory


def test_if_subclass_of_outbound_table_factory(factory_cls):
    assert issubclass(factory_cls, OutboundTableFactory)


def test_if_source_table_factory_is_stored_as_instance_attribute(factory, source_table_factory):
    assert factory.source_table_factory is source_table_factory


@pytest.mark.usefixtures("configure")
class TestSpawnTableCls:
    def test_if_returned_class_is_subclass_of_spawned_table_cls(self, factory, spawned_table_cls):
        assert issubclass(factory.spawn_table_cls(), spawned_table_cls)

    def test_if_returned_class_is_subclass_of_created_table_cls(self, factory, created_table_cls):
        assert issubclass(factory.spawn_table_cls(), created_table_cls)

    def test_if_name_attribute_of_returned_class_is_correctly_set(self, factory, table_name):
        assert factory.spawn_table_cls().__name__ == table_name


def test_repr(factory, created_table_cls):
    assert repr(factory) == f"LocalTableFactory({created_table_cls}, source_table_factory)"
