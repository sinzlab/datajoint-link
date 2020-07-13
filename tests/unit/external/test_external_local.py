from unittest.mock import MagicMock, call

import pytest
from datajoint import Part

from link.external.outbound import OutboundTableFactory


@pytest.fixture
def factory_type():
    return "local"


@pytest.fixture
def factory_args(source_table_factory, created_table_cls):
    return [created_table_cls, source_table_factory]


@pytest.fixture
def make_heading():
    def _make_heading(prefix):
        name = prefix + "_heading"
        heading = MagicMock(name=name)
        heading.__str__ = MagicMock(name=name + ".__str__", return_value=name)
        return heading

    return _make_heading


@pytest.fixture
def source_table_part_names():
    return ["PartA", "PartB", "PartC"]


@pytest.fixture
def source_table_parts(make_heading, source_table_part_names):
    return {name: type(name, tuple(), dict(heading=make_heading(name))) for name in source_table_part_names}


@pytest.fixture
def source_table(make_heading, source_table_parts):
    return MagicMock(name="source_table", heading=make_heading("source_table"), parts=source_table_parts)


@pytest.fixture
def source_table_factory(source_table):
    name = "source_table_factory"
    source_table_factory = MagicMock(name=name, return_value=source_table)
    source_table_factory.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return source_table_factory


@pytest.fixture
def replace_stores():
    return MagicMock(name="replace_stores", side_effect=lambda heading: heading + "_with_replaced_stores")


@pytest.fixture
def configure_kwargs(configure_kwargs, replace_stores):
    configure_kwargs["replace_stores"] = replace_stores
    return configure_kwargs


def test_if_subclass_of_outbound_table_factory(factory_cls):
    assert issubclass(factory_cls, OutboundTableFactory)


def test_if_replace_stores_is_none(factory):
    assert factory.replace_stores is None


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


@pytest.mark.usefixtures("configure")
class TestCreateTableCls:
    @pytest.fixture
    def local_table_cls(self, factory):
        return factory.create_table_cls()

    def test_if_replace_stores_is_called_correctly(self, factory, replace_stores):
        factory.create_table_cls()
        assert replace_stores.mock_calls == [
            call("source_table_heading"),
            call("PartA_heading"),
            call("PartB_heading"),
            call("PartC_heading"),
        ]

    def test_if_source_table_factory_is_called_correctly(self, factory, source_table_factory):
        factory.create_table_cls()
        assert source_table_factory.mock_calls == [call(), call(), call()]

    def test_if_source_table_heading_with_replaced_stores_is_set_as_definition_of_created_table_cls(
        self, local_table_cls
    ):
        assert local_table_cls.definition == "source_table_heading_with_replaced_stores"

    def test_if_source_part_tables_are_assigned_to_created_table_cls(self, local_table_cls, source_table_part_names):
        assert all(hasattr(local_table_cls, part_name) for part_name in source_table_part_names)

    def test_if_references_to_assigned_part_table_classes_is_stored_in_parts_attribute(
        self, local_table_cls, source_table_part_names
    ):
        assert all(name in local_table_cls.parts for name in source_table_part_names)

    def test_if_part_tables_are_part_tables(self, local_table_cls, source_table_part_names):
        assert all(issubclass(part, Part) for part in local_table_cls.parts.values())

    def test_if_definition_of_part_tables_is_correct(self, local_table_cls, source_table_part_names):
        assert all(
            part.definition == f"-> master\n{name}_heading_with_replaced_stores"
            for name, part in local_table_cls.parts.items()
        )


def test_repr(factory, created_table_cls):
    assert repr(factory) == f"LocalTableFactory({created_table_cls}, source_table_factory)"
