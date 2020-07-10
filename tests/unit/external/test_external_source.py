from unittest.mock import MagicMock, DEFAULT
from copy import deepcopy

import pytest
from datajoint import Part


@pytest.fixture
def factory_type():
    return "source"


@pytest.fixture
def factory_args():
    return list()


@pytest.fixture
def copy_call_args():
    def _copy_call_args(mock):
        new_mock = MagicMock()

        def side_effect(*args, **kwargs):
            args = deepcopy(args)
            kwargs = deepcopy(kwargs)
            new_mock(*args, **kwargs)
            return DEFAULT

        mock.side_effect = side_effect
        return new_mock

    return _copy_call_args


def test_if_schema_is_none(factory):
    assert factory.schema is None


def test_if_table_name_is_none(factory):
    assert factory.table_name is None


@pytest.mark.usefixtures("configure")
class TestSpawnTableClass:
    @pytest.fixture
    def lowercase_attr(self):
        return dict(lowercae_attr="lowercase_attr")

    @pytest.fixture
    def ignored_part_table(self, factory):
        class IgnoredPartTable(Part):
            definition = ""

        factory._ignored_parts = [IgnoredPartTable.__name__]
        return dict(IgnoredPartTable=IgnoredPartTable)

    @pytest.fixture
    def non_cls_attr(self):
        return dict(NonClsAttr="NonClsAttr")

    @pytest.fixture
    def non_part_cls(self):
        class NotPartTable:
            pass

        return dict(NotPartTable=NotPartTable)

    @pytest.fixture
    def part_table(self):
        class PartTable(Part):
            definition = ""

        return dict(PartTable=PartTable)

    @pytest.fixture
    def attrs(self, lowercase_attr, ignored_part_table, non_cls_attr, non_part_cls, part_table):
        return {**lowercase_attr, **ignored_part_table, **non_cls_attr, **non_part_cls, **part_table}

    @pytest.fixture
    def add_attrs_to_spawned_table_cls(self, spawned_table_cls, attrs):
        for name, attr in attrs.items():
            setattr(spawned_table_cls, name, attr)

    def test_if_missing_classes_are_spawned(self, factory, schema, copy_call_args):
        new_mock = copy_call_args(schema.spawn_missing_classes)
        factory.spawn_table_cls()
        new_mock.assert_called_once_with(context=dict())

    def test_if_table_cls_is_returned(self, factory, spawned_table_cls):
        assert factory.spawn_table_cls() is spawned_table_cls

    @pytest.mark.usefixtures("add_attrs_to_spawned_table_cls")
    def test_if_part_tables_attribute_is_set_correctly(self, factory, spawned_table_cls, part_table):
        factory()
        assert spawned_table_cls.parts == part_table


@pytest.mark.usefixtures("configure")
class TestCall:
    def test_if_source_table_is_returned(self, factory, spawned_table_cls):
        assert isinstance(factory(), spawned_table_cls)


def test_repr(factory):
    assert repr(factory) == "SourceTableFactory()"
