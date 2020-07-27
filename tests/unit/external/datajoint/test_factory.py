from unittest.mock import MagicMock, DEFAULT
from copy import deepcopy

import pytest
from datajoint import Lookup, Part

from link.external.datajoint.factory import TableFactory, SpawnTableConfig, CreateTableConfig


@pytest.fixture
def factory():
    return TableFactory()


class TestInit:
    def test_if_spawn_table_config_is_none(self, factory):
        assert factory.spawn_table_config is None

    def test_if_create_table_config_is_none(self, factory):
        assert factory.create_table_config is None


@pytest.fixture
def fake_schema(table_name, dummy_spawned_table_cls):
    class FakeSchema:
        @staticmethod
        def spawn_missing_classes(context=None):
            context[table_name] = dummy_spawned_table_cls

        def __call__(self, table_cls):
            table_cls.schema_applied = True
            return table_cls

    FakeSchema.spawn_missing_classes = MagicMock(
        name="FakeSchema.spawn_missing_classes", wraps=FakeSchema.spawn_missing_classes
    )
    return FakeSchema()


@pytest.fixture
def table_name():
    return "table"


@pytest.fixture
def table_cls_attrs():
    return dict(some_attr=10, another_attr="Hello!")


@pytest.fixture
def flag_part_table_names():
    return ["SomeFlagTable", "AnotherFlagTable"]


@pytest.fixture
def spawn_table_config(fake_schema, table_name, table_cls_attrs, flag_part_table_names):
    return SpawnTableConfig(fake_schema, table_name, table_cls_attrs, flag_part_table_names)


@pytest.fixture
def add_spawn_table_config(factory, spawn_table_config):
    factory.spawn_table_config = spawn_table_config


@pytest.fixture
def table_definition():
    return "some definition"


@pytest.fixture
def non_flag_part_table_definitions():
    return dict(SomePartTable="some part table definition", AnotherPartTable="another part table definition")


@pytest.fixture
def dummy_spawned_table_cls():
    class DummySpawnedTable:
        def __eq__(self, other):
            return isinstance(other, self.__class__)

    return DummySpawnedTable


@pytest.fixture
def create_table_config(table_definition, non_flag_part_table_definitions):
    return CreateTableConfig(table_definition, non_flag_part_table_definitions)


@pytest.fixture
def add_create_table_config(factory, add_spawn_table_config, create_table_config, fake_schema):
    factory.create_table_config = create_table_config


@pytest.fixture
def table_can_not_be_spawned(fake_schema):
    fake_schema.spawn_missing_classes.side_effect = KeyError


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


@pytest.fixture
def part_tables(factory, non_flag_part_table_definitions):
    return {name: getattr(factory(), name) for name in non_flag_part_table_definitions}


class TestCall:
    def test_if_runtime_error_is_raised_if_spawn_table_config_attribute_is_not_set(self, factory):
        with pytest.raises(RuntimeError):
            factory()

    @pytest.mark.usefixtures("add_spawn_table_config")
    def test_if_call_to_spawn_missing_classes_method_of_schema_is_correct(self, factory, fake_schema, copy_call_args):
        copied_call_args_mock = copy_call_args(fake_schema.spawn_missing_classes)
        factory()
        copied_call_args_mock.assert_called_once_with(context=dict())

    @pytest.mark.usefixtures("add_spawn_table_config")
    def test_if_class_attributes_are_set_on_spawned_table_class(
        self, factory, dummy_spawned_table_cls, table_cls_attrs
    ):
        factory()
        assert all(getattr(dummy_spawned_table_cls, name) == value for name, value in table_cls_attrs.items())

    @pytest.mark.usefixtures("add_spawn_table_config")
    def test_if_spawned_table_is_returned(self, factory, dummy_spawned_table_cls):
        assert factory() == dummy_spawned_table_cls()

    @pytest.mark.usefixtures("add_spawn_table_config", "table_can_not_be_spawned")
    def test_if_runtime_error_is_raised_if_spawning_fails_and_factory_is_not_configured_to_create_table(self, factory):
        with pytest.raises(RuntimeError):
            factory()

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_created_table_is_lookup_table(self, factory):
        assert isinstance(factory(), Lookup)

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_name_of_created_table_is_correct(self, factory, table_name):
        assert factory().__class__.__name__ == table_name

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_definition_of_created_table_is_correct(self, factory, table_definition):
        assert factory().definition == table_definition

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_flag_tables_are_part_tables(self, factory, flag_part_table_names):
        assert all(issubclass(getattr(factory(), name), Part) for name in flag_part_table_names)

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_names_of_flag_tables_are_correct(self, factory, flag_part_table_names):
        assert all(getattr(factory(), name).__name__ == name for name in flag_part_table_names)

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_definitions_of_flag_tables_are_correct(self, factory, flag_part_table_names):
        assert all(getattr(factory(), name).definition == "-> master" for name in flag_part_table_names)

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_part_tables_are_part_tables(self, part_tables):
        assert all(issubclass(part, Part) for part in part_tables.values())

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_names_of_flag_tables_are_correct(self, part_tables):
        assert all(part.__name__ == name for name, part in part_tables.items())

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_definitions_of_part_tables_are_correct(self, part_tables, non_flag_part_table_definitions):
        assert all(part.definition == non_flag_part_table_definitions[name] for name, part in part_tables.items())

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_class_attributes_are_set_on_created_table_class(self, factory, table_cls_attrs):
        assert all(getattr(factory(), name) == value for name, value in table_cls_attrs.items())

    @pytest.mark.usefixtures("add_create_table_config", "table_can_not_be_spawned")
    def test_if_schema_is_applied_to_created_table_class(self, factory):
        assert factory().schema_applied
