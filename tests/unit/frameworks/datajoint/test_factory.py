from copy import deepcopy
from dataclasses import is_dataclass
from functools import partial
from unittest.mock import DEFAULT, MagicMock, create_autospec

import pytest
from datajoint import Part

from dj_link.base import Base
from dj_link.frameworks.datajoint.factory import TableFactory, TableFactoryConfig


class TestTableFactoryConfig:
    def test_if_dataclass(self):
        assert is_dataclass(TableFactoryConfig)

    @pytest.fixture
    def partial_config_cls(self):
        return partial(TableFactoryConfig, MagicMock(name="dummy_schema"), "table_name")

    def test_if_table_bases_are_empty_tuple_if_not_provided(self, partial_config_cls):
        assert partial_config_cls().table_bases == tuple()

    def test_if_table_class_attributes_are_empty_dict_if_not_provided(self, partial_config_cls):
        assert partial_config_cls().table_cls_attrs == dict()

    def test_if_flag_table_names_are_empty_list_if_not_provided(self, partial_config_cls):
        assert partial_config_cls().flag_table_names == list()

    def test_if_table_cls_is_none_if_not_provided(self, partial_config_cls):
        assert partial_config_cls().table_cls is None

    def test_if_table_definition_is_none_if_not_provided(self, partial_config_cls):
        assert partial_config_cls().table_definition is None

    def test_if_part_table_definitions_are_empty_dict_if_not_provided(self, partial_config_cls):
        assert partial_config_cls().part_table_definitions == dict()

    @pytest.fixture
    def dummy_table_base_cls(self):
        return MagicMock(name="dummy_table_base_cls")

    def test_if_table_creation_is_possible_if_table_class_and_definition_are_provided(
        self, partial_config_cls, dummy_table_base_cls
    ):
        assert (
            partial_config_cls(table_cls=dummy_table_base_cls, table_definition="definition").is_table_creation_possible
            is True
        )

    def test_if_table_creation_is_not_possible_if_table_class_is_not_provided(self, partial_config_cls):
        assert partial_config_cls(table_definition="definition").is_table_creation_possible is False

    def test_if_table_creation_is_not_possible_if_table_definition_is_not_provided(
        self, partial_config_cls, dummy_table_base_cls
    ):
        assert partial_config_cls(table_cls=dummy_table_base_cls).is_table_creation_possible is False

    def test_if_table_creation_is_not_possible_if_table_class_and_definition_are_not_provided(self, partial_config_cls):
        assert partial_config_cls().is_table_creation_possible is False


@pytest.fixture
def factory():
    return TableFactory()


def test_if_table_factory_is_subclass_of_base():
    assert issubclass(TableFactory, Base)


def test_if_runtime_error_is_raised_if_config_is_accessed_while_not_being_set(factory):
    with pytest.raises(RuntimeError) as exc_info:
        _ = factory.config
    assert str(exc_info.value) == "Config is not set"


@pytest.fixture
def fake_schema(dummy_table_cls):
    class FakeSchema:
        def __init__(self, table_classes):
            self.table_classes = set(table_classes)

        def spawn_missing_classes(self, context=None):
            for missing_class in self.table_classes:
                context[missing_class.__name__] = missing_class

        def __call__(self, table_cls):
            self.table_classes.add(table_cls)
            table_cls.schema_applied = True
            return table_cls

    return FakeSchema([dummy_table_cls])


@pytest.fixture
def table_name():
    return "Table"


@pytest.fixture
def table_bases():
    return tuple(type(name, tuple(), dict()) for name in ("BaseClass", "AnotherBaseClass"))


@pytest.fixture
def table_cls_attrs():
    return dict(some_attr=10, another_attr="Hello!")


@pytest.fixture
def flag_part_table_names():
    return ["SomeFlagTable", "AnotherFlagTable"]


@pytest.fixture
def flag_part_tables(flag_part_table_names):
    return {name: type(name, (Part,), dict(definition="-> master")) for name in flag_part_table_names}


@pytest.fixture
def configure_for_spawning(factory, fake_schema, table_name, table_bases, table_cls_attrs, flag_part_table_names):
    config = create_autospec(TableFactoryConfig, instance=True)
    config.schema = fake_schema
    config.table_name = table_name
    config.table_bases = table_bases
    config.table_cls_attrs = table_cls_attrs
    config.flag_table_names = flag_part_table_names
    config.table_cls = None
    config.table_definition = None
    config.part_table_definitions = {}
    config.is_table_creation_possible = False
    factory.config = config


@pytest.fixture
def dummy_table_base_cls():
    class DummyTableBase:
        pass

    return DummyTableBase


@pytest.fixture
def table_definition():
    return "some definition"


@pytest.fixture
def non_flag_part_table_names():
    return ["SomePartTable", "AnotherPartTable"]


@pytest.fixture
def non_flag_part_table_definitions(non_flag_part_table_names):
    return {name: name + "_definition" for name in non_flag_part_table_names}


@pytest.fixture
def non_flag_part_tables(non_flag_part_table_definitions):
    return {
        name: type(name, (Part,), dict(definition=definition))
        for name, definition in non_flag_part_table_definitions.items()
    }


@pytest.fixture
def part_tables(flag_part_tables, non_flag_part_tables):
    return {**flag_part_tables, **non_flag_part_tables}


@pytest.fixture
def dummy_table_cls(table_name, dummy_table_base_cls, part_tables):
    return type(table_name, (dummy_table_base_cls,), part_tables)


@pytest.fixture
def configure_for_creating(
    factory,
    fake_schema,
    table_name,
    table_bases,
    table_cls_attrs,
    flag_part_table_names,
    dummy_table_base_cls,
    table_definition,
    non_flag_part_table_definitions,
):
    config = create_autospec(TableFactoryConfig, instance=True)
    config.schema = fake_schema
    config.table_name = table_name
    config.table_bases = table_bases
    config.table_cls_attrs = table_cls_attrs
    config.flag_table_names = flag_part_table_names
    config.table_cls = dummy_table_base_cls
    config.table_definition = table_definition
    config.part_table_definitions = non_flag_part_table_definitions
    config.is_table_creation_possible = True
    factory.config = config


@pytest.fixture
def table_can_not_be_spawned(fake_schema):
    fake_schema.table_classes = set()


@pytest.fixture
def returned_non_flag_part_tables(factory, non_flag_part_table_definitions):
    return {name: getattr(factory(), name) for name in non_flag_part_table_definitions}


class TestCall:
    @pytest.mark.usefixtures("configure_for_spawning")
    def test_if_name_of_spawned_table_class_is_correct(self, factory, table_name):
        assert factory().__name__ == table_name

    @pytest.mark.usefixtures("configure_for_spawning")
    def test_if_spawned_table_class_is_subclass_of_spawned_table_class_table_bases(
        self, factory, dummy_table_cls, table_bases
    ):
        for cls in (dummy_table_cls,) + table_bases:
            assert issubclass(factory(), cls)

    @pytest.mark.usefixtures("configure_for_spawning")
    def test_if_class_attributes_are_set_on_spawned_table_class(self, factory, table_cls_attrs):
        for name, value in table_cls_attrs.items():
            assert getattr(factory(), name) == value

    @pytest.mark.usefixtures("configure_for_spawning", "table_can_not_be_spawned")
    def test_if_runtime_error_is_raised_if_spawning_fails_and_table_creation_is_not_possible(self, factory):
        with pytest.raises(RuntimeError) as exc_info:
            factory()
        assert str(exc_info.value) == "Table could neither be spawned nor created"

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_created_table_class_is_subclass_of_table_class_and_table_bases(
        self, factory, dummy_table_base_cls, table_bases
    ):
        for cls in (dummy_table_base_cls,) + table_bases:
            assert issubclass(factory(), cls)

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_name_of_created_table_class_is_correct(self, factory, table_name):
        assert factory().__name__ == table_name

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_definition_of_created_table_class_is_correct(self, factory, table_definition):
        assert factory().definition == table_definition

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_flag_tables_are_part_tables(self, factory, flag_part_table_names):
        assert all(issubclass(getattr(factory(), name), Part) for name in flag_part_table_names)

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_names_of_flag_tables_are_correct(self, factory, flag_part_table_names):
        assert all(getattr(factory(), name).__name__ == name for name in flag_part_table_names)

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_definitions_of_flag_tables_are_correct(self, factory, flag_part_table_names):
        assert all(getattr(factory(), name).definition == "-> master" for name in flag_part_table_names)

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_part_tables_are_part_tables(self, returned_non_flag_part_tables):
        assert all(issubclass(part, Part) for part in returned_non_flag_part_tables.values())

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_names_of_non_flag_tables_are_correct(self, returned_non_flag_part_tables):
        assert all(part.__name__ == name for name, part in returned_non_flag_part_tables.items())

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_definitions_of_part_tables_are_correct(
        self, returned_non_flag_part_tables, non_flag_part_table_definitions
    ):
        assert all(
            part.definition == non_flag_part_table_definitions[name]
            for name, part in returned_non_flag_part_tables.items()
        )

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_class_attributes_are_set_on_created_table_class(self, factory, table_cls_attrs):
        assert all(getattr(factory(), name) == value for name, value in table_cls_attrs.items())

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_schema_is_applied_to_created_table_class(self, factory):
        # noinspection PyUnresolvedReferences
        assert factory().schema_applied

    @pytest.mark.usefixtures("configure_for_creating", "table_can_not_be_spawned")
    def test_if_table_base_class_is_subclassed_before_being_passed_to_schema(
        self, factory, fake_schema, dummy_table_base_cls
    ):
        factory()
        assert dummy_table_base_cls not in fake_schema.table_classes


@pytest.mark.usefixtures("configure_for_spawning")
def test_if_part_tables_attribute_is_correct(factory, non_flag_part_tables):
    assert factory.part_tables == non_flag_part_tables


@pytest.mark.usefixtures("configure_for_spawning")
def test_if_flag_tables_attribute_is_correct(factory, flag_part_tables):
    assert factory.flag_tables == flag_part_tables
