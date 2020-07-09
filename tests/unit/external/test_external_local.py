from unittest.mock import MagicMock

import pytest
from datajoint.errors import LostConnectionError

from link.external.outbound import OutboundTableFactory


@pytest.fixture
def factory_type():
    return "local"


@pytest.fixture
def factory_args(source_table_factory, table_cls):
    return [table_cls, source_table_factory]


@pytest.fixture
def source_table_factory():
    name = "source_table_factory"
    source_table_factory = MagicMock(name=name)
    source_table_factory.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return source_table_factory


@pytest.fixture
def mock_spawn_table_cls(factory, table_cls):
    factory.spawn_table_cls = MagicMock(name="LocalTableFactory.spawn_table", return_value=table_cls)


def test_if_subclass_of_outbound_table_factory(factory_cls):
    assert issubclass(factory_cls, OutboundTableFactory)


def test_if_source_table_factory_is_stored_as_instance_attribute(factory, source_table_factory):
    assert factory.source_table_factory is source_table_factory


@pytest.mark.usefixtures("mock_spawn_table_cls")
class TestCall:
    @pytest.fixture
    def mock_create_table_cls(self, factory, table_cls):
        factory.create_table_cls = MagicMock(name="LocalTableFactory.create_table", return_value=table_cls)

    @pytest.fixture
    def local_table_not_created(self, factory, mock_spawn_table_cls):
        factory.spawn_table_cls.side_effect = KeyError

    def test_if_local_table_is_spawned(self, factory):
        factory()
        factory.spawn_table_cls.assert_called_once_with()

    def test_if_spawned_table_is_returned(self, factory, table):
        assert factory() == table

    @pytest.mark.usefixtures("local_table_not_created", "mock_create_table_cls")
    def test_if_local_table_is_created_if_not_already_created(self, factory):
        factory()
        factory.create_table_cls.assert_called_once_with()

    @pytest.mark.usefixtures("local_table_not_created", "mock_create_table_cls")
    def test_if_created_table_is_returned(self, factory, table):
        assert factory() == table

    @pytest.mark.usefixtures("local_table_not_created", "mock_create_table_cls")
    def test_if_runtime_error_is_raised_if_local_table_can_not_be_spawned_or_created(self, factory):
        factory.create_table_cls.side_effect = LostConnectionError
        with pytest.raises(RuntimeError):
            factory()
