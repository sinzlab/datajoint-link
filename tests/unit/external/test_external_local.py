from unittest.mock import MagicMock

import pytest
from datajoint.errors import LostConnectionError

from link.external.outbound import OutboundTableFactory


@pytest.fixture
def factory_type():
    return "local"


@pytest.fixture
def factory_args(source_table_factory, table_cls):
    return [source_table_factory, table_cls]


@pytest.fixture
def mock_spawn_table(factory):
    factory.spawn_table = MagicMock(name="LocalTableFactory.spawn_table")


@pytest.fixture
def mock_create_table(factory):
    factory.create_table = MagicMock(name="LocalTableFactory.create_table")


def test_if_subclass_of_outbound_table_factory(factory_cls):
    assert issubclass(factory_cls, OutboundTableFactory)


@pytest.mark.usefixtures("mock_spawn_table")
def test_if_local_table_is_spawned(factory):
    factory()
    factory.spawn_table.assert_called_once_with()


@pytest.mark.usefixtures("mock_spawn_table", "mock_create_table")
def test_if_local_table_is_created_if_not_already_created(factory):
    factory.spawn_table.side_effect = KeyError
    factory()
    factory.create_table.assert_called_once_with()


@pytest.mark.usefixtures("mock_spawn_table", "mock_create_table")
def test_if_runtime_error_is_raised_if_local_table_can_not_be_spawned_or_created(factory):
    factory.spawn_table.side_effect = KeyError
    factory.create_table.side_effect = LostConnectionError
    with pytest.raises(RuntimeError):
        factory()
