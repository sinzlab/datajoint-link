from unittest.mock import create_autospec

import pytest

from link.entities.transaction_manager import TransactionManager
from link.entities.abstract_gateway import AbstractGateway
from link.base import Base


@pytest.fixture
def gateway_spy():
    return create_autospec(AbstractGateway, instance=True)


@pytest.fixture
def manager(entities, gateway_spy):
    return TransactionManager(entities, gateway_spy)


def test_if_transaction_manager_inherits_from_base():
    assert issubclass(TransactionManager, Base)


class TestInit:
    def test_if_entities_are_stored_as_instance_attribute(self, manager, entities):
        assert manager.entities is entities

    def test_if_gateway_is_stored_as_instance_attribute(self, manager, gateway_spy):
        assert manager.gateway is gateway_spy

    def test_if_manager_is_not_in_transaction_after_initialization(self, manager):
        assert manager.in_transaction is False


class TestStart:
    def test_if_transaction_is_started_in_gateway(self, manager, gateway_spy):
        manager.start()
        gateway_spy.start_transaction.assert_called_once_with()

    def test_if_manager_is_in_transaction_after_it_is_started(self, manager):
        manager.start()
        assert manager.in_transaction is True

    def test_if_manager_is_not_in_transaction_after_it_is_started_but_fails_in_gateway(self, manager, gateway_spy):
        gateway_spy.start_transaction.side_effect = RuntimeError
        try:
            manager.start()
        except RuntimeError:
            pass
        assert manager.in_transaction is False


class TestCommit:
    def test_if_transaction_is_committed_in_gateway(self, manager, gateway_spy):
        manager.start()
        manager.commit()
        gateway_spy.commit_transaction.assert_called_once_with()

    def test_if_manager_is_no_longer_in_transaction_after_it_is_committed(self, manager):
        manager.start()
        manager.commit()
        assert manager.in_transaction is False

    def test_if_manager_is_still_in_transaction_after_it_is_committed_but_fails_in_gateway(self, manager, gateway_spy):
        gateway_spy.commit_transaction.side_effect = RuntimeError
        manager.start()
        try:
            manager.commit()
        except RuntimeError:
            pass
        assert manager.in_transaction is True


class TestCancel:
    def test_if_transaction_is_cancelled_in_gateway(self, manager, gateway_spy):
        manager.start()
        manager.cancel()
        gateway_spy.cancel_transaction.assert_called_once_with()

    def test_if_changes_are_rolled_back_if_transaction_is_cancelled(self, manager, entities):
        original = entities.copy()
        manager.start()
        entities.clear()
        manager.cancel()
        assert entities == original

    def test_if_manager_is_no_longer_in_transaction_after_it_is_cancelled(self, manager):
        manager.start()
        manager.cancel()
        assert manager.in_transaction is False

    def test_if_manager_is_still_in_transaction_after_it_is_cancelled_but_errors_in_gateway(self, manager, gateway_spy):
        gateway_spy.cancel_transaction.side_effect = RuntimeError
        manager.start()
        try:
            manager.cancel()
        except RuntimeError:
            pass
        assert manager.in_transaction is True


class TestTransaction:
    def test_if_start_is_called(self, manager, wrap_spy_around_method):
        wrap_spy_around_method(manager, "start")
        with manager.transaction():
            pass
        manager.start.assert_called_once_with()

    def test_if_commit_is_called(self, manager, wrap_spy_around_method):
        wrap_spy_around_method(manager, "commit")
        with manager.transaction():
            pass
        manager.commit.assert_called_once_with()

    def test_if_cancel_is_called_after_runtime_error_is_raised(self, manager, wrap_spy_around_method):
        wrap_spy_around_method(manager, "cancel")
        with manager.transaction():
            raise RuntimeError
        manager.cancel.assert_called_once_with()
