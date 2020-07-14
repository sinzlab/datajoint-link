import pytest

from link.external.proxies import LocalTableProxy, OutboundTableProxy


def test_if_subclass_of_non_source_table_proxy():
    assert issubclass(OutboundTableProxy, LocalTableProxy)


@pytest.fixture
def proxy_cls():
    return OutboundTableProxy


class TestDeletionApprovedProperty:
    def test_if_table_is_instantiated(self, table_factory, proxy):
        _ = proxy.deletion_approved
        table_factory.assert_called_once_with()

    def test_if_primary_keys_of_deletion_approved_entities_are_fetched_correctly(self, table, proxy):
        _ = proxy.deletion_approved
        table.DeletionApproved.fetch.assert_called_once_with(as_dict=True)

    def test_if_primary_keys_of_deletion_approved_entities_are_returned(self, primary_keys, proxy):
        assert proxy.deletion_approved == primary_keys


class TestApproveDeletion:
    def test_if_table_is_instantiated(self, primary_keys, table_factory, proxy):
        proxy.approve_deletion(primary_keys)
        table_factory.assert_called_once_with()

    def test_if_primary_keys_are_inserted_into_deletion_approved_part_table(self, primary_keys, table, proxy):
        proxy.approve_deletion(primary_keys)
        table.DeletionApproved.insert.assert_called_once_with(primary_keys)
