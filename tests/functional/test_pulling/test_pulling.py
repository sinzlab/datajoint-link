import pytest


@pytest.fixture
def pulled_data(local_table_cls):
    local_table_cls().pull()
    return local_table_cls().fetch(as_dict=True)


@pytest.fixture
def expected_pulled_data(src_data, src_db_config):
    return [dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name) for e in src_data]


@pytest.mark.usefixtures("src_table_with_data")
def test_pulling(pulled_data, expected_pulled_data):
    assert pulled_data == expected_pulled_data
