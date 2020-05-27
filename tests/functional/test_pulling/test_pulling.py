import pytest
import datajoint as dj


@pytest.fixture
def src_table_cls():
    class Table(dj.Manual):
        definition = """
        prim_attr: int
        ---
        sec_attr: int
        """

    return Table


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
