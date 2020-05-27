import pytest
import datajoint as dj


@pytest.fixture
def src_part_definition():
    return """
    -> master
    ---
    sec_attr: int
    """


@pytest.fixture
def src_part_cls(src_part_definition):
    class Part(dj.Part):
        definition = src_part_definition

    return Part


@pytest.fixture
def src_table_cls(src_table_cls, src_part_cls):
    src_table_cls.Part = src_part_cls
    return src_table_cls


@pytest.fixture
def src_part_data(src_data):
    return [dict(prim_attr=e["prim_attr"], sec_attr=e["sec_attr"] * 2) for e in src_data]


@pytest.fixture
def src_table_with_data(src_table_with_data, src_part_data):
    src_table_with_data.Part().insert(src_part_data)
    return src_table_with_data


@pytest.fixture
def pulled_data(local_table_cls):
    local_table_cls().pull()
    return dict(master=local_table_cls().fetch(as_dict=True), part=local_table_cls.Part().fetch(as_dict=True))


@pytest.fixture
def expected_pulled_data(src_data, src_part_data, src_db_config):
    return dict(
        master=[dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name) for e in src_data],
        part=[dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name) for e in src_part_data],
    )


@pytest.mark.usefixtures("src_table_with_data")
def test_pulling(pulled_data, expected_pulled_data):
    assert pulled_data == expected_pulled_data
