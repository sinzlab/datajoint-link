import os

import pytest
import datajoint as dj

from link import main


@pytest.fixture
def src_table_definition():
    return """
    prim_attr: int
    ---
    sec_attr: int
    """


@pytest.fixture
def src_table_cls(src_table_definition):
    class Table(dj.Manual):
        definition = src_table_definition

    return Table


@pytest.fixture
def src_data():
    return [dict(prim_attr=i, sec_attr=-i) for i in range(10)]


@pytest.fixture
def src_table_with_data(src_schema, src_table_cls, src_data):
    src_table = src_schema(src_table_cls)
    src_table.insert(src_data)
    return src_table


@pytest.fixture
def remote_schema(src_db_config):
    os.environ["REMOTE_DJ_USER"] = src_db_config.users["dj_user"].name
    os.environ["REMOTE_DJ_PASS"] = src_db_config.users["dj_user"].password
    return main.SchemaProxy(src_db_config.schema_name, host=src_db_config.name)


@pytest.fixture
def local_table_cls(local_schema, remote_schema):
    @main.Link(local_schema, remote_schema)
    class Table:
        """Local table."""

    return Table


@pytest.fixture
def pulled_data(local_table_cls):
    local_table_cls().pull()
    return local_table_cls().fetch(as_dict=True)


@pytest.fixture
def expected_data(src_data, src_db_config):
    return [dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name) for e in src_data]
