import os

import pytest

from link import main


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
