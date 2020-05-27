import os
from tempfile import TemporaryDirectory

import pytest
import datajoint as dj

from link import main


@pytest.fixture
def src_table_cls(src_store_config):
    class Table(dj.Manual):
        definition = f"""
        prim_attr: int
        ---
        sec_attr: int
        ext_attr: attach@{src_store_config.name}
        """

    return Table


@pytest.fixture
def src_temp_dir():
    with TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def file_paths(src_temp_dir):
    file_paths = []
    for i in range(10):
        filename = os.path.join(src_temp_dir, f"src_external{i}.rand")
        with open(filename, "wb") as file:
            file.write(os.urandom(1024))
        file_paths.append(filename)
    return file_paths


@pytest.fixture
def src_data(file_paths):
    return [dict(prim_attr=i, sec_attr=-i, ext_attr=f) for i, f in enumerate(file_paths)]


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
def local_temp_dir():
    with TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def pulled_data(local_table_cls, local_temp_dir):
    local_table_cls().pull()
    return local_table_cls().fetch(as_dict=True, download_path=local_temp_dir)


@pytest.fixture
def expected_pulled_data(src_data, src_db_config, local_temp_dir):
    data = [dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name) for e in src_data]
    for entity in data:
        entity["ext_attr"] = os.path.join(local_temp_dir, os.path.basename(entity["ext_attr"]))
    return data


@pytest.mark.usefixtures("src_table_with_data")
def test_pulling(pulled_data, expected_pulled_data):
    assert pulled_data == expected_pulled_data
