import os
from tempfile import TemporaryDirectory

import pytest


@pytest.fixture
def src_table_definition(src_table_definition, src_store_config):
    src_table_definition += "ext_attr: attach@" + src_store_config.name
    return src_table_definition


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
def src_data(src_data, file_paths):
    return [dict(e, ext_attr=f) for e, f in zip(src_data, file_paths)]


@pytest.fixture
def local_temp_dir():
    with TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def pulled_data(local_table_cls, local_temp_dir):
    local_table_cls().pull()
    return local_table_cls().fetch(as_dict=True, download_path=local_temp_dir)


@pytest.fixture
def expected_data(src_data, src_db_config, local_temp_dir):
    data = [dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name) for e in src_data]
    for entity in data:
        entity["ext_attr"] = os.path.join(local_temp_dir, os.path.basename(entity["ext_attr"]))
    return data


@pytest.mark.usefixtures("src_table_with_data")
def test_pulling(pulled_data, expected_data):
    assert pulled_data == expected_data
