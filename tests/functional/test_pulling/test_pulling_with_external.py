import os

import pytest

USES_EXTERNAL = True


@pytest.fixture
def src_table_definition(src_table_definition, src_store_config):
    src_table_definition += "ext_attr: attach@" + src_store_config.name
    return src_table_definition


@pytest.fixture
def src_data(src_data, file_paths):
    return [dict(e, ext_attr=f) for e, f in zip(src_data, file_paths)]


@pytest.fixture
def pulled_data(local_table_cls, local_dir, dj_config, local_db_spec, src_store_config, local_store_config):
    with dj_config(
        local_db_spec, local_db_spec.config.users["end_user"], stores=[src_store_config, local_store_config]
    ):
        local_table_cls().pull()
    return local_table_cls().fetch(as_dict=True, download_path=local_dir)


@pytest.fixture
def expected_data(expected_data, local_dir):
    for entity in expected_data:
        entity["ext_attr"] = os.path.join(local_dir, os.path.basename(entity["ext_attr"]))
    return expected_data


@pytest.mark.usefixtures("create_and_cleanup_buckets", "src_table_with_data")
def test_pulling(pulled_data, expected_data):
    assert pulled_data == expected_data
