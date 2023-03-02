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


@pytest.mark.usefixtures("create_and_cleanup_buckets", "src_table_with_data")
def test_pulling(
    local_table_cls,
    local_dir,
    dj_config,
    stores_config,
    local_db_spec,
    src_store_config,
    local_store_config,
    expected_data,
):
    with dj_config(local_db_spec, local_db_spec.config.users["end_user"]), stores_config(
        [src_store_config, local_store_config]
    ):
        local_table_cls().pull()
    pulled_data = local_table_cls().fetch(as_dict=True, download_path=local_dir)
    for entity in expected_data:
        entity["ext_attr"] = os.path.join(local_dir, os.path.basename(entity["ext_attr"]))
    assert pulled_data == expected_data
