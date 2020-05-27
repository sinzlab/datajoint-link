import os

import pytest


@pytest.fixture
def src_part_definition(src_part_definition, src_store_config):
    src_part_definition += "ext_attr: attach@" + src_store_config.name
    return src_part_definition


@pytest.fixture
def src_part_data(src_part_data, file_paths):
    return [dict(e, ext_attr=f) for e, f in zip(src_part_data, file_paths)]


@pytest.fixture
def pulled_data(pulled_data, local_table_cls, local_dir):
    pulled_data["part"] = local_table_cls.Part().fetch(as_dict=True, download_path=local_dir)
    return pulled_data


@pytest.fixture
def expected_data(expected_data, local_dir):
    for entity in expected_data["part"]:
        entity["ext_attr"] = os.path.join(local_dir, os.path.basename(entity["ext_attr"]))
    return expected_data


@pytest.mark.usefixtures("src_table_with_data")
def test_pulling(pulled_data, expected_data):
    assert pulled_data == expected_data
