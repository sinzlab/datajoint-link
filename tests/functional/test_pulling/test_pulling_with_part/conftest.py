import datajoint as dj
import pytest


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
def pulled_data(pulled_data):
    return dict(master=pulled_data)


@pytest.fixture
def expected_data(expected_data, src_part_data):
    return dict(master=expected_data, part=src_part_data)
