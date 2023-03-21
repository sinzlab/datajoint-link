import pytest

USES_EXTERNAL = False


@pytest.fixture
def pulled_data(pulled_data, local_table_cls):
    pulled_data["part"] = local_table_cls.Part().fetch(as_dict=True)
    return pulled_data


@pytest.mark.usefixtures("src_db_spec", "local_db_spec", "src_table_with_data")
def test_pulling(pulled_data, expected_data):
    assert pulled_data == expected_data
