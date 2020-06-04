import pytest


@pytest.mark.usefixtures("src_db", "local_db", "src_table_with_data")
def test_pulling(pulled_data, expected_data):
    assert pulled_data == expected_data
