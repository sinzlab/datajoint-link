USES_EXTERNAL = False


def test_if_source_property_returns_source_table_cls(src_table_with_data, local_table_cls):
    assert local_table_cls().source.full_table_name == src_table_with_data.full_table_name
