import datajoint as dj
import pytest

from dj_link import LazySchema, Link

USES_EXTERNAL = False


def test_if_source_property_returns_source_table_cls(src_table_with_data, local_table_cls):
    assert local_table_cls().source.full_table_name == src_table_with_data.full_table_name


@pytest.mark.xfail
def test_if_source_attributes_of_different_local_tables_differ(
    prepare_link, source_db, local_db, create_table, get_conn, configured_environment
):
    schema_names, user_specs = prepare_link()

    source_table_names = (
        create_table(source_db, user_specs["source"], schema_names["source"], "foo: int\n---") for _ in range(2)
    )

    with get_conn(local_db, user_specs["local"]), configured_environment(user_specs["link"], schema_names["outbound"]):
        local_schema = LazySchema(schema_names["local"])
        source_schema = LazySchema(schema_names["source"], host=source_db.container.name)
        link = Link(local_schema, source_schema)
        local_table_cls1, local_table_cls2 = (link(type(name, (dj.Manual,), {})) for name in source_table_names)
        assert local_table_cls1().source.full_table_name != local_table_cls2().source.full_table_name
