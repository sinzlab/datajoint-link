import datajoint as dj
import pytest

from dj_link import LazySchema, Link

USES_EXTERNAL = False


def test_if_source_attribute_returns_source_table_cls(
    prepare_link, create_table, dj_config, databases, configured_environment
):
    schema_names, user_specs = prepare_link()

    source_table_name = create_table(databases["source"], user_specs["source"], schema_names["source"], "foo: int\n---")

    with dj_config(databases["local"], user_specs["local"]), configured_environment(
        user_specs["link"], schema_names["outbound"]
    ):
        local_schema = LazySchema(schema_names["local"])
        source_schema = LazySchema(schema_names["source"], host=databases["source"].container.name)
        link = Link(local_schema, source_schema)
        local_table_cls = link(type(source_table_name, (dj.Manual,), {}))
        assert (
            local_table_cls().source.full_table_name.replace("`", "")
            == f"{schema_names['source']}.{source_table_name}".lower()
        )


@pytest.mark.xfail
def test_if_source_attributes_of_different_local_tables_differ(
    prepare_link, databases, create_table, dj_config, configured_environment
):
    schema_names, user_specs = prepare_link()

    source_table_names = (
        create_table(databases["source"], user_specs["source"], schema_names["source"], "foo: int\n---")
        for _ in range(2)
    )

    with dj_config(databases["local"], user_specs["local"]), configured_environment(
        user_specs["link"], schema_names["outbound"]
    ):
        local_schema = LazySchema(schema_names["local"])
        source_schema = LazySchema(schema_names["source"], host=databases["source"].container.name)
        link = Link(local_schema, source_schema)
        local_table_cls1, local_table_cls2 = (link(type(name, (dj.Manual,), {})) for name in source_table_names)
        assert local_table_cls1().source.full_table_name != local_table_cls2().source.full_table_name
