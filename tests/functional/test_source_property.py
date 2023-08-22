import datajoint as dj

from dj_link import link


def test_if_source_attribute_returns_source_table_cls(
    prepare_link, connection_config, databases, configured_environment, create_table, prepare_table
):
    schema_names, user_specs = prepare_link()

    source_table_name = "Foo"
    table_cls = create_table(source_table_name, dj.Manual, "foo: int\n---")
    prepare_table(databases["source"], user_specs["source"], schema_names["source"], table_cls)

    with connection_config(databases["local"], user_specs["local"]), configured_environment(
        user_specs["link"], schema_names["outbound"]
    ):
        linker = link(databases["source"].container.name, schema_names["source"], schema_names["local"])
        local_table_cls = linker(type(source_table_name, (dj.Manual,), {}))
        assert (
            local_table_cls().source.full_table_name.replace("`", "")
            == f"{schema_names['source']}.{source_table_name}".lower()
        )


def test_if_source_attributes_of_different_local_tables_differ(
    prepare_link, databases, connection_config, configured_environment, create_table, prepare_table
):
    schema_names, user_specs = prepare_link()

    source_table_names = ("Foo", "Bar")
    for name in source_table_names:
        table_cls = create_table(name, dj.Manual, "foo: int\n---")
        prepare_table(databases["source"], user_specs["source"], schema_names["source"], table_cls)

    with connection_config(databases["local"], user_specs["local"]), configured_environment(
        user_specs["link"], schema_names["outbound"]
    ):
        linker = link(databases["source"].container.name, schema_names["source"], schema_names["local"])
        local_table_cls1, local_table_cls2 = (linker(type(name, (dj.Manual,), {})) for name in source_table_names)
        assert local_table_cls1().source.full_table_name != local_table_cls2().source.full_table_name
