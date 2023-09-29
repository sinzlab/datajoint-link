import datajoint as dj

from link import link


def test_local_table_creation_from_source_table_that_has_parent_raises_no_error(
    prepare_link, create_table, prepare_table, databases, configured_environment, connection_config
):
    schema_names, user_specs = prepare_link()
    source_table_parent = create_table("Foo", dj.Manual, "foo: int")
    prepare_table(databases["source"], user_specs["source"], schema_names["source"], source_table_parent)
    source_table_name = "Bar"
    source_table = create_table(source_table_name, dj.Manual, "-> source_table_parent")
    prepare_table(
        databases["source"],
        user_specs["source"],
        schema_names["source"],
        source_table,
        context={"source_table_parent": source_table_parent},
    )
    with connection_config(databases["local"], user_specs["local"]), configured_environment(
        user_specs["link"], schema_names["outbound"]
    ):
        link(
            databases["source"].container.name,
            schema_names["source"],
            schema_names["outbound"],
            "Outbound",
            schema_names["local"],
        )(type(source_table_name, (dj.Manual,), {}))
