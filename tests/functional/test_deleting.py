import datajoint as dj

from dj_link import link


def test_deleting(
    prepare_link, prepare_table, dj_connection, databases, connection_config, configured_environment, create_table
):
    schema_names, user_specs = prepare_link()

    table_cls = create_table("Foo", dj.Manual, "foo: int")
    prepare_table(
        databases["source"],
        user_specs["source"],
        schema_names["source"],
        table_cls,
        data=[{"foo": 1}, {"foo": 2}, {"foo": 3}],
    )

    with connection_config(databases["local"], user_specs["local"]), configured_environment(
        user_specs["link"], schema_names["outbound"]
    ):
        local_table_cls = link(
            databases["source"].container.name,
            schema_names["source"],
            schema_names["outbound"],
            "Outbound",
            schema_names["local"],
        )(type("Foo", tuple(), {}))
        local_table_cls().source.pull()

    with dj_connection(databases["source"], user_specs["admin"]) as connection:
        table_classes = {}
        dj.schema(schema_names["outbound"], connection=connection).spawn_missing_classes(context=table_classes)
        outbound_table_cls = table_classes["Outbound"]
        row = (outbound_table_cls & {"foo": 1}).fetch1()
        (outbound_table_cls() & row).delete_quick()
        row["is_flagged"] = "TRUE"
        outbound_table_cls().insert1(row)

    with connection_config(databases["local"], user_specs["local"]), configured_environment(
        user_specs["link"], schema_names["outbound"]
    ):
        (local_table_cls() & {"foo": 1}).delete()
        assert {"foo": 1} not in local_table_cls().fetch(as_dict=True)

    assert (outbound_table_cls() & {"foo": 1}).fetch1("is_deprecated") == "TRUE"
