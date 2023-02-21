import datajoint as dj

from dj_link import LazySchema, Link

USES_EXTERNAL = False


def test_pulling(prepare_link, create_table, get_conn, source_db, local_db, configured_environment):
    schema_names, user_specs = prepare_link()

    expected = [{"foo": 1, "bar": "a"}, {"foo": 2, "bar": "b"}]
    source_table_name = create_table(
        source_db, user_specs["source"], schema_names["source"], "foo: int\n---\nbar: varchar(64)", expected
    )

    with get_conn(local_db, user_specs["local"]), configured_environment(user_specs["link"], schema_names["outbound"]):
        local_schema = LazySchema(schema_names["local"])
        source_schema = LazySchema(schema_names["source"], host=source_db.container.name)
        local_table_cls = Link(local_schema, source_schema)(type(source_table_name, (dj.Manual,), {}))
        local_table_cls().pull()
        actual = local_table_cls().fetch(as_dict=True)
        assert actual == expected
