import os

import datajoint as dj

from dj_link import LazySchema, Link

USES_EXTERNAL = False


def test_pulling(prepare_link, create_table, connection_config, databases, configured_environment):
    schema_names, user_specs = prepare_link()

    expected = [{"foo": 1, "bar": "a"}, {"foo": 2, "bar": "b"}]
    source_table_name = create_table(
        databases["source"], user_specs["source"], schema_names["source"], "foo: int\n---\nbar: varchar(64)", expected
    )

    with connection_config(databases["local"], user_specs["local"]), configured_environment(
        user_specs["link"], schema_names["outbound"]
    ):
        local_schema = LazySchema(schema_names["local"])
        source_schema = LazySchema(schema_names["source"], host=databases["source"].container.name)
        local_table_cls = Link(local_schema, source_schema)(type(source_table_name, (dj.Manual,), {}))
        local_table_cls().pull()
        actual = local_table_cls().fetch(as_dict=True)
        assert actual == expected


def test_pulling_with_external(
    create_random_string,
    prepare_link,
    tmpdir,
    create_table,
    connection_config,
    stores_config,
    temp_store,
    databases,
    minios,
    configured_environment,
):
    def create_random_binary_file(n_bytes=1024):
        filepath = tmpdir / create_random_string()
        with open(filepath, "wb") as file:
            file.write(os.urandom(n_bytes))
        return filepath

    expected = [{"foo": 1, "bar": create_random_binary_file()}, {"foo": 2, "bar": create_random_binary_file()}]

    schema_names, user_specs = prepare_link()
    with temp_store(minios["source"]) as source_store_spec, temp_store(minios["local"]) as local_store_spec:
        with stores_config([source_store_spec]):
            source_table_name = create_table(
                databases["source"],
                user_specs["source"],
                schema_names["source"],
                f"foo: int\n---\nbar: attach@{source_store_spec.name}",
                expected,
            )

        with connection_config(databases["local"], user_specs["local"]), configured_environment(
            user_specs["link"], schema_names["outbound"]
        ), stores_config([source_store_spec, local_store_spec]):
            local_schema = LazySchema(schema_names["local"])
            source_schema = LazySchema(schema_names["source"], host=databases["source"].container.name)
            local_table_cls = Link(local_schema, source_schema)(type(source_table_name, (dj.Manual,), {}))
            local_table_cls().pull()
            actual = local_table_cls().fetch(as_dict=True, download_path=tmpdir)
            assert len(actual) == len(expected) and all(entry in expected for entry in actual)
