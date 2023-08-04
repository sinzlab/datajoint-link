import os

import datajoint as dj

from dj_link import link


def test_pulling(prepare_link, create_table, connection_config, databases, configured_environment):
    schema_names, user_specs = prepare_link()

    expected = [{"foo": 1, "bar": "a"}, {"foo": 2, "bar": "b"}]
    source_table_name = create_table(
        databases["source"], user_specs["source"], schema_names["source"], "foo: int\n---\nbar: varchar(64)", expected
    )

    with connection_config(databases["local"], user_specs["local"]), configured_environment(
        user_specs["link"], schema_names["outbound"]
    ):
        local_table_cls = link(databases["source"].container.name, schema_names["source"], schema_names["local"])(
            type(source_table_name, (dj.Manual,), {})
        )
        local_table_cls().pull()
        actual = local_table_cls().fetch(as_dict=True)
        assert actual == expected


def test_can_pull_into_different_local_tables_from_same_source(
    prepare_multiple_links, create_table, databases, connection_config, configured_environment
):
    schema_names, user_specs = prepare_multiple_links(2)

    expected = [{"foo": 1, "bar": "a"}, {"foo": 2, "bar": "b"}]
    source_table_name = create_table(
        databases["source"], user_specs["source"], schema_names["source"], "foo: int\n---\nbar: varchar(64)", expected
    )

    def fetch(local_schema_name):
        local_table_cls = link(databases["source"].container.name, schema_names["source"], local_schema_name)(
            type(source_table_name, (dj.Manual,), {})
        )
        local_table_cls().pull()
        return local_table_cls().fetch(as_dict=True)

    with connection_config(databases["local"], user_specs["local"]), configured_environment(
        user_specs["link"], schema_names["outbound"]
    ):
        assert all(fetch(name) == expected for name in schema_names["local"])


def test_pulling_with_external(
    create_random_string,
    prepare_link,
    tmpdir,
    create_table,
    connection_config,
    temp_dj_store_config,
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
        with temp_dj_store_config([source_store_spec]):
            source_table_name = create_table(
                databases["source"],
                user_specs["source"],
                schema_names["source"],
                f"foo: int\n---\nbar: attach@{source_store_spec.name}",
                expected,
            )

        with connection_config(databases["local"], user_specs["local"]), configured_environment(
            user_specs["link"], schema_names["outbound"]
        ), temp_dj_store_config([source_store_spec, local_store_spec]):
            local_table_cls = link(databases["source"].container.name, schema_names["source"], schema_names["local"])(
                type(source_table_name, (dj.Manual,), {})
            )
            local_table_cls().pull()
            actual = local_table_cls().fetch(as_dict=True, download_path=tmpdir)
            assert len(actual) == len(expected)
            assert all(entry in expected for entry in actual)
