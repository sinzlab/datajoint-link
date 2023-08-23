import os

import datajoint as dj

from dj_link import link


def test_pulling(
    create_random_string,
    prepare_link,
    prepare_table,
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
    expected_parts = {
        "Part1": [
            {"foo": 1, "baz": 1, "egg": create_random_binary_file()},
            {"foo": 2, "baz": 13, "egg": create_random_binary_file()},
        ],
        "Part2": [{"foo": 1, "bacon": 3, "apple": 34}, {"foo": 2, "bacon": 64, "apple": 72}],
    }

    schema_names, user_specs = prepare_link()
    with temp_store(minios["source"]) as source_store_spec, temp_store(minios["local"]) as local_store_spec:
        part_table_definitions = {
            "Part1": f"-> master\nbaz: int\n---\negg: attach@{source_store_spec.name}",
            "Part2": "-> master\nbacon: int\n---\napple: int",
        }
        with temp_dj_store_config([source_store_spec]):

            def create_random_table_name():
                return create_random_string().title()

            source_table_name = create_random_table_name()
            part_table_classes = [
                create_table(name, dj.Part, definition) for name, definition in part_table_definitions.items()
            ]
            table_cls = create_table(
                source_table_name,
                dj.Manual,
                f"foo: int\n---\nbar: attach@{source_store_spec.name}",
                parts=part_table_classes,
            )
            prepare_table(
                databases["source"],
                user_specs["source"],
                schema_names["source"],
                table_cls,
                data=expected,
                parts=expected_parts,
            )

        with connection_config(databases["local"], user_specs["local"]), configured_environment(
            user_specs["link"], schema_names["outbound"]
        ), temp_dj_store_config([source_store_spec, local_store_spec]):
            local_table_cls = link(
                databases["source"].container.name,
                schema_names["source"],
                schema_names["outbound"],
                "Outbound",
                schema_names["local"],
            )(type(source_table_name, (dj.Manual,), {}))
            local_table_cls().pull()
            actual = local_table_cls().fetch(as_dict=True, download_path=tmpdir)
            assert len(actual) == len(expected)
            assert all(entry in expected for entry in actual)
            for part_table_name, part_table_expected in expected_parts.items():
                actual = getattr(local_table_cls(), part_table_name).fetch(as_dict=True, download_path=tmpdir)
                assert len(actual) == len(part_table_expected)
                assert all(entry in part_table_expected for entry in actual)
