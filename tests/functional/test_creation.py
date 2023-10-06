import datajoint as dj

from link import link


def test_local_table_creation_from_source_table_that_has_parent_raises_no_error(
    prepare_link, create_table, prepare_table, act_as
):
    schema_names, actors = prepare_link()
    with act_as(actors["source"]):
        source_table_parent = create_table("Foo", dj.Manual, "foo: int")
        prepare_table(schema_names["source"], source_table_parent)
        source_table_name = "Bar"
        source_table = create_table(source_table_name, dj.Manual, "-> source_table_parent")
        prepare_table(schema_names["source"], source_table, context={"source_table_parent": source_table_parent})
    with act_as(actors["local"]):
        link(
            actors["source"].credentials.host,
            schema_names["source"],
            schema_names["outbound"],
            "Outbound",
            schema_names["local"],
        )(type(source_table_name, tuple(), {}))


def test_local_table_creation_from_source_table_that_uses_current_timestamp_default_raises_no_error(
    prepare_link, create_table, prepare_table, act_as
):
    schema_names, actors = prepare_link()
    with act_as(actors["source"]):
        source_table_name = "Foo"
        source_table = create_table(source_table_name, dj.Manual, "foo = CURRENT_TIMESTAMP : timestamp")
        prepare_table(schema_names["source"], source_table)
    with act_as(actors["local"]):
        link(
            actors["source"].credentials.host,
            schema_names["source"],
            schema_names["outbound"],
            "Outbound",
            schema_names["local"],
        )(type(source_table_name, tuple(), {}))


def test_part_tables_of_computed_source_gets_created_with_correct_name(
    prepare_link, create_table, prepare_table, act_as
):
    schema_names, actors = prepare_link()
    with act_as(actors["source"]):
        source_table_name = "Foo"
        source_table = create_table(
            source_table_name, dj.Computed, "foo: int", parts=[create_table("Bar", dj.Part, "-> master")]
        )
        prepare_table(schema_names["source"], source_table)
    with act_as(actors["local"]):
        local_table = link(
            actors["source"].credentials.host,
            schema_names["source"],
            schema_names["outbound"],
            "Outbound",
            schema_names["local"],
        )(type(source_table_name, tuple(), {}))
        assert hasattr(local_table, "Bar")
