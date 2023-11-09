import datajoint as dj

from link import link


def test_deleting(prepare_link, prepare_table, act_as, create_table):
    data = [{"foo": 1}, {"foo": 2}, {"foo": 3}]
    expected = [{"foo": 2}, {"foo": 3}]
    schema_names, actors = prepare_link()

    with act_as(actors["source"]):
        table_cls = create_table("Foo", dj.Manual, "foo: int")
        prepare_table(schema_names["source"], table_cls, data=data)

    with act_as(actors["local"]):
        local_table_cls = link(
            actors["source"].credentials.host,
            schema_names["source"],
            schema_names["outbound"],
            "Outbound",
            schema_names["local"],
        )(type("Foo", tuple(), {}))
        local_table_cls().source.pull()

    with act_as(actors["admin"]):
        table_classes = {}
        dj.schema(schema_names["outbound"]).spawn_missing_classes(context=table_classes)
        outbound_table_cls = table_classes["Outbound"]
        row = (outbound_table_cls & {"foo": 1}).fetch1()
        (outbound_table_cls() & row).delete_quick()
        row["is_flagged"] = "TRUE"
        outbound_table_cls().insert1(row)

    with act_as(actors["local"]):
        (local_table_cls() & local_table_cls().source.flagged).delete(display_progress=True)
        assert local_table_cls().fetch(as_dict=True) == expected

    assert (outbound_table_cls() & {"foo": 1}).fetch1("is_deprecated") == "TRUE"
