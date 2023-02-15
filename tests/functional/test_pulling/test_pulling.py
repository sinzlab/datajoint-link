import os

import datajoint as dj
import pytest

from dj_link import LazySchema, Link

USES_EXTERNAL = False


@pytest.mark.usefixtures("src_table_with_data")
def test_pulling(pulled_data, expected_data):
    assert pulled_data == expected_data


def test_pulling2(get_conn, src_db_spec, local_db_spec):
    expected = [{"foo": 1, "bar": "a"}, {"foo": 2, "bar": "b"}]
    with get_conn(src_db_spec, src_db_spec.config.users["end_user"]) as connection:
        source_schema = dj.schema("end_user_schema", connection=connection)

        @source_schema
        class SomeTable(dj.Manual):
            definition = """
            foo: int
            ---
            bar: varchar(64)
            """

        SomeTable().insert(expected)

    os.environ["LINK_USER"] = src_db_spec.config.users["dj_user"].name
    os.environ["LINK_PASS"] = src_db_spec.config.users["dj_user"].password
    os.environ["LINK_OUTBOUND"] = "outbound_schema"

    dj.config["database.host"] = local_db_spec.container.name
    dj.config["database.user"] = local_db_spec.config.users["end_user"].name
    dj.config["database.password"] = local_db_spec.config.users["end_user"].password

    local_schema = LazySchema("end_user_local_schema")
    source_schema = LazySchema("end_user_schema", host=src_db_spec.container.name)

    @Link(local_schema, source_schema)
    class SomeTable(dj.Manual):
        pass

    SomeTable().pull()
    actual = SomeTable().fetch(as_dict=True)
    assert actual == expected
