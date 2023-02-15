import os

import datajoint as dj
import pytest

from dj_link import LazySchema, Link

from ..conftest import UserConfig

USES_EXTERNAL = False


@pytest.mark.usefixtures("src_table_with_data")
def test_pulling(pulled_data, expected_data):
    assert pulled_data == expected_data


def test_pulling2(get_conn, source_db, local_db, create_user):
    local_schema_name = "awesome"
    source_schema_name = "cool"
    outbound_schema_name = "cool_outbound"

    source_user = UserConfig(
        "John", "apples", grants=[f"GRANT ALL PRIVILEGES ON `{source_schema_name}`.* TO 'John'@'%';"]
    )
    link_user = UserConfig(
        "Link",
        "bananas",
        grants=[
            f"GRANT SELECT, REFERENCES ON `{source_schema_name}`.* TO 'Link'@'%';",
            f"GRANT ALL PRIVILEGES ON `{outbound_schema_name}`.* TO 'Link'@'%';",
        ],
    )
    local_user = UserConfig("Amy", "pears", grants=[f"GRANT ALL PRIVILEGES ON `{local_schema_name}`.* TO 'Amy'@'%';"])

    create_user(source_db, source_user)
    create_user(source_db, link_user)
    create_user(local_db, local_user)

    expected = [{"foo": 1, "bar": "a"}, {"foo": 2, "bar": "b"}]
    with get_conn(source_db, source_user) as connection:

        @dj.schema(source_schema_name, connection=connection)
        class SomeTable(dj.Manual):
            definition = """
            foo: int
            ---
            bar: varchar(64)
            """

        SomeTable().insert(expected)

    os.environ["LINK_USER"] = link_user.name
    os.environ["LINK_PASS"] = link_user.password
    os.environ["LINK_OUTBOUND"] = outbound_schema_name

    dj.config["database.host"] = local_db.container.name
    dj.config["database.user"] = local_user.name
    dj.config["database.password"] = local_user.password

    dj.conn(reset=True)

    local_schema = LazySchema(local_schema_name)
    source_schema = LazySchema(source_schema_name, host=source_db.container.name)

    @Link(local_schema, source_schema)
    class SomeTable(dj.Manual):
        pass

    SomeTable().pull()
    actual = SomeTable().fetch(as_dict=True)
    assert actual == expected
