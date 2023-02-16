import os
from contextlib import contextmanager

import datajoint as dj

from dj_link import LazySchema, Link

USES_EXTERNAL = False


@contextmanager
def temp_env_var(name, value):
    original_value = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if original_value is not None:
            os.environ[name] = original_value
        else:
            del os.environ[name]


def test_pulling(get_conn, source_db, local_db, create_user):
    local_schema_name = "awesome"
    source_schema_name = "cool"
    outbound_schema_name = "cool_outbound"

    source_user = create_user(
        source_db, "John", grants=[f"GRANT ALL PRIVILEGES ON `{source_schema_name}`.* TO 'John'@'%';"]
    )
    link_user = create_user(
        source_db,
        "Link",
        grants=[
            f"GRANT SELECT, REFERENCES ON `{source_schema_name}`.* TO 'Link'@'%';",
            f"GRANT ALL PRIVILEGES ON `{outbound_schema_name}`.* TO 'Link'@'%';",
        ],
    )
    local_user = create_user(local_db, "Amy", grants=[f"GRANT ALL PRIVILEGES ON `{local_schema_name}`.* TO 'Amy'@'%';"])

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

    dj.config["database.host"] = local_db.container.name
    dj.config["database.user"] = local_user.name
    dj.config["database.password"] = local_user.password

    with temp_env_var("LINK_USER", link_user.name), temp_env_var("LINK_PASS", link_user.password), temp_env_var(
        "LINK_OUTBOUND", outbound_schema_name
    ):
        local_schema = LazySchema(local_schema_name)
        source_schema = LazySchema(source_schema_name, host=source_db.container.name)

        @Link(local_schema, source_schema)
        class SomeTable(dj.Manual):
            pass

        SomeTable().pull()
        actual = SomeTable().fetch(as_dict=True)
        assert actual == expected
