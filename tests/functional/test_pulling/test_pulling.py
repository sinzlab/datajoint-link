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


def test_pulling(create_random_string, get_conn, source_db, local_db, create_user):
    local_schema_name, source_schema_name, outbound_schema_name = (create_random_string() for _ in range(3))

    source_user = create_user(source_db, grants=[f"GRANT ALL PRIVILEGES ON `{source_schema_name}`.* TO '$name'@'%';"])
    link_user = create_user(
        source_db,
        grants=[
            f"GRANT SELECT, REFERENCES ON `{source_schema_name}`.* TO '$name'@'%';",
            f"GRANT ALL PRIVILEGES ON `{outbound_schema_name}`.* TO '$name'@'%';",
        ],
    )
    local_user = create_user(local_db, grants=[f"GRANT ALL PRIVILEGES ON `{local_schema_name}`.* TO '$name'@'%';"])

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

    with get_conn(local_db, local_user), temp_env_var("LINK_USER", link_user.name), temp_env_var(
        "LINK_PASS", link_user.password
    ), temp_env_var("LINK_OUTBOUND", outbound_schema_name):
        local_schema = LazySchema(local_schema_name)
        source_schema = LazySchema(source_schema_name, host=source_db.container.name)

        @Link(local_schema, source_schema)
        class SomeTable(dj.Manual):
            pass

        SomeTable().pull()
        actual = SomeTable().fetch(as_dict=True)
        assert actual == expected
