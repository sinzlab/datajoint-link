import os
from contextlib import contextmanager

import datajoint as dj
import pytest

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


@pytest.fixture
def create_table(get_conn, create_random_string):
    def _create_table(db_spec, user_spec, schema_name, definition, data=None):
        if data is None:
            data = []
        with get_conn(db_spec, user_spec) as connection:
            table_name = create_random_string().title()
            table_cls = type(table_name, (dj.Manual,), {"definition": definition})
            schema = dj.schema(schema_name, connection=connection)
            schema(table_cls)
            table_cls().insert(data)
        return table_name

    return _create_table


def test_pulling(create_random_string, create_table, get_conn, source_db, local_db, create_user):
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
    source_table_name = create_table(
        source_db, source_user, source_schema_name, "foo: int\n---\nbar: varchar(64)", expected
    )

    with get_conn(local_db, local_user), temp_env_var("LINK_USER", link_user.name), temp_env_var(
        "LINK_PASS", link_user.password
    ), temp_env_var("LINK_OUTBOUND", outbound_schema_name):
        local_schema = LazySchema(local_schema_name)
        source_schema = LazySchema(source_schema_name, host=source_db.container.name)
        local_table_cls = Link(local_schema, source_schema)(type(source_table_name, (dj.Manual,), {}))
        local_table_cls().pull()
        actual = local_table_cls().fetch(as_dict=True)
        assert actual == expected


@pytest.mark.xfail
def test_if_source_attributes_of_different_local_tables_differ(
    create_random_string, create_user, source_db, local_db, create_table, get_conn
):
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

    source_table_names = (create_table(source_db, source_user, source_schema_name, "foo: int\n---") for _ in range(2))

    with get_conn(local_db, local_user), temp_env_var("LINK_USER", link_user.name), temp_env_var(
        "LINK_PASS", link_user.password
    ), temp_env_var("LINK_OUTBOUND", outbound_schema_name):
        local_schema = LazySchema(local_schema_name)
        source_schema = LazySchema(source_schema_name, host=source_db.container.name)
        link = Link(local_schema, source_schema)
        local_table_cls1, local_table_cls2 = (link(type(name, (dj.Manual,), {})) for name in source_table_names)
        assert local_table_cls1().source.full_table_name != local_table_cls2().source.full_table_name
