import os
from contextlib import contextmanager

import datajoint as dj
import pytest

from dj_link import LazySchema, Link

USES_EXTERNAL = False


@pytest.fixture
def temp_env_vars():
    @contextmanager
    def _temp_env_vars(**vars):
        original_values = {name: os.environ.get(name) for name in vars}
        os.environ.update(vars)
        try:
            yield
        finally:
            for name, value in original_values.items():
                if value is None:
                    del os.environ[name]
                else:
                    os.environ[name] = value

    return _temp_env_vars


@pytest.fixture
def configured_environment(temp_env_vars):
    @contextmanager
    def _configured_environment(user_spec, schema_name):
        with temp_env_vars(LINK_USER=user_spec.name, LINK_PASS=user_spec.password, LINK_OUTBOUND=schema_name):
            yield

    return _configured_environment


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


@pytest.fixture
def prepare_link(create_random_string, create_user, source_db, local_db):
    def _prepare_link():
        schema_names = {kind: create_random_string() for kind in ("source", "local", "outbound")}
        user_specs = {
            "source": create_user(
                source_db, grants=[f"GRANT ALL PRIVILEGES ON `{schema_names['source']}`.* TO '$name'@'%';"]
            ),
            "local": create_user(
                local_db, grants=[f"GRANT ALL PRIVILEGES ON `{schema_names['local']}`.* TO '$name'@'%';"]
            ),
            "link": create_user(
                source_db,
                grants=[
                    f"GRANT SELECT, REFERENCES ON `{schema_names['source']}`.* TO '$name'@'%';",
                    f"GRANT ALL PRIVILEGES ON `{schema_names['outbound']}`.* TO '$name'@'%';",
                ],
            ),
        }
        return schema_names, user_specs

    return _prepare_link


def test_pulling(prepare_link, create_table, get_conn, source_db, local_db, configured_environment):
    schema_names, user_specs = prepare_link()

    expected = [{"foo": 1, "bar": "a"}, {"foo": 2, "bar": "b"}]
    source_table_name = create_table(
        source_db, user_specs["source"], schema_names["source"], "foo: int\n---\nbar: varchar(64)", expected
    )

    with get_conn(local_db, user_specs["local"]), configured_environment(user_specs["link"], schema_names["outbound"]):
        local_schema = LazySchema(schema_names["local"])
        source_schema = LazySchema(schema_names["source"], host=source_db.container.name)
        local_table_cls = Link(local_schema, source_schema)(type(source_table_name, (dj.Manual,), {}))
        local_table_cls().pull()
        actual = local_table_cls().fetch(as_dict=True)
        assert actual == expected


@pytest.mark.xfail
def test_if_source_attributes_of_different_local_tables_differ(
    prepare_link, source_db, local_db, create_table, get_conn, configured_environment
):
    schema_names, user_specs = prepare_link()

    source_table_names = (
        create_table(source_db, user_specs["source"], schema_names["source"], "foo: int\n---") for _ in range(2)
    )

    with get_conn(local_db, user_specs["local"]), configured_environment(user_specs["link"], schema_names["outbound"]):
        local_schema = LazySchema(schema_names["local"])
        source_schema = LazySchema(schema_names["source"], host=source_db.container.name)
        link = Link(local_schema, source_schema)
        local_table_cls1, local_table_cls2 = (link(type(name, (dj.Manual,), {})) for name in source_table_names)
        assert local_table_cls1().source.full_table_name != local_table_cls2().source.full_table_name
