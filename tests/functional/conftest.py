from __future__ import annotations

import os
import pathlib
import warnings
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from random import choices
from string import ascii_lowercase
from typing import Dict

import datajoint as dj
import docker
import minio
import pymysql
import pytest
from minio.deleteobjects import DeleteObject

from dj_link import LazySchema, Link
from dj_link.docker import ContainerRunner

SCOPE = os.environ.get("SCOPE", "session")
REMOVE = True
NETWORK = os.environ["DOCKER_NETWORK"]
DATABASE_IMAGE = "datajoint/mysql:latest"
MINIO_IMAGE = "minio/minio:latest"
DATABASE_ROOT_PASSWORD = "root"


def pytest_collection_modifyitems(config, items):
    for item in items:
        module_path = pathlib.Path(item.fspath)
        if config.rootdir / pathlib.Path("tests/functional") in module_path.parents:
            item.add_marker(pytest.mark.slow)


@dataclass(frozen=True)
class ContainerConfig:
    image: str
    name: str
    health_check: HealthCheckConfig


@dataclass(frozen=True)
class HealthCheckConfig:
    start_period_seconds: int = 0  # period after which health is first checked
    max_retries: int = 60  # max number of retries before raising an error
    interval_seconds: int = 1  # interval between health checks
    timeout_seconds: int = 5  # max time a health check test has to finish


@dataclass(frozen=True)
class DatabaseConfig:
    password: str  # MYSQL root user password
    users: Dict[str, UserConfig]
    schema_name: str


@dataclass(frozen=True)
class DatabaseSpec:
    container: ContainerConfig
    config: DatabaseConfig


@dataclass(frozen=True)
class UserConfig:
    name: str
    password: str
    grants: list[str]


@dataclass(frozen=True)
class MinIOConfig:
    access_key: str
    secret_key: str


@dataclass(frozen=True)
class MinIOSpec:
    container: ContainerConfig
    config: MinIOConfig


@dataclass(frozen=True)
class StoreConfig:
    name: str
    protocol: str
    endpoint: str
    bucket: str
    location: str
    access_key: str
    secret_key: str


@pytest.fixture(scope=SCOPE)
def docker_client():
    return docker.client.from_env()


@pytest.fixture(scope=SCOPE)
def create_user_configs(outbound_schema_name):
    def _create_user_configs(schema_name):
        return dict(
            admin_user=UserConfig(
                "admin_user",
                "admin_user_password",
                grants=[
                    f"GRANT ALL PRIVILEGES ON `{outbound_schema_name}`.* TO '$name'@'%';",
                ],
            ),
            end_user=UserConfig(
                "end_user",
                "end_user_password",
                grants=[r"GRANT ALL PRIVILEGES ON `end_user\_%`.* TO '$name'@'%';"],
            ),
            dj_user=UserConfig(
                "dj_user",
                "dj_user_password",
                grants=[
                    f"GRANT SELECT, REFERENCES ON `{schema_name}`.* TO '$name'@'%';",
                    f"GRANT ALL PRIVILEGES ON `{outbound_schema_name}`.* TO '$name'@'%';",
                ],
            ),
        )

    return _create_user_configs


@pytest.fixture(scope=SCOPE)
def create_random_string():
    def _create_random_string(length=6):
        return "".join(choices(ascii_lowercase, k=length))

    return _create_random_string


@pytest.fixture(scope=SCOPE)
def get_db_spec(create_random_string, create_user_configs):
    def _get_db_spec(name):
        schema_name = "end_user_schema"
        return DatabaseSpec(
            ContainerConfig(
                image=DATABASE_IMAGE,
                name=f"{name}-{create_random_string()}",
                health_check=HealthCheckConfig(),
            ),
            DatabaseConfig(
                password=DATABASE_ROOT_PASSWORD,
                users=create_user_configs(schema_name),
                schema_name=schema_name,
            ),
        )

    return _get_db_spec


@pytest.fixture(scope=SCOPE)
def get_minio_spec(create_random_string):
    def _get_minio_spec(name):
        return MinIOSpec(
            ContainerConfig(
                image=MINIO_IMAGE,
                name=f"{name}-{create_random_string()}",
                health_check=HealthCheckConfig(),
            ),
            MinIOConfig(
                access_key="access_key",
                secret_key="secret_key",
            ),
        )

    return _get_minio_spec


@pytest.fixture(scope=SCOPE)
def outbound_schema_name():
    name = "outbound_schema"
    os.environ["LINK_OUTBOUND"] = name
    return name


@pytest.fixture(scope=SCOPE)
def get_runner_kwargs(docker_client):
    def _get_runner_kwargs(spec):
        common = dict(detach=True, network=NETWORK, name=spec.container.name, image=spec.container.image)
        if isinstance(spec, DatabaseSpec):
            processed_container_config = dict(
                environment=dict(MYSQL_ROOT_PASSWORD=spec.config.password),
                **common,
            )
        elif isinstance(spec, MinIOSpec):
            processed_container_config = dict(
                environment=dict(MINIO_ACCESS_KEY=spec.config.access_key, MINIO_SECRET_KEY=spec.config.secret_key),
                command=["server", "/data"],
                healthcheck=dict(
                    test=["CMD", "curl", "-f", "127.0.0.1:9000/minio/health/ready"],
                    start_period=int(spec.container.health_check.start_period_seconds * 1e9),  # nanoseconds
                    interval=int(spec.container.health_check.interval_seconds * 1e9),  # nanoseconds
                    retries=spec.container.health_check.max_retries,
                    timeout=int(spec.container.health_check.timeout_seconds * 1e9),  # nanoseconds
                ),
                **common,
            )
        else:
            raise ValueError
        return {
            "docker_client": docker_client,
            "container_config": processed_container_config,
            "health_check_config": {
                "max_retries": spec.container.health_check.max_retries,
                "interval": spec.container.health_check.interval_seconds,
            },
            "remove": REMOVE,
        }

    return _get_runner_kwargs


@pytest.fixture(scope=SCOPE)
def create_user_config(create_random_string):
    def _create_user_config(grants):
        name = create_random_string()
        return UserConfig(
            name=name, password=create_random_string(), grants=[grant.replace("$name", name) for grant in grants]
        )

    return _create_user_config


@pytest.fixture(scope=SCOPE)
def create_user(create_user_config):
    def _create_user(db_spec, grants):
        config = create_user_config(grants)
        with mysql_conn(db_spec) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE USER '{config.name}'@'%' IDENTIFIED BY '{config.password}';")
                for grant in config.grants:
                    cursor.execute(grant)
            connection.commit()
        return config

    return _create_user


@pytest.fixture(scope=SCOPE)
def create_db(get_db_spec, get_runner_kwargs):
    def _create_db(name):
        db_spec = get_db_spec(name)
        with ContainerRunner(**get_runner_kwargs(db_spec)):
            yield db_spec

    return _create_db


@pytest.fixture(scope=SCOPE)
def source_db(create_db):
    yield from create_db("source")


@pytest.fixture(scope=SCOPE)
def local_db(create_db):
    yield from create_db("local")


@pytest.fixture(scope=SCOPE)
def src_db_spec(source_db, create_user):
    for name, user in source_db.config.users.items():
        source_db.config.users[name] = create_user(source_db, user.grants)
    return source_db


@pytest.fixture(scope=SCOPE)
def local_db_spec(local_db, create_user):
    for name, user in local_db.config.users.items():
        local_db.config.users[name] = create_user(local_db, user.grants)
    return local_db


@contextmanager
def mysql_conn(db_spec):
    connection = None
    try:
        connection = pymysql.connect(
            host=db_spec.container.name,
            user="root",
            password=db_spec.config.password,
            cursorclass=pymysql.cursors.DictCursor,
        )
        yield connection
    finally:
        if connection is not None:
            connection.close()


@pytest.fixture(scope=SCOPE)
def create_minio(get_minio_spec, get_runner_kwargs):
    def _create_minio(name):
        spec = get_minio_spec(name)
        with ContainerRunner(**get_runner_kwargs(spec)):
            yield spec

    return _create_minio


@pytest.fixture(scope=SCOPE)
def src_minio_spec(create_minio):
    yield from create_minio("source")


@pytest.fixture(scope=SCOPE)
def local_minio_spec(create_minio):
    yield from create_minio("local")


@pytest.fixture(scope=SCOPE)
def get_minio_client():
    def _get_minio_client(spec):
        return minio.Minio(
            spec.container.name + ":9000",
            access_key=spec.config.access_key,
            secret_key=spec.config.secret_key,
            secure=False,
        )

    return _get_minio_client


@pytest.fixture
def src_store_name():
    return os.environ.get("SOURCE_STORE_NAME", "source_store")


@pytest.fixture
def local_store_name():
    return os.environ.get("LOCAL_STORE_NAME", "local_store")


@pytest.fixture
def src_store_config(get_store_config, src_minio_spec, src_store_name):
    return get_store_config(src_minio_spec, src_store_name)


@pytest.fixture
def get_store_config(create_random_string):
    def _get_store_config(minio_spec, store_name, protocol="s3", port=9000):
        return StoreConfig(
            name=store_name,
            protocol=protocol,
            endpoint=f"{minio_spec.container.name}:{port}",
            bucket=create_random_string(),
            location=create_random_string(),
            access_key=minio_spec.config.access_key,
            secret_key=minio_spec.config.secret_key,
        )

    return _get_store_config


@pytest.fixture
def local_store_config(get_store_config, local_minio_spec, local_store_name):
    return get_store_config(local_minio_spec, local_store_name)


@pytest.fixture
def get_conn():
    @contextmanager
    def _get_conn(db_spec, user, stores=None):
        if stores is None:
            stores = dict()
        try:
            with dj.config(
                database__host=db_spec.container.name,
                database__user=user.name,
                database__password=user.password,
                stores={s.pop("name"): s for s in [asdict(s) for s in stores]},
                safemode=False,
            ):
                yield dj.conn(reset=True)
        finally:
            try:
                delattr(dj.conn, "connection")
            except AttributeError:
                pass

    return _get_conn


@pytest.fixture
def src_conn(src_db_spec, src_store_config, get_conn):
    with get_conn(src_db_spec, src_db_spec.config.users["end_user"], stores=[src_store_config]) as conn:
        yield conn


@pytest.fixture
def local_conn(local_db_spec, local_store_config, src_store_config, get_conn):
    with get_conn(
        local_db_spec, local_db_spec.config.users["end_user"], stores=[local_store_config, src_store_config]
    ) as conn:
        yield conn


@pytest.fixture
def create_and_cleanup_buckets(
    get_minio_client, src_minio_spec, local_minio_spec, src_store_config, local_store_config
):
    local_minio_client = get_minio_client(local_minio_spec)
    src_minio_client = get_minio_client(src_minio_spec)
    for client, config in zip((src_minio_client, local_minio_client), (src_store_config, local_store_config)):
        client.make_bucket(config.bucket)
    yield
    for client, config in zip((src_minio_client, local_minio_client), (src_store_config, local_store_config)):
        try:
            client.remove_bucket(config.bucket)
        except minio.error.S3Error as error:
            if error.code == "NoSuchBucket":
                warnings.warn(f"Tried to remove bucket '{config.bucket}' but it does not exist")
            if error.code == "BucketNotEmpty":
                delete_object_list = [
                    DeleteObject(o.object_name) for o in client.list_objects(config.bucket, recursive=True)
                ]
                for del_err in client.remove_objects(config.bucket, delete_object_list):
                    print(f"Deletion Error: {del_err}")
                client.remove_bucket(config.bucket)


@pytest.fixture
def test_session(src_db_spec, local_db_spec, src_conn, local_conn, outbound_schema_name):
    src_schema = LazySchema(src_db_spec.config.schema_name, connection=src_conn)
    local_schema = LazySchema(local_db_spec.config.schema_name, connection=local_conn)
    yield dict(src=src_schema, local=local_schema)
    local_schema.drop(force=True)
    with mysql_conn(src_db_spec) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {outbound_schema_name};")
        conn.commit()
    src_schema.drop(force=True)


@pytest.fixture
def src_schema(test_session):
    return test_session["src"]


@pytest.fixture
def local_schema(test_session):
    return test_session["local"]


@pytest.fixture
def src_table_name():
    return "Table"


@pytest.fixture
def src_table_definition():
    return """
    prim_attr: int
    ---
    sec_attr: int
    """


@pytest.fixture
def src_table_cls(src_table_name, src_table_definition):
    class Table(dj.Manual):
        definition = src_table_definition

    Table.__name__ = src_table_name
    return Table


@pytest.fixture
def n_entities():
    return int(os.environ.get("N_ENTITIES", 10))


@pytest.fixture
def src_data(n_entities):
    return [dict(prim_attr=i, sec_attr=-i) for i in range(n_entities)]


@pytest.fixture
def src_table_with_data(src_schema, src_table_cls, src_data):
    src_table = src_schema(src_table_cls)
    src_table().insert(src_data)
    return src_table


@pytest.fixture
def remote_schema(src_db_spec):
    os.environ["LINK_USER"] = src_db_spec.config.users["dj_user"].name
    os.environ["LINK_PASS"] = src_db_spec.config.users["dj_user"].password
    return LazySchema(src_db_spec.config.schema_name, host=src_db_spec.container.name)


@pytest.fixture
def stores(request, local_store_name, src_store_name):
    if getattr(request.module, "USES_EXTERNAL"):
        return {local_store_name: src_store_name}


@pytest.fixture
def local_table_cls(local_schema, remote_schema, stores):
    @Link(local_schema, remote_schema, stores=stores)
    class Table:
        """Local table."""

    return Table


@pytest.fixture
def local_table_cls_with_pulled_data(src_table_with_data, local_table_cls):
    local_table_cls().pull()
    return local_table_cls


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
