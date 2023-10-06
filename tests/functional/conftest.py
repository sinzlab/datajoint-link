from __future__ import annotations

import os
import pathlib
from concurrent import futures
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from random import choices
from string import ascii_lowercase

import datajoint as dj
import docker
import minio
import pymysql
import pytest
from minio.deleteobjects import DeleteObject

from tests.docker.runner import ContainerRunner

SCOPE = os.environ.get("SCOPE", "session")
REMOVE = True
DATABASE_IMAGE = "cblessing24/mariadb:11.1"
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
    ulimits: frozenset[Ulimit]
    network: str


@dataclass(frozen=True)
class Ulimit:
    name: str
    soft: int
    hard: int


@dataclass(frozen=True)
class HealthCheckConfig:
    start_period_seconds: int = 0  # period after which health is first checked
    max_retries: int = 120  # max number of retries before raising an error
    interval_seconds: int = 1  # interval between health checks
    timeout_seconds: int = 5  # max time a health check test has to finish


@dataclass(frozen=True)
class DatabaseConfig:
    password: str  # MYSQL root user password
    schema_name: str


@dataclass(frozen=True)
class DatabaseSpec:
    container: ContainerConfig
    config: DatabaseConfig


@dataclass(frozen=True)
class UserConfig:
    host: str
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
def create_random_string():
    def _create_random_string(length=6):
        return "".join(choices(ascii_lowercase, k=length))

    return _create_random_string


@pytest.fixture(scope=SCOPE)
def network():
    return os.environ["DOCKER_NETWORK"]


@pytest.fixture(scope=SCOPE)
def get_db_spec(create_random_string, network):
    def _get_db_spec(name):
        schema_name = "end_user_schema"
        return DatabaseSpec(
            ContainerConfig(
                image=DATABASE_IMAGE,
                name=f"{name}-{create_random_string()}",
                health_check=HealthCheckConfig(),
                ulimits=frozenset([Ulimit("nofile", 262144, 262144)]),
                network=network,
            ),
            DatabaseConfig(
                password=DATABASE_ROOT_PASSWORD,
                schema_name=schema_name,
            ),
        )

    return _get_db_spec


@pytest.fixture(scope=SCOPE)
def get_minio_spec(create_random_string, network):
    def _get_minio_spec(name):
        return MinIOSpec(
            ContainerConfig(
                image=MINIO_IMAGE,
                name=f"{name}-{create_random_string()}",
                health_check=HealthCheckConfig(),
                ulimits=frozenset(),
                network=network,
            ),
            MinIOConfig(
                access_key=create_random_string(),
                secret_key=create_random_string(8),
            ),
        )

    return _get_minio_spec


def get_runner_kwargs(docker_client, spec):
    common = dict(
        detach=True,
        network=spec.container.network,
        name=spec.container.name,
        image=spec.container.image,
        ulimits=[docker.types.Ulimit(**asdict(ulimit)) for ulimit in spec.container.ulimits],
    )
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


@pytest.fixture(scope=SCOPE)
def create_user(create_random_string):
    def _create_user(db_spec, grants):
        user_name = create_random_string()
        config = UserConfig(
            host=db_spec.container.name,
            name=user_name,
            password=create_random_string(),
            grants=[grant.replace("$name", user_name) for grant in grants],
        )
        with mysql_conn(db_spec) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE USER '{config.name}'@'%' IDENTIFIED BY '{config.password}';")
                for grant in config.grants:
                    cursor.execute(grant)
            connection.commit()
        return config

    return _create_user


@contextmanager
def create_containers(docker_client, specs):
    def execute_runner_method(method):
        futures_to_names = create_futures_to_names(method)
        for future in futures.as_completed(futures_to_names):
            handle_result(future, f"Container {futures_to_names[future]} failed to {method}")

    def create_futures_to_names(method):
        return {executor.submit(getattr(runner, method)): name for name, runner in names_to_runners.items()}

    def handle_result(future, message):
        try:
            future.result()
        except Exception as exc:
            raise RuntimeError(message) from exc

    names_to_runners = {
        spec.container.name: ContainerRunner(**get_runner_kwargs(docker_client, spec)) for spec in specs
    }
    with futures.ThreadPoolExecutor() as executor:
        execute_runner_method("start")
        yield names_to_runners
        execute_runner_method("stop")


@pytest.fixture(scope=SCOPE)
def databases(get_db_spec, docker_client):
    kinds_to_specs = {kind: get_db_spec(kind) for kind in ["source", "local"]}
    with create_containers(docker_client, kinds_to_specs.values()):
        yield kinds_to_specs


@pytest.fixture(scope=SCOPE)
def minios(get_minio_spec, docker_client):
    kinds_to_specs = {kind: get_minio_spec(kind) for kind in ["source", "local"]}
    with create_containers(docker_client, kinds_to_specs.values()):
        yield kinds_to_specs


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
def get_minio_client():
    def _get_minio_client(spec):
        return minio.Minio(
            spec.container.name + ":9000",
            access_key=spec.config.access_key,
            secret_key=spec.config.secret_key,
            secure=False,
        )

    return _get_minio_client


@pytest.fixture()
def get_store_spec(create_random_string):
    def _get_store_spec(minio_spec, protocol="s3", port=9000):
        return StoreConfig(
            name=create_random_string(),
            protocol=protocol,
            endpoint=f"{minio_spec.container.name}:{port}",
            bucket=create_random_string(),
            location=create_random_string(),
            access_key=minio_spec.config.access_key,
            secret_key=minio_spec.config.secret_key,
        )

    return _get_store_spec


@pytest.fixture()
def dj_connection():
    @contextmanager
    def _dj_connection():
        connection = dj.Connection(
            dj.config["database.host"], dj.config["database.user"], dj.config["database.password"]
        )
        try:
            yield connection
        finally:
            connection.close()

    return _dj_connection


@pytest.fixture()
def connection_config():
    @contextmanager
    def _connection_config(actor):
        try:
            with dj.config(
                database__host=actor.credentials.host,
                database__user=actor.credentials.name,
                database__password=actor.credentials.password,
                safemode=False,
            ):
                dj.conn(reset=True)
                yield
        finally:
            try:
                delattr(dj.conn, "connection")
            except AttributeError:
                pass

    return _connection_config


@pytest.fixture()
def temp_dj_store_config():
    @contextmanager
    def _temp_dj_store_config(stores):
        with dj.config(stores={store.pop("name"): store for store in [asdict(store) for store in stores]}):
            yield

    return _temp_dj_store_config


@pytest.fixture()
def temp_store(get_minio_client, get_store_spec):
    @contextmanager
    def _temp_store(minio_spec):
        def create_bucket():
            store_spec = get_store_spec(minio_spec)
            get_minio_client(minio_spec).make_bucket(store_spec.bucket)
            return store_spec

        def delete_objects():
            to_be_deleted = [
                DeleteObject(object.object_name)
                for object in get_minio_client(minio_spec).list_objects(store_spec.bucket, recursive=True)
            ]
            if deletion_errors := list(get_minio_client(minio_spec).remove_objects(store_spec.bucket, to_be_deleted)):
                raise RuntimeError(f"Error(s) during object deletion: {deletion_errors}")

        def cleanup_bucket():
            try:
                get_minio_client(minio_spec).remove_bucket(store_spec.bucket)
            except minio.error.S3Error as error:
                if error.code == "BucketNotEmpty":
                    delete_objects()
                    get_minio_client(minio_spec).remove_bucket(store_spec.bucket)
                else:
                    raise error

        store_spec = create_bucket()
        try:
            yield store_spec
        finally:
            cleanup_bucket()

    return _temp_store


@pytest.fixture()
def temp_stores(temp_store, src_minio_spec, local_minio_spec):
    with temp_store(src_minio_spec) as src_store_spec, temp_store(local_minio_spec) as local_store_spec:
        yield {"source": src_store_spec, "local": local_store_spec}


@pytest.fixture()
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


@pytest.fixture()
def act_as(connection_config, temp_env_vars):
    @contextmanager
    def _act_as(actor):
        with connection_config(actor), temp_env_vars(**actor.environment):
            yield

    return _act_as


@pytest.fixture()
def configured_environment(temp_env_vars):
    @contextmanager
    def _configured_environment(actor, schema_name):
        with temp_env_vars(
            LINK_USER=actor.credentials.name, LINK_PASS=actor.credentials.password, LINK_OUTBOUND=schema_name
        ):
            yield

    return _configured_environment


@dataclass(frozen=True)
class Actor:
    credentials: UserConfig
    environment: dict[str, str] = field(default_factory=dict)


@pytest.fixture()
def prepare_multiple_links(create_random_string, create_user, databases):
    def _prepare_multiple_links(n_local_schemas):
        def create_schema_names():
            names = {kind: create_random_string() for kind in ("source", "outbound")}
            names["local"] = [create_random_string() for _ in range(n_local_schemas)]
            return names

        schema_names = create_schema_names()
        link_actor = Actor(
            create_user(
                databases["source"],
                grants=[
                    f"GRANT SELECT, REFERENCES ON `{schema_names['source']}`.* TO '$name'@'%';",
                    f"GRANT ALL PRIVILEGES ON `{schema_names['outbound']}`.* TO '$name'@'%';",
                ],
            )
        )
        actors = {
            "admin": Actor(create_user(databases["source"], grants=["GRANT ALL PRIVILEGES ON *.* TO '$name'@'%';"])),
            "source": Actor(
                create_user(
                    databases["source"],
                    grants=[f"GRANT ALL PRIVILEGES ON `{schema_names['source']}`.* TO '$name'@'%';"],
                )
            ),
            "local": Actor(
                create_user(
                    databases["local"],
                    grants=[f"GRANT ALL PRIVILEGES ON `{name}`.* TO '$name'@'%';" for name in schema_names["local"]],
                ),
                {
                    "LINK_USER": link_actor.credentials.name,
                    "LINK_PASS": link_actor.credentials.password,
                    "LINK_OUTBOUND": schema_names["outbound"],
                },
            ),
            "link": link_actor,
        }
        return schema_names, actors

    return _prepare_multiple_links


@pytest.fixture()
def prepare_link(prepare_multiple_links):
    def _prepare_link():
        schema_names, user_specs = prepare_multiple_links(1)
        schema_names["local"] = schema_names["local"][0]
        return schema_names, user_specs

    return _prepare_link


@pytest.fixture()
def create_table():
    def _create_table(name, tier, definition, *, parts=None):
        if tier is dj.Part:
            assert parts is None
        if parts is None:
            parts = []
        return type(name, (tier,), {"definition": definition, **{part.__name__: part for part in parts}})

    return _create_table


@pytest.fixture()
def prepare_table(dj_connection):
    def _prepare_table(schema, table_cls, *, data=None, parts=None, context=None):
        if data is None:
            data = []
        if parts is None:
            parts = {}
        with dj_connection() as connection:
            dj.schema(schema, connection=connection, context=context)(table_cls)
            table_cls().insert(data, allow_direct_insert=True)
            for name, part_data in parts.items():
                getattr(table_cls, name).insert(part_data)

    return _prepare_table
