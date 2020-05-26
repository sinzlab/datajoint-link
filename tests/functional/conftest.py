from __future__ import annotations

import os
import time
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from typing import Dict
import warnings

import pytest
import docker
import pymysql
import minio
import datajoint as dj

from link import main


@pytest.fixture(scope="module")
def container_config_cls(health_check_config_cls):
    @dataclass
    class ContainerConfig:
        image_tag: str
        name: str
        network: str  # docker network to use for testing
        health_check: health_check_config_cls
        remove: bool  # container and associated volume will be removed if true

    return ContainerConfig


@pytest.fixture(scope="module")
def health_check_config_cls():
    @dataclass
    class HealthCheckConfig:
        start_period: int  # period after which health is first checked in seconds
        max_retries: int  # max number of retries before raising an error
        interval: int  # interval between health checks in seconds
        timeout: int  # max time a health check test has to finish

    return HealthCheckConfig


@pytest.fixture(scope="module")
def database_config_cls(container_config_cls, user_config):
    @dataclass
    class DatabaseConfig(container_config_cls):
        password: str  # MYSQL root user password
        users: Dict[str, user_config]
        schema_name: str

    return DatabaseConfig


@pytest.fixture(scope="module")
def user_config():
    @dataclass
    class UserConfig:
        name: str
        password: str

    return UserConfig


@pytest.fixture(scope="module")
def minio_config_cls(container_config_cls):
    @dataclass
    class MinIOConfig(container_config_cls):
        access_key: str
        secret_key: str

    return MinIOConfig


@pytest.fixture()
def store_config():
    @dataclass
    class StoreConfig:
        name: str
        protocol: str
        endpoint: str
        bucket: str
        location: str
        access_key: str
        secret_key: str

    return StoreConfig


@pytest.fixture(scope="module")
def docker_client():
    return docker.client.from_env()


@pytest.fixture(scope="module")
def network_config():
    return os.environ.get("DOCKER_NETWORK", "test_runner_network")


# noinspection PyArgumentList
@pytest.fixture(scope="module")
def health_check_config(health_check_config_cls):
    return health_check_config_cls(
        start_period=int(os.environ.get("DATABASE_HEALTH_CHECK_START_PERIOD", 0)),
        max_retries=int(os.environ.get("DATABASE_HEALTH_CHECK_MAX_RETRIES", 60)),
        interval=int(os.environ.get("DATABASE_HEALTH_CHECK_INTERVAL", 1)),
        timeout=int(os.environ.get("DATABASE_HEALTH_CHECK_TIMEOUT", 5)),
    )


@pytest.fixture(scope="module")
def remove():
    return bool(int(os.environ.get("REMOVE", True)))


# noinspection PyArgumentList
@pytest.fixture(scope="module")
def src_user_configs(user_config):
    return dict(
        end_user=user_config(
            os.environ.get("SOURCE_DATABASE_END_USER", "source_end_user"),
            os.environ.get("SOURCE_DATABASE_END_PASS", "source_end_user_password"),
        ),
        dj_user=user_config(
            os.environ.get("SOURCE_DATABASE_DATAJOINT_USER", "source_datajoint_user"),
            os.environ.get("SOURCE_DATABASE_DATAJOINT_PASS", "source_datajoint_user_password"),
        ),
    )


# noinspection PyArgumentList
@pytest.fixture(scope="module")
def local_user_configs(user_config):
    return dict(
        end_user=user_config(
            os.environ.get("LOCAL_DATABASE_END_USER", "local_end_user"),
            os.environ.get("LOCAL_DATABASE_END_PASS", "local_end_user_password"),
        ),
    )


@pytest.fixture(scope="module")
def src_db_config(get_db_config, network_config, health_check_config, remove, src_user_configs):
    return get_db_config("source", network_config, health_check_config, remove, src_user_configs)


@pytest.fixture(scope="module")
def get_db_config(database_config_cls):
    def _get_db_config(kind, network_config, health_check_config, remove, user_configs):
        return database_config_cls(
            image_tag=os.environ.get(kind.upper() + "_DATABASE_TAG", "latest"),
            name=os.environ.get(kind.upper() + "_DATABASE_NAME", "test_" + kind + "_database"),
            network=network_config,
            health_check=health_check_config,
            remove=remove,
            password=os.environ.get(kind.upper() + "_DATABASE_ROOT_PASS", "root"),
            users=user_configs,
            schema_name=os.environ.get(kind.upper() + "_DATABASE_END_USER_SCHEMA", kind + "_end_user_schema"),
        )

    return _get_db_config


@pytest.fixture(scope="module")
def local_db_config(get_db_config, network_config, health_check_config, remove, local_user_configs):
    return get_db_config("local", network_config, health_check_config, remove, local_user_configs)


@pytest.fixture(scope="module")
def src_minio_config(get_minio_config, network_config, health_check_config):
    return get_minio_config(network_config, health_check_config, "source")


@pytest.fixture(scope="module")
def get_minio_config(minio_config_cls):
    def _get_minio_config(network_config, health_check_config, kind):
        return minio_config_cls(
            image_tag=os.environ.get(kind.upper() + "_MINIO_TAG", "latest"),
            name=os.environ.get(kind.upper() + "_MINIO_NAME", "test_" + kind + "_minio"),
            network=network_config,
            health_check=health_check_config,
            remove=remove,
            access_key=os.environ.get(kind.upper() + "_MINIO_ACCESS_KEY", kind + "_minio_access_key"),
            secret_key=os.environ.get(kind.upper() + "_MINIO_SECRET_KEY", kind + "_minio_secret_key"),
        )

    return _get_minio_config


@pytest.fixture(scope="module")
def local_minio_config(get_minio_config, network_config, health_check_config):
    return get_minio_config(network_config, health_check_config, "local")


@pytest.fixture(scope="module", autouse=True)
def src_db(create_container, src_db_config, docker_client):
    with create_container(docker_client, src_db_config), mysql_conn(src_db_config) as connection:
        with connection.cursor() as cursor:
            for user in src_db_config.users.values():
                cursor.execute(f"CREATE USER '{user.name}'@'%' IDENTIFIED BY '{user.password}';")
            sql_statements = (
                fr"GRANT ALL PRIVILEGES ON `{src_db_config.users['end_user'].name}\_%`.* "
                f"TO '{src_db_config.users['end_user'].name}'@'%';",
                (
                    f"GRANT SELECT, REFERENCES ON `{src_db_config.schema_name}`.* "
                    f"TO '{src_db_config.users['dj_user'].name}'@'%';"
                ),
                (
                    f"GRANT ALL PRIVILEGES ON `{'datajoint_outbound__' + src_db_config.schema_name}`.* "
                    f"TO '{src_db_config.users['dj_user'].name}'@'%';"
                ),
            )
            for sql_statement in sql_statements:
                cursor.execute(sql_statement)
        connection.commit()
        yield


@pytest.fixture(scope="module", autouse=True)
def local_db(create_container, local_db_config, docker_client):
    with create_container(docker_client, local_db_config), mysql_conn(local_db_config) as connection:
        with connection.cursor() as cursor:
            for user in local_db_config.users.values():
                cursor.execute(f"CREATE USER '{user.name}'@'%' " f"IDENTIFIED BY '{user.password}';")
            cursor.execute(
                (
                    f"GRANT ALL PRIVILEGES ON `{local_db_config.schema_name}`.* "
                    f"TO '{local_db_config.users['end_user'].name}'"
                )
            )
        connection.commit()
        yield


@pytest.fixture(scope="module")
def create_container(run_container):
    @contextmanager
    def _create_container(client, container_config):
        container = None
        try:
            container = run_container(client, container_config)
            wait_until_healthy(container_config, container)
            yield container
        finally:
            if container is not None:
                container.stop()
                if container_config.remove:
                    container.remove(v=True)

    return _create_container


@pytest.fixture(scope="module")
def run_container(database_config_cls, minio_config_cls):
    def _run_container(client, container_config):
        common = dict(detach=True, network=container_config.network)
        if isinstance(container_config, database_config_cls):
            container = client.containers.run(
                "datajoint/mysql:" + container_config.image_tag,
                name=container_config.name,
                environment=dict(MYSQL_ROOT_PASSWORD=container_config.password),
                **common,
            )
        elif isinstance(container_config, minio_config_cls):
            container = client.containers.run(
                "minio/minio:" + container_config.image_tag,
                name=container_config.name,
                environment=dict(
                    MINIO_ACCESS_KEY=container_config.access_key, MINIO_SECRET_KEY=container_config.secret_key,
                ),
                command=["server", "/data"],
                healthcheck=dict(
                    test=["CMD", "curl", "-f", container_config.name + ":9000/minio/health/ready"],
                    start_period=int(container_config.health_check.start_period * 1e9),  # nanoseconds
                    interval=int(container_config.health_check.interval * 1e9),  # nanoseconds
                    retries=container_config.health_check.max_retries,
                    timeout=int(container_config.health_check.timeout * 1e9),  # nanoseconds
                ),
                **common,
            )
        else:
            raise ValueError
        return container

    return _run_container


def wait_until_healthy(container_config, container):
    time.sleep(container_config.health_check.start_period)
    n_tries = 0
    while True:
        container.reload()
        if container.attrs["State"]["Health"]["Status"] == "healthy":
            break
        if n_tries >= container_config.health_check.max_retries:
            raise RuntimeError(f"Trying to bring up container '{container_config.name}' exceeded max number of retries")
        time.sleep(container_config.health_check.interval)
        n_tries += 1


@contextmanager
def mysql_conn(db_config):
    connection = None
    try:
        connection = pymysql.connect(
            host=db_config.name, user="root", password=db_config.password, cursorclass=pymysql.cursors.DictCursor,
        )
        yield connection
    finally:
        if connection is not None:
            connection.close()


@pytest.fixture(scope="module", autouse=True)
def src_minio(create_container, src_minio_config, docker_client):
    with create_container(docker_client, src_minio_config):
        yield


@pytest.fixture(scope="module", autouse=True)
def local_minio(create_container, local_minio_config, docker_client):
    with create_container(docker_client, local_minio_config):
        yield


@pytest.fixture
def src_minio_client(src_minio_config):
    return get_minio_client(src_minio_config)


def get_minio_client(minio_config):
    return minio.Minio(
        minio_config.name + ":9000",
        access_key=minio_config.access_key,
        secret_key=minio_config.secret_key,
        secure=False,
    )


@pytest.fixture
def local_minio_client(local_minio_config):
    return get_minio_client(local_minio_config)


@pytest.fixture
def src_store_config(get_store_config, src_minio_config):
    return get_store_config(src_minio_config, "source")


@pytest.fixture
def get_store_config(store_config):
    # noinspection PyArgumentList
    def _get_store_config(minio_config, kind):
        return store_config(
            name=os.environ.get(kind.upper() + "_STORE_NAME", kind + "_store"),
            protocol=os.environ.get(kind.upper() + "_STORE_PROTOCOL", "s3"),
            endpoint=minio_config.name + ":9000",
            bucket=os.environ.get(kind.upper() + "_STORE_BUCKET", kind + "-store-bucket"),
            location=os.environ.get(kind.upper() + "_STORE_LOCATION", kind + "_store_location"),
            access_key=minio_config.access_key,
            secret_key=minio_config.secret_key,
        )

    return _get_store_config


@pytest.fixture
def local_store_config(get_store_config, local_minio_config):
    return get_store_config(local_minio_config, "local")


@pytest.fixture
def src_conn(src_db_config, src_store_config):
    with get_conn(src_db_config, [src_store_config]) as conn:
        yield conn


@contextmanager
def get_conn(db_config, stores):
    conn = None
    try:
        dj.config["database.host"] = db_config.name
        dj.config["database.user"] = db_config.users["end_user"].name
        dj.config["database.password"] = db_config.users["end_user"].password
        dj.config["stores"] = {s.pop("name"): s for s in [asdict(s) for s in stores]}
        conn = dj.conn(reset=True)
        yield conn
    finally:
        if conn is not None:
            conn.close()


@pytest.fixture
def local_conn(local_db_config, local_store_config, src_store_config):
    with get_conn(local_db_config, [local_store_config, src_store_config]) as conn:
        yield conn


@pytest.fixture
def test_session(
    src_db_config,
    local_db_config,
    src_conn,
    local_conn,
    src_minio_client,
    local_minio_client,
    src_store_config,
    local_store_config,
):
    def remove_bucket(store_config):
        try:
            local_minio_client.remove_bucket(store_config.bucket)
        except minio.error.NoSuchBucket:
            warnings.warn(f"Tried to remove bucket '{store_config.bucket}' but it does not exist")

    src_schema = main.SchemaProxy(src_db_config.schema_name, connection=src_conn)
    local_schema = main.SchemaProxy(local_db_config.schema_name, connection=local_conn)
    yield dict(src=src_schema, local=local_schema)
    local_schema.drop(force=True)
    remove_bucket(local_store_config)
    with mysql_conn(src_db_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {'datajoint_outbound__' + src_db_config.schema_name};")
        conn.commit()
    src_schema.drop(force=True)
    remove_bucket(src_store_config)


@pytest.fixture
def src_schema(test_session):
    return test_session["src"]


@pytest.fixture
def local_schema(test_session):
    return test_session["local"]


@pytest.fixture
def src_data():
    return [dict(prim_attr=i, sec_attr=-i) for i in range(10)]
