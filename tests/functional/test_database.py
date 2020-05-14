from __future__ import annotations

import os
import time
from dataclasses import dataclass
from contextlib import contextmanager

import pytest
import docker
import pymysql


@dataclass
class Container:
    image_tag: str
    name: str
    network: str
    health_check: HealthCheck
    remove: bool


@dataclass
class HealthCheck:
    start_period: int
    max_retries: int
    interval: int
    timeout: int


@dataclass
class Database(Container):
    password: str
    end_user: User
    schema: str


@dataclass
class User:
    name: str
    password: str


@dataclass
class SourceDatabase(Database):
    dj_user: User


@dataclass
class LocalDatabase(Database):
    pass


@dataclass
class MinIO(Container):
    access_key: str
    secret_key: str


@pytest.fixture
def docker_client():
    return docker.client.from_env()


@pytest.fixture
def network_config():
    return os.environ.get("DOCKER_NETWORK", "test_network")


@pytest.fixture
def health_check_config():
    return HealthCheck(
        int(os.environ.get("DATABASE_HEALTH_CHECK_START_PERIOD", 0)),
        int(os.environ.get("DATABASE_HEALTH_CHECK_MAX_RETRIES", 60)),
        int(os.environ.get("DATABASE_HEALTH_CHECK_INTERVAL", 1)),
        int(os.environ.get("DATABASE_HEALTH_CHECK_TIMEOUT", 5)),
    )


@pytest.fixture
def remove():
    return bool(int(os.environ.get("REMOVE", True)))


@pytest.fixture
def src_db_config(network_config, health_check_config, remove):
    return SourceDatabase(
        os.environ.get("SOURCE_DATABASE_TAG", "latest"),
        os.environ.get("SOURCE_DATABASE_NAME", "test_source_database"),
        network_config,
        health_check_config,
        remove,
        os.environ.get("SOURCE_DATABASE_ROOT_PASS", "root"),
        User(
            os.environ.get("SOURCE_DATABASE_END_USER", "source_end_user"),
            os.environ.get("SOURCE_DATABASE_END_PASS", "source_end_user_password"),
        ),
        os.environ.get("SOURCE_DATABASE_END_USER_SCHEMA", "source_end_user_schema"),
        User(
            os.environ.get("SOURCE_DATABASE_DATAJOINT_USER", "source_datajoint_user"),
            os.environ.get("SOURCE_DATABASE_DATAJOINT_PASS", "source_datajoint_user_password"),
        ),
    )


@pytest.fixture
def local_db_config(network_config, health_check_config, remove):
    return LocalDatabase(
        os.environ.get("LOCAL_DATABASE_TAG", "latest"),
        os.environ.get("LOCAL_DATABASE_NAME", "test_local_database"),
        network_config,
        health_check_config,
        remove,
        os.environ.get("LOCAL_DATABASE_ROOT_PASS", "root"),
        User(
            os.environ.get("LOCAL_DATABASE_END_USER", "local_end_user"),
            os.environ.get("LOCAL_DATABASE_END_PASS", "local_end_user_password"),
        ),
        os.environ.get("LOCAL_DATABASE_END_USER_SCHEMA", "local_end_user_schema"),
    )


@pytest.fixture
def src_minio_config(network_config, health_check_config):
    return MinIO(
        os.environ.get("SOURCE_MINIO_TAG", "latest"),
        os.environ.get("SOURCE_MINIO_NAME", "test_source_minio"),
        network_config,
        health_check_config,
        remove,
        os.environ.get("SOURCE_MINIO_ACCESS_KEY", "source_minio_access_key"),
        os.environ.get("SOURCE_MINIO_SECRET_KEY", "source_minio_secret_key"),
    )


@pytest.fixture
def local_minio_config(network_config, health_check_config):
    return MinIO(
        os.environ.get("LOCAL_MINIO_TAG", "latest"),
        os.environ.get("LOCAL_MINIO_NAME", "test_local_minio"),
        network_config,
        health_check_config,
        remove,
        os.environ.get("LOCAL_MINIO_ACCESS_KEY", "local_minio_access_key"),
        os.environ.get("LOCAL_MINIO_SECRET_KEY", "local_minio_secret_key"),
    )


@pytest.fixture
def src_db(src_db_config, docker_client):
    with create_container(docker_client, src_db_config), db_root_conn(src_db_config) as connection:
        with connection.cursor() as cursor:
            for user in (src_db_config.dj_user, src_db_config.end_user):
                cursor.execute(f"CREATE USER '{user.name}'@'%' IDENTIFIED BY '{user.password}';")
            sql_statements = (
                fr"GRANT ALL PRIVILEGES ON `{src_db_config.end_user.name}\_%`.* "
                f"TO '{src_db_config.end_user.name}'@'%';",
                f"GRANT SELECT, REFERENCES ON `{src_db_config.schema}`.* " f"TO '{src_db_config.dj_user.name}'@'%';",
                (
                    f"GRANT ALL PRIVILEGES ON `{'datajoint_outbound__' + src_db_config.schema}`.* "
                    f"TO '{src_db_config.dj_user.name}'@'%';"
                ),
            )
            for sql_statement in sql_statements:
                cursor.execute(sql_statement)
        connection.commit()
        yield


@pytest.fixture
def local_db(local_db_config, docker_client):
    with create_container(docker_client, local_db_config), db_root_conn(local_db_config) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                (
                    f"CREATE USER '{local_db_config.end_user.name}'@'%' "
                    f"IDENTIFIED BY '{local_db_config.end_user.name}';"
                )
            )
            cursor.execute(
                (
                    f"GRANT ALL PRIVILEGES ON `{local_db_config.end_user.name}`.* "
                    f"TO '{local_db_config.end_user.name}'"
                )
            )
        connection.commit()
        yield


@contextmanager
def create_container(client, container_config):
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


def run_container(client, container_config):
    common = dict(detach=True, network=container_config.network)
    if isinstance(container_config, Database):
        container = client.containers.run(
            "datajoint/mysql:" + container_config.image_tag,
            name=container_config.name,
            environment=dict(MYSQL_ROOT_PASSWORD=container_config.password),
            **common,
        )
    elif isinstance(container_config, MinIO):
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
def db_root_conn(db_config):
    connection = None
    try:
        connection = pymysql.connect(
            host=db_config.name, user="root", password=db_config.password, cursorclass=pymysql.cursors.DictCursor,
        )
        yield connection
    finally:
        if connection is not None:
            connection.close()


@pytest.fixture
def src_minio(src_minio_config, docker_client):
    with create_container(docker_client, src_minio_config):
        yield


@pytest.fixture
def local_minio(local_minio_config, docker_client):
    with create_container(docker_client, local_minio_config):
        yield


def test_source_database(src_db):
    pass


def test_local_database(local_db):
    pass


def test_source_minio(src_minio):
    pass


def test_local_minio(local_minio):
    pass
