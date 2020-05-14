from __future__ import annotations

import os
import time
from dataclasses import dataclass
from contextlib import contextmanager

import pytest
import docker
import pymysql


@dataclass
class Config:
    src_db: SourceDatabase
    src_minio: MinIO


@dataclass
class HealthCheck:
    start_period: int
    max_retries: int
    interval: int
    timeout: int


@dataclass
class Container:
    image_tag: str
    name: str
    network: str
    health_check: HealthCheck
    remove: bool


@dataclass
class SourceDatabase(Container):
    password: str
    dj_user: User
    end_user: User
    schema: str


@dataclass
class User:
    name: str
    password: str


@dataclass
class MinIO(Container):
    access_key: str
    secret_key: str


@pytest.fixture
def config():
    network = os.environ.get("DOCKER_NETWORK", "test_network")
    remove = bool(int(os.environ.get("REMOVE", True)))
    database_health_check = HealthCheck(
        int(os.environ.get("DATABASE_HEALTH_CHECK_START_PERIOD", 0)),
        int(os.environ.get("DATABASE_HEALTH_CHECK_MAX_RETRIES", 60)),
        int(os.environ.get("DATABASE_HEALTH_CHECK_INTERVAL", 1)),
        int(os.environ.get("DATABASE_HEALTH_CHECK_TIMEOUT", 5)),
    )
    src_db = SourceDatabase(
        os.environ.get("SOURCE_DATABASE_TAG", "latest"),
        os.environ.get("SOURCE_DATABASE_NAME", "test_source_database"),
        network,
        database_health_check,
        remove,
        os.environ.get("SOURCE_DATABASE_ROOT_PASS", "root"),
        User(
            os.environ.get("SOURCE_DATABASE_DATAJOINT_USER", "source_datajoint_user"),
            os.environ.get("SOURCE_DATABASE_DATAJOINT_PASS", "source_datajoint_user_password"),
        ),
        User(
            os.environ.get("SOURCE_DATABASE_END_USER", "source_end_user"),
            os.environ.get("SOURCE_DATABASE_END_PASS", "source_end_user_password"),
        ),
        os.environ.get("SOURCE_DATABASE_END_USER_SCHEMA", "source_end_user_schema"),
    )
    minio_health_check = HealthCheck(
        int(os.environ.get("MINIO_HEALTH_CHECK_START_PERIOD", 0)),
        int(os.environ.get("MINIO_HEALTH_CHECK_MAX_RETRIES", 60)),
        int(os.environ.get("MINIO_HEALTH_CHECK_INTERVAL", 1)),
        int(os.environ.get("MINIO_HEALTH_CHECK_TIMEOUT", 5)),
    )
    src_minio = MinIO(
        os.environ.get("SOURCE_MINIO_TAG", "latest"),
        os.environ.get("SOURCE_MINIO_NAME", "test_source_minio"),
        network,
        minio_health_check,
        remove,
        os.environ.get("SOURCE_MINIO_ACCESS_KEY", "source_minio_access_key"),
        os.environ.get("SOURCE_MINIO_SECRET_KEY", "source_minio_secret_key"),
    )
    return Config(src_db, src_minio)


@pytest.fixture
def docker_client():
    return docker.client.from_env()


@pytest.fixture
def source_database(config, docker_client):
    with create_container(docker_client, config.src_db), database_root_connection(config.src_db) as connection:
        with connection.cursor() as cursor:
            for user in (config.src_db.dj_user, config.src_db.end_user):
                cursor.execute(f"CREATE USER '{user.name}'@'%' IDENTIFIED BY '{user.password}';")
            sql_statements = (
                (
                    fr"GRANT ALL PRIVILEGES ON `{config.src_db.end_user.name}\_%`.* "
                    f"TO '{config.src_db.end_user.name}'@'%';"
                ),
                f"GRANT SELECT, REFERENCES ON `{config.src_db.schema}`.* " f"TO '{config.src_db.dj_user.name}'@'%';",
                (
                    f"GRANT ALL PRIVILEGES ON `{'datajoint_outbound__' + config.src_db.schema}`.* "
                    f"TO '{config.src_db.dj_user.name}'@'%';"
                ),
            )
            for sql_statement in sql_statements:
                cursor.execute(sql_statement)
        connection.commit()
        yield connection


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
    if isinstance(container_config, SourceDatabase):
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
def database_root_connection(db_config):
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
def source_minio(config, docker_client):
    with create_container(docker_client, config.src_minio) as container:
        yield container


def test_database(source_database):
    pass


def test_minio(source_minio):
    pass
