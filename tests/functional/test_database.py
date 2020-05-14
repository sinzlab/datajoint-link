from __future__ import annotations

import os
import time
from dataclasses import dataclass
from contextlib import contextmanager

import pytest
import docker
import pymysql


@pytest.fixture
def docker_client():
    return docker.client.from_env()


@pytest.fixture
def config():
    @dataclass
    class Config:
        network: str
        remove: bool
        health_check: HealthCheck
        src_db: SourceDatabase
        src_minio: MinIO

    @dataclass
    class HealthCheck:
        start_period: int
        max_retries: int
        interval: int
        timeout: int

    @dataclass
    class SourceDatabase:
        name: str
        password: str
        dj_user: User
        end_user: User
        schema: str

    @dataclass
    class User:
        name: str
        password: str

    @dataclass
    class MinIO:
        name: str
        access_key: str
        secret_key: str

    health_check = HealthCheck(
        int(os.environ.get("HEALTH_CHECK_START_PERIOD", 0)),
        int(os.environ.get("HEALTH_CHECK_MAX_RETRIES", 60)),
        int(os.environ.get("HEALTH_CHECK_INTERVAL", 1)),
        int(os.environ.get("HEALTH_CHECK_TIMEOUT", 5)),
    )
    src_db = SourceDatabase(
        os.environ.get("SOURCE_DATABASE_NAME", "test_source_database"),
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
    src_minio = MinIO(
        os.environ.get("SOURCE_MINIO_NAME", "test_source_minio"),
        os.environ.get("SOURCE_MINIO_ACCESS_KEY", "source_minio_access_key"),
        os.environ.get("SOURCE_MINIO_SECRET_KEY", "source_minio_secret_key"),
    )
    return Config(
        os.environ.get("DOCKER_NETWORK", "test_network"),
        bool(int(os.environ.get("REMOVE", True))),
        health_check,
        src_db,
        src_minio,
    )


@pytest.fixture
def source_database(docker_client, config):
    with create_container("database", docker_client, config), source_database_root_connection(config) as connection:
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
def create_container(kind, client, config):
    container = None
    try:
        container = run_container(kind, config, client)
        wait_until_healthy(config, container)
        yield container
    finally:
        if container is not None:
            container.stop()
            if config.remove:
                container.remove(v=True)


def run_container(kind, config, client):
    common = dict(detach=True, network=config.network)
    if kind == "database":
        container = client.containers.run(
            "datajoint/mysql:5.7",
            name=config.src_db.name,
            environment=dict(MYSQL_ROOT_PASSWORD=config.src_db.password),
            **common,
        )
    elif kind == "minio":
        container = client.containers.run(
            "minio/minio",
            name=config.src_minio.name,
            environment=dict(
                MINIO_ACCESS_KEY=config.src_minio.access_key, MINIO_SECRET_KEY=config.src_minio.secret_key
            ),
            command=["server", "/data"],
            healthcheck=dict(
                test=["CMD", "curl", "-f", config.src_minio.name + ":9000/minio/health/ready"],
                start_period=int(config.health_check.start_period * 1e9),  # nanoseconds
                interval=int(config.health_check.interval * 1e9),  # nanoseconds
                retries=config.health_check.max_retries,
                timeout=int(config.health_check.timeout * 1e9),  # nanoseconds
            ),
            **common,
        )
    else:
        raise ValueError
    return container


def wait_until_healthy(config, container):
    time.sleep(config.health_check.start_period)
    n_tries = 0
    while True:
        container.reload()
        if container.attrs["State"]["Health"]["Status"] == "healthy":
            break
        if n_tries >= config.health_check.max_retries:
            raise RuntimeError(f"Trying to bring up container '{config.src_db_name}' exceeded max number of retries")
        time.sleep(config.health_check.interval)
        n_tries += 1


@contextmanager
def source_database_root_connection(config):
    connection = None
    try:
        connection = pymysql.connect(
            host=config.src_db.name,
            user="root",
            password=config.src_db.password,
            cursorclass=pymysql.cursors.DictCursor,
        )
        yield connection
    finally:
        if connection is not None:
            connection.close()


@pytest.fixture
def source_minio(docker_client, config):
    with create_container("minio", docker_client, config) as container:
        yield container


def test_database(source_database):
    pass


def test_minio(source_minio):
    pass
