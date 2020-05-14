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
    class User:
        name: str
        password: str

    @dataclass
    class Config:
        network: str
        health_check_start_period: float
        health_check_max_retries: int
        health_check_interval: float
        remove_containers: bool
        src_db_name: str
        src_db_root_user_password: str
        src_db_dj_user: User
        src_db_end_user: User
        src_db_end_user_schema: str

    src_db_end_user_name = os.environ.get("SOURCE_DATABASE_END_USER", "source_end_user")
    return Config(
        os.environ.get("DOCKER_NETWORK", "test_network"),
        float(os.environ.get("HEALTH_CHECK_START_PERIOD", 0)),
        int(os.environ.get("HEALTH_CHECK_MAX_RETRIES", 60)),
        float(os.environ.get("HEALTH_CHECK_INTERVAL", 1)),
        bool(int(os.environ.get("DOCKER_REMOVE_CONTAINERS", True))),
        os.environ.get("SOURCE_DATABASE_NAME", "test_source_database"),
        os.environ.get("SOURCE_DATABASE_ROOT_PASS", "root"),
        User(
            os.environ.get("SOURCE_DATABASE_DATAJOINT_USER", "source_datajoint_user"),
            os.environ.get("SOURCE_DATABASE_DATAJOINT_PASS", "source_datajoint_user_password"),
        ),
        User(src_db_end_user_name, os.environ.get("SOURCE_DATABASE_END_PASS", "source_end_user_password")),
        src_db_end_user_name + "_" + os.environ.get("SOURCE_DATABASE_END_USER_SCHEMA", "schema"),
    )


@pytest.fixture
def source_database(docker_client, config):
    with source_database_container(docker_client, config), source_database_root_connection(config) as connection:
        with connection.cursor() as cursor:
            for user in (config.src_db_dj_user, config.src_db_end_user):
                cursor.execute(f"CREATE USER '{user.name}'@'%' IDENTIFIED BY '{user.password}';")
            sql_statements = (
                (
                    fr"GRANT ALL PRIVILEGES ON `{config.src_db_end_user.name}\_%`.* "
                    f"TO '{config.src_db_end_user.name}'@'%';"
                ),
                (
                    f"GRANT SELECT, REFERENCES ON `{config.src_db_end_user_schema}`.* "
                    f"TO '{config.src_db_dj_user.name}'@'%';"
                ),
                (
                    f"GRANT ALL PRIVILEGES ON `{'datajoint_outbound__' + config.src_db_end_user_schema}`.* "
                    f"TO '{config.src_db_dj_user.name}'@'%';"
                ),
            )
            for sql_statement in sql_statements:
                cursor.execute(sql_statement)
        connection.commit()
        yield connection


@contextmanager
def source_database_container(client, config):
    container = None
    try:
        container = run_container(config, client)
        wait_until_healthy(config, container)
        yield container
    finally:
        if container is not None:
            container.stop()
            if config.remove_containers:
                container.remove(v=True)


def run_container(config, client):
    container = client.containers.run(
        "datajoint/mysql:5.7",
        detach=True,
        name=config.src_db_name,
        environment=dict(MYSQL_ROOT_PASSWORD=config.src_db_root_user_password),
        network=config.network,
    )
    return container


def wait_until_healthy(config, container):
    time.sleep(config.health_check_start_period)
    n_tries = 0
    while True:
        container.reload()
        if container.attrs["State"]["Health"]["Status"] == "healthy":
            break
        if n_tries >= config.health_check_max_retries:
            raise RuntimeError(f"Trying to bring up container '{config.src_db_name}' exceeded max number of retries")
        time.sleep(config.health_check_interval)
        n_tries += 1


@contextmanager
def source_database_root_connection(config):
    connection = None
    try:
        connection = pymysql.connect(
            host=config.src_db_name,
            user="root",
            password=config.src_db_root_user_password,
            cursorclass=pymysql.cursors.DictCursor,
        )
        yield connection
    finally:
        if connection is not None:
            connection.close()


def test_dummy(source_database):
    with source_database.cursor() as cursor:
        sql = "SELECT `user`, `host` FROM mysql.user;"
        cursor.execute(sql)
        result = cursor.fetchall()
        print(result)
