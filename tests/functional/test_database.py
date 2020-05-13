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
        health_check_start_period: float
        health_check_max_retries: int
        health_check_interval: float
        remove_containers: bool
        src_db_name: str
        src_db_root_pass: str

    return Config(
        os.environ.get("DOCKER_NETWORK", "test_network"),
        float(os.environ.get("HEALTH_CHECK_START_PERIOD", 0)),
        int(os.environ.get("HEALTH_CHECK_MAX_RETRIES", 60)),
        float(os.environ.get("HEALTH_CHECK_INTERVAL", 1)),
        bool(int(os.environ.get("DOCKER_REMOVE_CONTAINERS", True))),
        os.environ.get("SOURCE_DATABASE_NAME", "test_source_database"),
        os.environ.get("SOURCE_MYSQL_ROOT_PASSWORD", "password"),
    )


@pytest.fixture
def source_database(docker_client, config):
    with source_database_container(docker_client, config):
        with source_database_root_connection(config):
            yield


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
                container.remove()


def run_container(config, client):
    container = client.containers.run(
        "datajoint/mysql:5.7",
        detach=True,
        name=config.src_db_name,
        environment=dict(MYSQL_ROOT_PASSWORD=config.src_db_root_pass),
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
            password=config.src_db_root_pass,
            cursorclass=pymysql.cursors.DictCursor,
        )
        yield connection
    finally:
        if connection is not None:
            connection.close()


def test_dummy(source_database):
    assert True
