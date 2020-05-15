from __future__ import annotations

import os
import time
from dataclasses import dataclass, asdict
from contextlib import contextmanager

import pytest
import docker
import pymysql
import datajoint as dj

from link import main


@dataclass
class Container:
    image_tag: str
    name: str
    network: str  # docker network to use for testing
    health_check: HealthCheck
    remove: bool  # container and associated volume will be removed if true


@dataclass
class HealthCheck:
    start_period: int  # period after which health is first checked in seconds
    max_retries: int  # max number of retries before raising an error
    interval: int  # interval between health checks in seconds
    timeout: int  # max time a health check test has to finish


@dataclass
class Database(Container):
    password: str  # MYSQL root user password
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


@dataclass
class Store:
    name: str
    protocol: str
    endpoint: str
    bucket: str
    location: str
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
    return minio_config(network_config, health_check_config, "source")


def minio_config(network_config, health_check_config, kind):
    return MinIO(
        image_tag=os.environ.get(kind.upper() + "_MINIO_TAG", "latest"),
        name=os.environ.get(kind.upper() + "_MINIO_NAME", "test_" + kind + "_minio"),
        network=network_config,
        health_check=health_check_config,
        remove=remove,
        access_key=os.environ.get(kind.upper() + "_MINIO_ACCESS_KEY", kind + "_minio_access_key"),
        secret_key=os.environ.get(kind.upper() + "_MINIO_SECRET_KEY", kind + "_minio_secret_key"),
    )


@pytest.fixture
def local_minio_config(network_config, health_check_config):
    return minio_config(network_config, health_check_config, "local")


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
                    f"IDENTIFIED BY '{local_db_config.end_user.password}';"
                )
            )
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON `{local_db_config.schema}`.* " f"TO '{local_db_config.end_user.name}'"
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


@pytest.fixture
def src_dj_store(src_minio_config):
    return dj_store(src_minio_config, "source")


def dj_store(minio_config, kind):
    return Store(
        name=os.environ.get(kind.upper() + "_STORE_NAME", kind + "_store"),
        protocol=os.environ.get(kind.upper() + "_STORE_PROTOCOL", "s3"),
        endpoint=minio_config.name + ":9000",
        bucket=os.environ.get(kind.upper() + "_STORE_BUCKET", kind + "_store_bucket"),
        location=os.environ.get(kind.upper() + "_STORE_LOCATION", kind + "_store_location"),
        access_key=minio_config.access_key,
        secret_key=minio_config.secret_key,
    )


@pytest.fixture
def local_dj_store(local_minio_config):
    return dj_store(local_minio_config, "local")


@pytest.fixture
def src_schema(src_db_config, src_dj_store):
    return schema(src_db_config, [src_dj_store])


def schema(db_config, stores):
    @contextmanager
    def _schema():
        try:
            dj.config["database.host"] = db_config.name
            dj.config["database.user"] = db_config.end_user.name
            dj.config["database.password"] = db_config.end_user.password
            dj.config["stores"] = {s.pop("name"): s for s in [asdict(s) for s in stores]}
            dj.conn(reset=True)
            yield db_config.schema
        finally:
            dj.conn().close()

    return _schema


@pytest.fixture
def local_schema(local_db_config, local_dj_store, src_dj_store):
    return schema(local_db_config, [local_dj_store, src_dj_store])


def test_pull(src_schema, local_schema, src_db_config, src_db, local_db):
    src_data = [dict(primary_key=i, secondary_key=-i) for i in range(10)]
    expected_local_data = [
        dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema) for e in src_data
    ]
    with src_schema() as src_schema_name:
        src_schema = dj.schema(src_schema_name)

        @src_schema
        class Experiment(dj.Manual):
            definition = """
            primary_key: int
            ---
            secondary_key: int
            """

        Experiment().insert(src_data)

    with local_schema() as local_schema_name:
        os.environ["REMOTE_DJ_USER"] = src_db_config.dj_user.name
        os.environ["REMOTE_DJ_PASS"] = src_db_config.dj_user.password

        local_schema = main.SchemaProxy(local_schema_name)
        remote_schema = main.SchemaProxy(src_db_config.schema, host=src_db_config.name)

        @main.Link(local_schema, remote_schema)
        class Experiment:
            pass

        Experiment().pull()
        local_data = Experiment().fetch(as_dict=True)
    assert local_data == expected_local_data
