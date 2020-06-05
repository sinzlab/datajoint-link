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

from link import main, schemas


SCOPE = os.environ.get("SCOPE", "session")


@pytest.fixture(scope=SCOPE)
def container_config_cls(health_check_config_cls):
    @dataclass
    class ContainerConfig:
        image_tag: str
        name: str
        network: str  # docker network to use for testing
        health_check: health_check_config_cls
        remove: bool  # container and associated volume will be removed if true

    return ContainerConfig


@pytest.fixture(scope=SCOPE)
def health_check_config_cls():
    @dataclass
    class HealthCheckConfig:
        start_period: int  # period after which health is first checked in seconds
        max_retries: int  # max number of retries before raising an error
        interval: int  # interval between health checks in seconds
        timeout: int  # max time a health check test has to finish

    return HealthCheckConfig


@pytest.fixture(scope=SCOPE)
def database_config_cls(container_config_cls, user_config):
    @dataclass
    class DatabaseConfig(container_config_cls):
        password: str  # MYSQL root user password
        users: Dict[str, user_config]
        schema_name: str

    return DatabaseConfig


@pytest.fixture(scope=SCOPE)
def user_config():
    @dataclass
    class UserConfig:
        name: str
        password: str

    return UserConfig


@pytest.fixture(scope=SCOPE)
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


@pytest.fixture(scope=SCOPE)
def docker_client():
    return docker.client.from_env()


@pytest.fixture(scope=SCOPE)
def network_config():
    return os.environ.get("DOCKER_NETWORK", "test_runner_network")


# noinspection PyArgumentList
@pytest.fixture(scope=SCOPE)
def health_check_config(health_check_config_cls):
    return health_check_config_cls(
        start_period=int(os.environ.get("DATABASE_HEALTH_CHECK_START_PERIOD", 0)),
        max_retries=int(os.environ.get("DATABASE_HEALTH_CHECK_MAX_RETRIES", 60)),
        interval=int(os.environ.get("DATABASE_HEALTH_CHECK_INTERVAL", 1)),
        timeout=int(os.environ.get("DATABASE_HEALTH_CHECK_TIMEOUT", 5)),
    )


@pytest.fixture(scope=SCOPE)
def remove():
    return bool(int(os.environ.get("REMOVE", True)))


# noinspection PyArgumentList
@pytest.fixture(scope=SCOPE)
def src_user_configs(user_config):
    return dict(
        admin_user=user_config(
            os.environ.get("SOURCE_DATABASE_ADMIN_USER", "source_admin_user"),
            os.environ.get("SOURCE_DATABASE_ADMIN_PASS", "source_admin_user_pass"),
        ),
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
@pytest.fixture(scope=SCOPE)
def local_user_configs(user_config):
    return dict(
        end_user=user_config(
            os.environ.get("LOCAL_DATABASE_END_USER", "local_end_user"),
            os.environ.get("LOCAL_DATABASE_END_PASS", "local_end_user_password"),
        ),
    )


@pytest.fixture(scope=SCOPE)
def src_db_config(get_db_config, network_config, health_check_config, remove, src_user_configs):
    return get_db_config("source", network_config, health_check_config, remove, src_user_configs)


@pytest.fixture(scope=SCOPE)
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


@pytest.fixture(scope=SCOPE)
def local_db_config(get_db_config, network_config, health_check_config, remove, local_user_configs):
    return get_db_config("local", network_config, health_check_config, remove, local_user_configs)


@pytest.fixture(scope=SCOPE)
def src_minio_config(get_minio_config, network_config, health_check_config):
    return get_minio_config(network_config, health_check_config, "source")


@pytest.fixture(scope=SCOPE)
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


@pytest.fixture(scope=SCOPE)
def local_minio_config(get_minio_config, network_config, health_check_config):
    return get_minio_config(network_config, health_check_config, "local")


@pytest.fixture(scope=SCOPE)
def src_db(create_container, src_db_config, docker_client):
    with create_container(docker_client, src_db_config), mysql_conn(src_db_config) as connection:
        with connection.cursor() as cursor:
            for user in src_db_config.users.values():
                cursor.execute(f"CREATE USER '{user.name}'@'%' IDENTIFIED BY '{user.password}';")
            sql_statements = (
                fr"GRANT ALL PRIVILEGES ON `{src_db_config.users['end_user'].name}\_%`.* "
                f"TO '{src_db_config.users['end_user'].name}'@'%';",
                f"GRANT SELECT, REFERENCES ON `{src_db_config.schema_name}`.* "
                f"TO '{src_db_config.users['dj_user'].name}'@'%';",
                f"GRANT ALL PRIVILEGES ON `{'datajoint_outbound__' + src_db_config.schema_name}`.* "
                f"TO '{src_db_config.users['dj_user'].name}'@'%';",
                f"GRANT ALL PRIVILEGES ON `{'datajoint_outbound__' + src_db_config.schema_name}`.* "
                f"TO '{src_db_config.users['admin_user'].name}'@'%';",
            )
            for sql_statement in sql_statements:
                cursor.execute(sql_statement)
        connection.commit()
        yield


@pytest.fixture(scope=SCOPE)
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


@pytest.fixture(scope=SCOPE)
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


@pytest.fixture(scope=SCOPE)
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


@pytest.fixture(scope=SCOPE)
def src_minio(create_container, src_minio_config, docker_client):
    with create_container(docker_client, src_minio_config):
        yield


@pytest.fixture(scope=SCOPE)
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
def src_store_name():
    return os.environ.get("SOURCE_STORE_NAME", "source_store")


@pytest.fixture
def local_store_name():
    return os.environ.get("LOCAL_STORE_NAME", "local_store")


@pytest.fixture
def src_store_config(get_store_config, src_minio_config, src_store_name):
    return get_store_config(src_minio_config, "source", src_store_name)


@pytest.fixture
def get_store_config(store_config):
    # noinspection PyArgumentList
    def _get_store_config(minio_config, kind, store_name):
        return store_config(
            name=store_name,
            protocol=os.environ.get(kind.upper() + "_STORE_PROTOCOL", "s3"),
            endpoint=minio_config.name + ":9000",
            bucket=os.environ.get(kind.upper() + "_STORE_BUCKET", kind + "-store-bucket"),
            location=os.environ.get(kind.upper() + "_STORE_LOCATION", kind + "_store_location"),
            access_key=minio_config.access_key,
            secret_key=minio_config.secret_key,
        )

    return _get_store_config


@pytest.fixture
def local_store_config(get_store_config, local_minio_config, local_store_name):
    return get_store_config(local_minio_config, "local", local_store_name)


@pytest.fixture
def get_conn():
    @contextmanager
    def _get_conn(db_config, user_type, stores=None):
        conn = None
        try:
            dj.config["database.host"] = db_config.name
            dj.config["database.user"] = db_config.users[user_type + "_user"].name
            dj.config["database.password"] = db_config.users[user_type + "_user"].password
            dj.config["stores"] = {s.pop("name"): s for s in [asdict(s) for s in stores]}
            conn = dj.conn(reset=True)
            yield conn
        finally:
            if conn is not None:
                conn.close()

    return _get_conn


@pytest.fixture
def src_conn(src_db_config, src_store_config, get_conn):
    with get_conn(src_db_config, "end", stores=[src_store_config]) as conn:
        yield conn


@pytest.fixture
def local_conn(local_db_config, local_store_config, src_store_config, get_conn):
    with get_conn(local_db_config, "end", stores=[local_store_config, src_store_config]) as conn:
        yield conn


@pytest.fixture
def cleanup_buckets(src_minio_client, local_minio_client, src_store_config, local_store_config):
    yield
    for client, config in zip((src_minio_client, local_minio_client), (src_store_config, local_store_config)):
        try:
            client.remove_bucket(config.bucket)
        except minio.error.NoSuchBucket:
            warnings.warn(f"Tried to remove bucket '{config.bucket}' but it does not exist")
        except minio.error.BucketNotEmpty:
            object_names = tuple(o.object_name for o in client.list_objects(config.bucket, recursive=True))
            for del_err in client.remove_objects(config.bucket, object_names):
                print(f"Deletion Error: {del_err}")
            client.remove_bucket(config.bucket)


@pytest.fixture
def test_session(src_db_config, local_db_config, src_conn, local_conn):
    src_schema = schemas.LazySchema(src_db_config.schema_name, connection=src_conn)
    local_schema = schemas.LazySchema(local_db_config.schema_name, connection=local_conn)
    yield dict(src=src_schema, local=local_schema)
    local_schema.drop(force=True)
    with mysql_conn(src_db_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {'datajoint_outbound__' + src_db_config.schema_name};")
        conn.commit()
    src_schema.drop(force=True)


@pytest.fixture
def src_schema(test_session):
    return test_session["src"]


@pytest.fixture
def local_schema(test_session):
    return test_session["local"]


@pytest.fixture
def src_table_definition():
    return """
    prim_attr: int
    ---
    sec_attr: int
    """


@pytest.fixture
def src_table_cls(src_table_definition):
    class Table(dj.Manual):
        definition = src_table_definition

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
    src_table.insert(src_data)
    return src_table


@pytest.fixture
def remote_schema(src_db_config):
    os.environ["REMOTE_DJ_USER"] = src_db_config.users["dj_user"].name
    os.environ["REMOTE_DJ_PASS"] = src_db_config.users["dj_user"].password
    return schemas.LazySchema(src_db_config.schema_name, host=src_db_config.name)


@pytest.fixture
def stores(request, local_store_name, src_store_name):
    if getattr(request.module, "USES_EXTERNAL"):
        return {local_store_name: src_store_name}


@pytest.fixture
def local_table_cls(local_schema, remote_schema, stores):
    @main.Link(local_schema, remote_schema, stores=stores)
    class Table:
        """Local table."""

    return Table


@pytest.fixture
def local_table_cls_with_pulled_data(local_table_cls):
    local_table_cls().pull()
    return local_table_cls
