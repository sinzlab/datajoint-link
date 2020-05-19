from __future__ import annotations

import os
import time
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from typing import Dict
from tempfile import TemporaryDirectory

import pytest
import docker
import pymysql
import datajoint as dj

from link import main


@dataclass
class ContainerConfig:
    image_tag: str
    name: str
    network: str  # docker network to use for testing
    health_check: HealthCheckConfig
    remove: bool  # container and associated volume will be removed if true


@dataclass
class HealthCheckConfig:
    start_period: int  # period after which health is first checked in seconds
    max_retries: int  # max number of retries before raising an error
    interval: int  # interval between health checks in seconds
    timeout: int  # max time a health check test has to finish


@dataclass
class DatabaseConfig(ContainerConfig):
    password: str  # MYSQL root user password
    users: Dict[str, UserConfig]
    schema_name: str


@dataclass
class UserConfig:
    name: str
    password: str


@dataclass
class MinIOConfig(ContainerConfig):
    access_key: str
    secret_key: str


@dataclass
class StoreConfig:
    name: str
    protocol: str
    endpoint: str
    bucket: str
    location: str
    access_key: str
    secret_key: str


@pytest.fixture(scope="module")
def docker_client():
    return docker.client.from_env()


@pytest.fixture(scope="module")
def network_config():
    return os.environ.get("DOCKER_NETWORK", "test_runner_network")


@pytest.fixture(scope="module")
def health_check_config():
    return HealthCheckConfig(
        start_period=int(os.environ.get("DATABASE_HEALTH_CHECK_START_PERIOD", 0)),
        max_retries=int(os.environ.get("DATABASE_HEALTH_CHECK_MAX_RETRIES", 60)),
        interval=int(os.environ.get("DATABASE_HEALTH_CHECK_INTERVAL", 1)),
        timeout=int(os.environ.get("DATABASE_HEALTH_CHECK_TIMEOUT", 5)),
    )


@pytest.fixture(scope="module")
def remove():
    return bool(int(os.environ.get("REMOVE", True)))


@pytest.fixture(scope="module")
def src_user_configs():
    return dict(
        end_user=UserConfig(
            os.environ.get("SOURCE_DATABASE_END_USER", "source_end_user"),
            os.environ.get("SOURCE_DATABASE_END_PASS", "source_end_user_password"),
        ),
        dj_user=UserConfig(
            os.environ.get("SOURCE_DATABASE_DATAJOINT_USER", "source_datajoint_user"),
            os.environ.get("SOURCE_DATABASE_DATAJOINT_PASS", "source_datajoint_user_password"),
        ),
    )


@pytest.fixture(scope="module")
def local_user_configs():
    return dict(
        end_user=UserConfig(
            os.environ.get("LOCAL_DATABASE_END_USER", "local_end_user"),
            os.environ.get("LOCAL_DATABASE_END_PASS", "local_end_user_password"),
        ),
    )


@pytest.fixture(scope="module")
def src_db_config(network_config, health_check_config, remove, src_user_configs):
    return get_db_config("source", network_config, health_check_config, remove, src_user_configs)


def get_db_config(kind, network_config, health_check_config, remove, user_configs):
    return DatabaseConfig(
        image_tag=os.environ.get(kind.upper() + "_DATABASE_TAG", "latest"),
        name=os.environ.get(kind.upper() + "_DATABASE_NAME", "test_" + kind + "_database"),
        network=network_config,
        health_check=health_check_config,
        remove=remove,
        password=os.environ.get(kind.upper() + "_DATABASE_ROOT_PASS", "root"),
        users=user_configs,
        schema_name=os.environ.get(kind.upper() + "_DATABASE_END_USER_SCHEMA", kind + "_end_user_schema"),
    )


@pytest.fixture(scope="module")
def local_db_config(network_config, health_check_config, remove, local_user_configs):
    return get_db_config("local", network_config, health_check_config, remove, local_user_configs)


@pytest.fixture(scope="module")
def src_minio_config(network_config, health_check_config):
    return get_minio_config(network_config, health_check_config, "source")


def get_minio_config(network_config, health_check_config, kind):
    return MinIOConfig(
        image_tag=os.environ.get(kind.upper() + "_MINIO_TAG", "latest"),
        name=os.environ.get(kind.upper() + "_MINIO_NAME", "test_" + kind + "_minio"),
        network=network_config,
        health_check=health_check_config,
        remove=remove,
        access_key=os.environ.get(kind.upper() + "_MINIO_ACCESS_KEY", kind + "_minio_access_key"),
        secret_key=os.environ.get(kind.upper() + "_MINIO_SECRET_KEY", kind + "_minio_secret_key"),
    )


@pytest.fixture(scope="module")
def local_minio_config(network_config, health_check_config):
    return get_minio_config(network_config, health_check_config, "local")


@pytest.fixture(scope="module", autouse=True)
def src_db(src_db_config, docker_client):
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
def local_db(local_db_config, docker_client):
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
    if isinstance(container_config, DatabaseConfig):
        container = client.containers.run(
            "datajoint/mysql:" + container_config.image_tag,
            name=container_config.name,
            environment=dict(MYSQL_ROOT_PASSWORD=container_config.password),
            **common,
        )
    elif isinstance(container_config, MinIOConfig):
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
def src_minio(src_minio_config, docker_client):
    with create_container(docker_client, src_minio_config):
        yield


@pytest.fixture(scope="module", autouse=True)
def local_minio(local_minio_config, docker_client):
    with create_container(docker_client, local_minio_config):
        yield


@pytest.fixture
def src_store_config(src_minio_config):
    return get_store_config(src_minio_config, "source")


def get_store_config(minio_config, kind):
    return StoreConfig(
        name=os.environ.get(kind.upper() + "_STORE_NAME", kind + "_store"),
        protocol=os.environ.get(kind.upper() + "_STORE_PROTOCOL", "s3"),
        endpoint=minio_config.name + ":9000",
        bucket=os.environ.get(kind.upper() + "_STORE_BUCKET", kind + "-store-bucket"),
        location=os.environ.get(kind.upper() + "_STORE_LOCATION", kind + "_store_location"),
        access_key=minio_config.access_key,
        secret_key=minio_config.secret_key,
    )


@pytest.fixture
def local_store_config(local_minio_config):
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
def schemas(src_db_config, local_db_config, src_conn, local_conn):
    src_schema = main.SchemaProxy(src_db_config.schema_name, connection=src_conn)
    local_schema = main.SchemaProxy(local_db_config.schema_name, connection=local_conn)
    yield dict(src=src_schema, local=local_schema)
    local_schema.drop(force=True)
    with mysql_conn(src_db_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {'datajoint_outbound__' + src_db_config.schema_name};")
        conn.commit()
    src_schema.drop(force=True)


@pytest.fixture
def src_temp_dir():
    with TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def local_temp_dir():
    with TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def ext_files():
    return dict()


@pytest.fixture
def create_ext_files(src_temp_dir, ext_files):
    def _create_ext_files(kind, n=10, size=1024):
        if kind in ext_files:
            return
        files = []
        for i in range(n):
            filename = os.path.join(src_temp_dir, f"src_external{i}.rand")
            with open(filename, "wb") as file:
                file.write(os.urandom(size))
            files.append(filename)
        ext_files[kind] = files

    return _create_ext_files


@pytest.fixture
def get_src_data(ext_files, create_ext_files):
    def _get_src_data(use_part_table, use_external):
        src_data = dict(master=[dict(primary_key=i, secondary_key=-i) for i in range(10)])
        if use_part_table:
            src_data["part"] = [
                dict(primary_key=e["primary_key"], secondary_key=i) for i, e in enumerate(src_data["master"])
            ]
        if use_external:
            create_ext_files("master")
            src_data["master"] = [dict(e, ext_attr=f) for e, f in zip(src_data["master"], ext_files["master"])]
            if use_part_table:
                create_ext_files("part")
                src_data["part"] = [dict(e, ext_attr=f) for e, f in zip(src_data["part"], ext_files["part"])]
        return src_data

    return _get_src_data


@pytest.fixture
def get_expected_local_data(get_src_data, src_db_config, local_temp_dir):
    def get_expected_local_data(use_part_table, use_external):
        src_data = get_src_data(use_part_table=use_part_table, use_external=use_external)
        expected_local_data = dict(
            master=[
                dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name)
                for e in src_data["master"]
            ]
        )
        if use_external:
            for entity in expected_local_data["master"]:
                entity["ext_attr"] = os.path.join(local_temp_dir, os.path.basename(entity["ext_attr"]))
        if use_part_table:
            expected_local_data["part"] = [
                dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name)
                for e in src_data["part"]
            ]
            if use_external:
                for entity in expected_local_data["part"]:
                    entity["ext_attr"] = os.path.join(local_temp_dir, os.path.basename(entity["ext_attr"]))
        return expected_local_data

    return get_expected_local_data


@pytest.fixture
def get_src_table(src_store_config):
    def _get_src_table(use_part_table, use_external):
        master_definition = """
        primary_key: int
        ---
        secondary_key: int
        """
        if use_external:
            master_definition += "ext_attr: attach@" + src_store_config.name

        class Table(dj.Manual):
            definition = master_definition

        if use_part_table:
            part_definition = """
            -> master
            ---
            secondary_key: int
            """
            if use_external:
                part_definition += "ext_attr: attach@" + src_store_config.name

            class Part(dj.Part):
                definition = part_definition

            Table.Part = Part

        return Table

    return _get_src_table


@pytest.fixture
def get_local_data(schemas, src_db_config, get_src_data, get_src_table, local_temp_dir):
    def _get_local_data(use_part_table, use_external):
        src_data = get_src_data(use_part_table=use_part_table, use_external=use_external)
        src_table = schemas["src"](get_src_table(use_part_table=use_part_table, use_external=use_external))
        src_table().insert(src_data["master"])
        if use_part_table:
            src_table.Part().insert(src_data["part"])
        os.environ["REMOTE_DJ_USER"] = src_db_config.users["dj_user"].name
        os.environ["REMOTE_DJ_PASS"] = src_db_config.users["dj_user"].password
        remote_schema = main.SchemaProxy(src_db_config.schema_name, host=src_db_config.name)

        @main.Link(schemas["local"], remote_schema)
        class Table:
            pass

        Table().pull()
        local_data = dict(master=Table().fetch(as_dict=True, download_path=local_temp_dir))
        if use_part_table:
            local_data["part"] = Table.Part().fetch(as_dict=True, download_path=local_temp_dir)
        return local_data

    return _get_local_data


def test_pull(get_local_data, get_expected_local_data):
    assert get_local_data(use_part_table=False, use_external=False) == get_expected_local_data(
        use_part_table=False, use_external=False
    )


def test_pull_with_part_table(get_local_data, get_expected_local_data):
    assert get_local_data(use_part_table=True, use_external=False) == get_expected_local_data(
        use_part_table=True, use_external=False
    )


def test_pull_with_external_files(get_local_data, get_expected_local_data):
    assert get_local_data(use_part_table=False, use_external=True) == get_expected_local_data(
        use_part_table=False, use_external=True
    )


def test_pull_with_external_files_and_part_table(get_local_data, get_expected_local_data):
    assert get_local_data(use_part_table=True, use_external=True) == get_expected_local_data(
        use_part_table=True, use_external=True
    )
