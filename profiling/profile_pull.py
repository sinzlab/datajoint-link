import cProfile
import os
import sys
from datetime import datetime
from subprocess import PIPE, Popen

import datajoint as dj
import docker
import pymysql

from dj_link import LazySchema, Link
from dj_link.docker import ContainerRunner

NETWORK_NAME = "profiling_network"
SRC_DB_PASSWORD = "password"
LOCAL_DB_PASSWORD = "password"
LINK_USER_NAME = "link"
LINK_USER_PASSWORD = "password"
OUTBOUND_SCHEMA_NAME = "outbound"
SRC_SCHEMA_NAME = "src"
LOCAL_SCHEMA_NAME = "local"
ENTRY_COUNT = int(sys.argv[2])
PRIMARY_KEY_COUNT = int(sys.argv[3])

common_db_config = {
    "image": "datajoint/mysql:latest",
    "environment": {"MYSQL_ROOT_PASSWORD": "password"},
    "detach": True,
    "network": NETWORK_NAME,
}
src_db_config = {"name": "src_db", **common_db_config}
local_db_config = {"name": "local_db", **common_db_config}

docker_client = docker.from_env()

with ContainerRunner(docker_client, src_db_config), ContainerRunner(docker_client, local_db_config):
    src_db_connection = pymysql.connect(
        host="src_db",
        user="root",
        password=SRC_DB_PASSWORD,
        cursorclass=pymysql.cursors.DictCursor,
    )
    with src_db_connection:
        with src_db_connection.cursor() as cursor:
            sql_statements = [
                f"CREATE USER '{LINK_USER_NAME}'@'%' IDENTIFIED BY '{LINK_USER_PASSWORD}';",
                f"GRANT ALL PRIVILEGES ON `{OUTBOUND_SCHEMA_NAME}`.* TO '{LINK_USER_NAME}'@'%';",
                f"GRANT SELECT, REFERENCES ON `{SRC_SCHEMA_NAME}`.* TO '{LINK_USER_NAME}'@'%';",
            ]
            for sql_statement in sql_statements:
                cursor.execute(sql_statement)
        src_db_connection.commit()

    dj.config["database.host"] = "src_db"
    dj.config["database.user"] = "root"
    dj.config["database.password"] = SRC_DB_PASSWORD
    src_schema = dj.schema(SRC_SCHEMA_NAME)

    @src_schema
    class Table(dj.Manual):
        definition = "\n".join(f"primary{i}: int" for i in range(PRIMARY_KEY_COUNT))

    Table().insert([{f"primary{i}": i + j for i in range(PRIMARY_KEY_COUNT)} for j in range(ENTRY_COUNT)])

    os.environ["LINK_USER"] = LINK_USER_NAME
    os.environ["LINK_PASS"] = LINK_USER_PASSWORD
    os.environ["LINK_OUTBOUND"] = OUTBOUND_SCHEMA_NAME

    dj.config["database.host"] = "local_db"
    dj.config["database.user"] = "root"
    dj.config["database.password"] = LOCAL_DB_PASSWORD
    dj.conn(reset=True)

    local_schema = LazySchema(LOCAL_SCHEMA_NAME)
    src_schema = LazySchema(SRC_SCHEMA_NAME, host="src_db")

    @Link(local_schema, src_schema)
    class Table:
        pass

    filename = f"pull_{ENTRY_COUNT}_{PRIMARY_KEY_COUNT}_{datetime.now().strftime('%Y%m%d_%H-%M-%S')}"
    filepath = os.path.join(sys.argv[1], filename)
    cProfile.run("Table().pull()", filepath + ".pstats")
    gprof2dot = Popen(["gprof2dot", "-f", "pstats", filepath + ".pstats"], stdout=PIPE)
    dot = Popen(["dot", "-Tpng", "-o", filepath + ".png"], stdin=gprof2dot.stdout, stdout=PIPE)
    gprof2dot.stdout.close()
    dot.communicate()
