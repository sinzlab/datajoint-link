import os
from tempfile import TemporaryDirectory

import pytest
import datajoint as dj

from link import main, schemas


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
def src_dir():
    with TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def file_size():
    return int(os.environ.get("FILE_SIZE", 1024))


@pytest.fixture
def file_paths(n_entities, file_size, src_dir):
    file_paths = []
    for i in range(n_entities):
        filename = os.path.join(src_dir, f"src_external{i}.rand")
        with open(filename, "wb") as file:
            file.write(os.urandom(file_size))
        file_paths.append(filename)
    return file_paths


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
def local_dir():
    with TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def local_table_cls_with_pulled_data(local_table_cls):
    local_table_cls().pull()
    return local_table_cls


@pytest.fixture
def pulled_data(local_table_cls_with_pulled_data):
    return local_table_cls_with_pulled_data().fetch(as_dict=True)


@pytest.fixture
def expected_data(src_data, src_db_config):
    return [dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name) for e in src_data]
