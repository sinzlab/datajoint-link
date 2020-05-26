import os
from tempfile import TemporaryDirectory

import pytest
import datajoint as dj

from link import main


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
def src_data(use_part_table, use_external, ext_files, create_ext_files):
    def add_ext_files(kind):
        create_ext_files(kind)
        src_data[kind] = [dict(e, ext_attr=f) for e, f in zip(src_data[kind], ext_files[kind])]

    src_data = dict(master=[dict(prim_attr=i, sec_attr=-i) for i in range(10)])
    if use_part_table:
        src_data["part"] = [dict(prim_attr=e["prim_attr"], sec_attr=i) for i, e in enumerate(src_data["master"])]
    if use_external:
        add_ext_files("master")
        if use_part_table:
            add_ext_files("part")
    return src_data


@pytest.fixture
def exp_local_data(use_part_table, use_external, src_data, src_db_config, local_temp_dir):
    def create_exp_local_data(kind):
        exp_local_data[kind] = [
            dict(e, remote_host=src_db_config.name, remote_schema=src_db_config.schema_name) for e in src_data[kind]
        ]

    def convert_ext_paths(kind):
        for entity in exp_local_data[kind]:
            entity["ext_attr"] = os.path.join(local_temp_dir, os.path.basename(entity["ext_attr"]))

    exp_local_data = dict()
    create_exp_local_data("master")
    if use_part_table:
        create_exp_local_data("part")
    if use_external:
        convert_ext_paths("master")
        if use_part_table:
            convert_ext_paths("part")
    return exp_local_data


@pytest.fixture
def src_table_cls(use_part_table, use_external, src_store_config):
    def add_ext_attr(definition):
        definition += "ext_attr: attach@" + src_store_config.name
        return definition

    master_definition = """
    prim_attr: int
    ---
    sec_attr: int
    """
    if use_external:
        master_definition = add_ext_attr(master_definition)

    class Table(dj.Manual):
        definition = master_definition

    if use_part_table:
        part_definition = """
        -> master
        ---
        sec_attr: int
        """
        if use_external:
            part_definition = add_ext_attr(part_definition)

        class Part(dj.Part):
            definition = part_definition

        Table.Part = Part

    return Table


@pytest.fixture
def src_table_with_data(use_part_table, src_schema, src_table_cls, src_data):
    src_table = src_schema(src_table_cls)
    src_table.insert(src_data["master"])
    if use_part_table:
        src_table.Part().insert(src_data["part"])
    return src_table


@pytest.fixture
def local_data(use_part_table, local_schema, src_db_config, src_table_with_data, local_temp_dir):
    os.environ["REMOTE_DJ_USER"] = src_db_config.users["dj_user"].name
    os.environ["REMOTE_DJ_PASS"] = src_db_config.users["dj_user"].password
    remote_schema = main.SchemaProxy(src_db_config.schema_name, host=src_db_config.name)

    @main.Link(local_schema, remote_schema)
    class Table:
        pass

    Table().pull()
    local_data = dict(master=Table().fetch(as_dict=True, download_path=local_temp_dir))
    if use_part_table:
        local_data["part"] = Table.Part().fetch(as_dict=True, download_path=local_temp_dir)
    return local_data


@pytest.fixture(params=(True, False), ids=["with_part_table", "without_part_table"])
def use_part_table(request):
    return request.param


@pytest.fixture(params=(True, False), ids=["with_external", "without_external"])
def use_external(request):
    return request.param


def test_pull(local_data, exp_local_data):
    assert local_data == exp_local_data
