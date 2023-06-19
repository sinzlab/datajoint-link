import os
from tempfile import TemporaryDirectory

import pytest


@pytest.fixture()
def src_dir():
    with TemporaryDirectory(prefix="link_test_src_") as temp_dir:
        yield temp_dir


@pytest.fixture()
def file_size():
    return int(os.environ.get("FILE_SIZE", 1024))


@pytest.fixture()
def file_paths(n_entities, file_size, src_dir):
    file_paths = []
    for i in range(n_entities):
        filename = os.path.join(src_dir, f"src_external{i}.rand")
        with open(filename, "wb") as file:
            file.write(os.urandom(file_size))
        file_paths.append(filename)
    return file_paths


@pytest.fixture()
def local_dir():
    with TemporaryDirectory(prefix="link_test_local_") as temp_dir:
        yield temp_dir


@pytest.fixture()
def pulled_data(local_table_cls_with_pulled_data):
    return local_table_cls_with_pulled_data().fetch(as_dict=True)


@pytest.fixture()
def expected_data(src_data):
    return src_data
