from link.entities import data_storage

import pytest


@pytest.fixture
def storage():
    return data_storage.DataStorage()


@pytest.fixture
def store(data, storage):
    storage.store(data)


@pytest.mark.usefixtures("store")
def test_store_and_retrieve(identifiers, data, storage):
    assert storage.retrieve(identifiers) == data


@pytest.mark.usefixtures("store")
def test_contains(identifiers, storage):
    assert all(identifier in storage for identifier in identifiers)


def test_repr(storage):
    assert repr(storage) == "DataStorage()"
