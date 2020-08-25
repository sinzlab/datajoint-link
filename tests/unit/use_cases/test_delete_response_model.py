from dataclasses import is_dataclass

import pytest

from link.use_cases.base import ResponseModel
from link.use_cases.delete import DeleteResponseModel


def test_if_dataclass():
    assert is_dataclass(DeleteResponseModel)


def test_if_subclass_of_response_model():
    assert issubclass(DeleteResponseModel, ResponseModel)


@pytest.fixture
def requested():
    return ["identifier" + str(i) for i in range(10)]


@pytest.fixture
def deletion_approved():
    return ["identifier" + str(i) for i in range(5)]


@pytest.fixture
def deleted_from_outbound():
    return ["identifier" + str(i) for i in range(5, 10)]


@pytest.fixture
def deleted_from_local():
    return ["identifier" + str(i) for i in range(10)]


@pytest.fixture
def model(requested, deletion_approved, deleted_from_outbound, deleted_from_local):
    return DeleteResponseModel(
        requested=requested,
        deletion_approved=deletion_approved,
        deleted_from_outbound=deleted_from_outbound,
        deleted_from_local=deleted_from_local,
    )


def test_n_requested_property(model, requested):
    assert model.n_requested == len(requested)


def test_n_deletion_approved_property(model, deletion_approved):
    assert model.n_deletion_approved == len(deletion_approved)


def test_n_deleted_from_outbound_property(model, deleted_from_outbound):
    assert model.n_deleted_from_outbound == len(deleted_from_outbound)


def test_n_deleted_from_local_property(model, deleted_from_local):
    assert model.n_deleted_from_local == len(deleted_from_local)
