import pytest

from link.use_cases.delete import DeleteResponseModel


@pytest.fixture
def deletion_approved():
    return {"identifier" + str(i) for i in range(5)}


@pytest.fixture
def deleted_from_outbound():
    return {"identifier" + str(i) for i in range(5, 10)}


@pytest.fixture
def deleted_from_local():
    return {"identifier" + str(i) for i in range(10)}


@pytest.fixture
def model(requested, deletion_approved, deleted_from_outbound, deleted_from_local):
    return DeleteResponseModel(
        requested=requested,
        deletion_approved=deletion_approved,
        deleted_from_outbound=deleted_from_outbound,
        deleted_from_local=deleted_from_local,
    )


@pytest.mark.parametrize(
    "name,length",
    [("requested", 10), ("deletion_approved", 5), ("deleted_from_outbound", 5), ("deleted_from_local", 10)],
)
def test_n_property(model, name, length):
    assert getattr(model, "n_" + name) == length
