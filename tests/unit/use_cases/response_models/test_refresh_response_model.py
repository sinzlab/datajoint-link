import pytest

from dj_link.use_cases.refresh import RefreshResponseModel


@pytest.fixture()
def refreshed(create_identifiers):
    return set(create_identifiers(10))


@pytest.fixture()
def model(refreshed):
    return RefreshResponseModel(refreshed=refreshed)


def test_n_refreshed_property(model, refreshed):
    assert model.n_refreshed == len(refreshed)
