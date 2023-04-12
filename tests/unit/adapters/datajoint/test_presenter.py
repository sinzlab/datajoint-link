from dataclasses import is_dataclass
from unittest.mock import create_autospec

import pytest

from dj_link.adapters.datajoint.presenter import Presenter, ViewModel
from dj_link.base import Base
from dj_link.use_cases import RESPONSE_MODELS


class TestViewModel:
    def test_if_dataclass(self):
        assert is_dataclass(ViewModel)

    @pytest.fixture
    def model(self):
        return ViewModel()

    @pytest.mark.parametrize("attr_name", ["message", "fields"])
    def test_if_message_property_raises_error_if_model_has_never_been_updated(self, model, attr_name):
        with pytest.raises(RuntimeError) as exc_info:
            getattr(model, attr_name)
        assert str(exc_info.value) == attr_name.title() + " attribute not set"

    @pytest.fixture
    def update_model(self, model):
        model.update("Hello World!", {"apples": 10})

    @pytest.mark.usefixtures("update_model")
    def test_if_message_property_returns_message_if_model_has_been_updated(self, model):
        assert model.message == "Hello World!"

    @pytest.mark.usefixtures("update_model")
    def test_if_fields_property_returns_fields_if_model_has_been_updated(self, model):
        assert model.fields == {"apples": 10}


def test_if_subclass_of_base():
    assert issubclass(Presenter, Base)


@pytest.fixture
def view_model_spy():
    return create_autospec(ViewModel, instance=True)


@pytest.fixture
def presenter(view_model_spy):
    return Presenter(view_model_spy)


class TestInit:
    def test_if_view_model_is_stored_as_instance_attribute(self, presenter, view_model_spy):
        assert presenter.view_model is view_model_spy


class TestPull:
    @pytest.fixture
    def response_model_stub(self):
        stub = create_autospec(RESPONSE_MODELS["pull"], instance=True)
        stub.n_requested = 10
        stub.n_valid = 5
        stub.n_invalid = 5
        return stub

    def test_if_view_model_is_updated(self, presenter, response_model_stub, view_model_spy):
        presenter.pull(response_model_stub)
        view_model_spy.update.assert_called_once_with(
            "Pull was successful",
            {"Number of requested entities": 10, "Number of valid entities": 5, "Number of invalid entities": 5},
        )


class TestDelete:
    @pytest.fixture
    def response_model_stub(self):
        stub = create_autospec(RESPONSE_MODELS["delete"], instance=True)
        stub.n_requested = 10
        stub.n_deletion_approved = 5
        stub.n_deleted_from_outbound = 3
        stub.n_deleted_from_local = 3
        return stub

    def test_if_view_model_is_updated(self, presenter, response_model_stub, view_model_spy):
        presenter.delete(response_model_stub)
        view_model_spy.update.assert_called_once_with(
            "Deletion was successful",
            {
                "Number of requested entities": 10,
                "Number of entities that had their deletion approved": 5,
                "Number of entities that were deleted from outbound table": 3,
                "Number of entities that were deleted from local table": 3,
            },
        )


class TestRefresh:
    @pytest.fixture
    def response_model_stub(self):
        stub = create_autospec(RESPONSE_MODELS["refresh"], instance=True)
        stub.n_refreshed = 10
        return stub

    def test_if_view_model_is_updated(self, presenter, response_model_stub, view_model_spy):
        presenter.refresh(response_model_stub)
        view_model_spy.update.assert_called_once_with("Refresh was successful", {"Number of refreshed entities": 10})
