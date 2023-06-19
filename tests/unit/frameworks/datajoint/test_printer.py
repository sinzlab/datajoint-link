from unittest.mock import create_autospec

import pytest

from dj_link.adapters.datajoint.presenter import ViewModel
from dj_link.base import Base
from dj_link.frameworks.datajoint.printer import Printer


def test_if_subclass_of_base():
    assert issubclass(Printer, Base)


@pytest.fixture(
    params=[
        (
            "Message width greater than maximum field width.",
            {"Apple": 10, "Banana": 3, "Watermelon": 7},
            [
                "=================================================",
                " Message width greater than maximum field width. ",
                "-------------------------------------------------",
                "Apple:                                         10",
                "Banana:                                         3",
                "Watermelon:                                     7",
                "=================================================",
            ],
        ),
        (
            "Message width equal to maximum field width.",
            {"Apple": 10, "Some nice fruit with a really long name": 31, "Pineapple": 7},
            [
                "=============================================",
                " Message width equal to maximum field width. ",
                "---------------------------------------------",
                "Apple:                                     10",
                "Some nice fruit with a really long name:   31",
                "Pineapple:                                  7",
                "=============================================",
            ],
        ),
        (
            "Not equal to 0.",
            {"Apple": 10, "Banana": 3, "Watermelon": 7123},
            [
                "=================",
                " Not equal to 0. ",
                "-----------------",
                "Apple:         10",
                "Banana:         3",
                "Watermelon:  7123",
                "=================",
            ],
        ),
        (
            "Hello John!",
            {"Apple": 10, "Banana": 3, "Watermelon": 7},
            [
                "=============",
                " Hello John! ",
                "-------------",
                "Apple:     10",
                "Banana:     3",
                "Watermelon: 7",
                "=============",
            ],
        ),
    ]
)
def data(request):
    return request.param


@pytest.fixture()
def message(data):
    return data[0]


@pytest.fixture()
def fields(data):
    return data[1]


@pytest.fixture()
def view_model_stub(message, fields):
    stub = create_autospec(ViewModel, instance=True)
    stub.message = message
    stub.fields = fields
    return stub


@pytest.fixture()
def printer(view_model_stub):
    return Printer(view_model_stub)


@pytest.fixture()
def expected(data):
    return "\n".join(data[2] + [""])


def test_if_view_model_gets_stored_as_instance_attribute(printer, view_model_stub):
    assert printer.view_model is view_model_stub


def test_if_contents_of_view_model_get_correctly_printed(capsys, printer, expected):
    printer()
    captured = capsys.readouterr()
    assert captured.out == expected
