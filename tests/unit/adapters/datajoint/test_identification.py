import pytest

from dj_link.adapters.datajoint.identification import IdentificationTranslator
from dj_link.base import Base


@pytest.fixture
def primary_key():
    return dict(a=5, b=20)


@pytest.fixture
def identifier():
    return (
        "ea420704c495156e41aaeda7c5f58301ea85d98e0003e914b1deedc6fd2f"
        "b19dbcd79bb10f4d3631c45373f67f0df16927e0da879ab99d1a7df59eefb7f77031"
    )


@pytest.fixture
def translator():
    return IdentificationTranslator()


def test_if_identification_translator_is_subclass_of_base():
    assert issubclass(IdentificationTranslator, Base)


def test_if_correct_identifier_is_returned(translator, primary_key, identifier):
    assert translator.to_identifier(primary_key) == identifier


def test_if_returned_identifier_is_key_order_independent(translator, primary_key, identifier):
    reversed_key_order = {k: primary_key[k] for k in reversed(primary_key)}
    assert translator.to_identifier(reversed_key_order) == identifier


def test_if_correct_primary_key_is_returned(translator, primary_key, identifier):
    translator.to_identifier(primary_key)
    assert translator.to_primary_key(identifier) == primary_key
