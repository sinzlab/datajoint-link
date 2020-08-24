from unittest.mock import MagicMock

import pytest

from link.base import Base
from link.adapters.datajoint.abstract_facade import AbstractTableFacade
from link.adapters.datajoint.identification import IdentificationTranslator


@pytest.fixture
def primary_keys():
    return [dict(a=0, b=2), dict(a=5, b=20), dict(a=7, b=2)]


@pytest.fixture
def table_facade_stub(primary_keys):
    return MagicMock(name="table_facade_stub", spec=AbstractTableFacade, primary_keys=primary_keys)


@pytest.fixture
def primary_key():
    return dict(a=5, b=20)


@pytest.fixture
def identifier():
    return "97b64207fa9872fe0263051190ca8990bc438fa8"


@pytest.fixture
def translator(table_facade_stub):
    return IdentificationTranslator(table_facade_stub)


def test_if_identification_translator_is_subclass_of_base():
    assert issubclass(IdentificationTranslator, Base)


def test_if_table_facade_is_stored_as_instance_attribute(translator, table_facade_stub):
    assert translator.table_facade is table_facade_stub


def test_if_correct_identifier_is_returned(translator, primary_key, identifier):
    assert translator.to_identifier(primary_key) == identifier


def test_if_returned_identifier_is_key_order_independent(translator, primary_key, identifier):
    reversed_key_order = {k: primary_key[k] for k in reversed(primary_key)}
    assert translator.to_identifier(reversed_key_order) == identifier


def test_if_correct_primary_key_is_returned(translator, primary_key, identifier):
    assert translator.to_primary_key(identifier) == primary_key
