from unittest.mock import MagicMock

import pytest

from link.adapters.datajoint.proxy import AbstractTableProxy
from link.adapters.datajoint.identification import IdentificationTranslator


@pytest.fixture
def primary_keys():
    return [dict(a=0, b=2), dict(a=5, b=20), dict(a=7, b=2)]


@pytest.fixture
def table_proxy_stub(primary_keys):
    name = "table_proxy_stub"
    table_proxy_stub = MagicMock(name=name, spec=AbstractTableProxy, primary_keys=primary_keys)
    table_proxy_stub.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return table_proxy_stub


@pytest.fixture
def primary_key():
    return dict(a=5, b=20)


@pytest.fixture
def identifier():
    return "97b64207fa9872fe0263051190ca8990bc438fa8"


@pytest.fixture
def translator(table_proxy_stub):
    return IdentificationTranslator(table_proxy_stub)


def test_if_table_proxy_is_stored_as_instance_attribute(translator, table_proxy_stub):
    assert translator.table_proxy is table_proxy_stub


def test_if_correct_identifier_is_returned(translator, primary_key, identifier):
    assert translator.to_identifier(primary_key) == identifier


def test_if_returned_identifier_is_key_order_independent(translator, primary_key, identifier):
    reversed_key_order = {k: primary_key[k] for k in reversed(primary_key)}
    assert translator.to_identifier(reversed_key_order) == identifier


def test_if_correct_primary_key_is_returned(translator, primary_key, identifier):
    assert translator.to_primary_key(identifier) == primary_key


def test_repr(translator):
    assert repr(translator) == "IdentificationTranslator(table_proxy=table_proxy_stub)"
