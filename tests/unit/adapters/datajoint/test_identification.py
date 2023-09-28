from __future__ import annotations

from link.adapters.custom_types import PrimaryKey
from link.adapters.identification import IdentificationTranslator


def test_primary_key_translated_to_identifier_and_back_is_identical_to_original() -> None:
    translator = IdentificationTranslator()
    primary_key = {"a": 5, "b": 20}
    assert translator.to_primary_key(translator.to_identifier(primary_key)) == primary_key


def test_translating_same_primary_key_multiple_times_yields_same_identifier() -> None:
    translator = IdentificationTranslator()
    primary_key: PrimaryKey = {"saffd": "heasdf", "12": 12}
    assert len({translator.to_identifier(primary_key) for _ in range(10)}) == 1


def test_different_primary_keys_are_translated_to_different_identifiers() -> None:
    translator = IdentificationTranslator()
    primary_key1: PrimaryKey
    primary_key2: PrimaryKey
    primary_key1, primary_key2 = {"a": 5, "b": 20}, {"abc": 1.2, "asd": "hello"}
    assert translator.to_identifier(primary_key1) != translator.to_identifier(primary_key2)


def test_translating_multiple_primary_keys_to_identifiers() -> None:
    translator = IdentificationTranslator()
    primary_keys = [{"a": 5, "b": 4}, {"a": 12, "b": 8}, {"a": 7, "b": 0}]
    assert translator.to_identifiers(primary_keys) == {translator.to_identifier(key) for key in primary_keys}
