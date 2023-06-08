from dj_link.adapters.datajoint.identification import UUIDIdentificationTranslator


def test_primary_key_translated_to_identifier_and_back_is_identical_to_original():
    translator = UUIDIdentificationTranslator()
    primary_key = {"a": 5, "b": 20}
    assert translator.to_primary_key(translator.to_identifier(primary_key)) == primary_key


def test_translating_same_primary_key_multiple_times_yields_same_identifier():
    translator = UUIDIdentificationTranslator()
    primary_key = {"saffd": "heasdf", "12": 12}
    assert len({translator.to_identifier(primary_key) for _ in range(10)}) == 1


def test_different_primary_keys_are_translated_to_different_identifiers():
    translator = UUIDIdentificationTranslator()
    primary_key1, primary_key2 = {"a": 5, "b": 20}, {"abc": 1.2, "asd": "hello"}
    assert translator.to_identifier(primary_key1) != translator.to_identifier(primary_key2)
