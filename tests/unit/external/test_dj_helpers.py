import pytest

from link.external import dj_helpers


@pytest.fixture
def original_definition_lines():
    return ["pa : attach@original_pa_store"]


@pytest.fixture
def original_definition(original_definition_lines):
    return "\n".join(original_definition_lines)


@pytest.fixture
def expected_definition_lines():
    return ["pa : attach@replacement_pa_store"]


@pytest.fixture
def expected_definition(expected_definition_lines):
    return "\n".join(expected_definition_lines)


@pytest.fixture
def stores():
    return dict(original_pa_store="replacement_pa_store")


@pytest.fixture
def add_store(original_definition_lines, expected_definition_lines, stores):
    original_definition_lines.append("pb: attach@original_pb_store")
    expected_definition_lines.append("pb: attach@replacement_pb_store")
    stores["original_pb_store"] = "replacement_pb_store"


@pytest.fixture
def add_store_name_outside_of_attached_attribute(original_definition_lines, expected_definition_lines, stores):
    original_definition_lines.append("pc: int # original_pc_store")
    expected_definition_lines.append("pc: int # original_pc_store")
    stores["original_pc_store"] = "replacement_pc_store"


@pytest.fixture
def replaced_definition_matches_expected_definition(original_definition, expected_definition, stores):
    return dj_helpers.replace_stores(original_definition, stores) == expected_definition


def test_if_single_store_is_replaced(replaced_definition_matches_expected_definition):
    assert replaced_definition_matches_expected_definition


@pytest.mark.usefixtures("add_store")
def test_if_multiple_stores_are_replaced(replaced_definition_matches_expected_definition):
    assert replaced_definition_matches_expected_definition


@pytest.mark.usefixtures("add_store_name_outside_of_attached_attribute")
def test_if_store_names_outside_of_attached_attributes_are_ignored(replaced_definition_matches_expected_definition):
    assert replaced_definition_matches_expected_definition
