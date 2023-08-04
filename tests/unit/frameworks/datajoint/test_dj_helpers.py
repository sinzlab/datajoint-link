import warnings

import pytest

from dj_link.frameworks import dj_helpers


class TestReplaceStores:
    @pytest.fixture()
    def original_definition_lines(self):
        return ["pa : attach@original_pa_store"]

    @pytest.fixture()
    def original_definition(self, original_definition_lines):
        return "\n".join(original_definition_lines)

    @pytest.fixture()
    def expected_definition_lines(self):
        return ["pa : attach@replacement_pa_store"]

    @pytest.fixture()
    def expected_definition(self, expected_definition_lines):
        return "\n".join(expected_definition_lines)

    @pytest.fixture()
    def stores(self):
        return dict(replacement_pa_store="original_pa_store")

    @pytest.fixture()
    def _add_store(self, original_definition_lines, expected_definition_lines, stores):
        original_definition_lines.append("pb: attach@original_pb_store")
        expected_definition_lines.append("pb: attach@replacement_pb_store")
        stores["replacement_pb_store"] = "original_pb_store"

    @pytest.fixture()
    def _add_store_name_outside_of_attached_attribute(
        self, original_definition_lines, expected_definition_lines, stores
    ):
        original_definition_lines.append("pc: int # original_pc_store")
        expected_definition_lines.append("pc: int # original_pc_store")
        stores["original_pc_store"] = "replacement_pc_store"

    @pytest.fixture()
    def _add_store_not_present_in_stores_mapping(self, original_definition_lines, expected_definition_lines):
        original_definition_lines.append("pc: attach@original_pc_store")
        expected_definition_lines.append("pc: attach@original_pc_store")

    @pytest.fixture()
    def replacement_matches_expectation(self, original_definition, expected_definition, stores):
        def _replacement_matches_expectation():
            return dj_helpers.replace_stores(original_definition, stores) == expected_definition

        return _replacement_matches_expectation

    @pytest.fixture()
    def recorded_warnings(self, original_definition, stores):
        with pytest.warns(UserWarning) as record:
            dj_helpers.replace_stores(original_definition, stores)
        return record

    def test_if_single_store_is_replaced(self, replacement_matches_expectation):
        assert replacement_matches_expectation()

    @pytest.mark.usefixtures("_add_store")
    def test_if_multiple_stores_are_replaced(self, replacement_matches_expectation):
        assert replacement_matches_expectation()

    @pytest.mark.usefixtures("_add_store_name_outside_of_attached_attribute")
    def test_if_store_names_outside_of_attached_attributes_are_ignored(self, replacement_matches_expectation):
        assert replacement_matches_expectation()

    @pytest.mark.usefixtures("_add_store_not_present_in_stores_mapping")
    def test_if_stores_not_present_in_stores_mapping_are_ignored(self, replacement_matches_expectation):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            assert replacement_matches_expectation()

    @pytest.mark.usefixtures("_add_store_not_present_in_stores_mapping")
    def test_if_user_warning_is_raised_if_stores_missing_in_stores_mapping_are_encountered(self, recorded_warnings):
        pass

    @pytest.mark.usefixtures("_add_store_not_present_in_stores_mapping")
    def test_if_only_one_warning_is_raised(self, recorded_warnings):
        assert len(recorded_warnings) == 1

    @pytest.mark.usefixtures("_add_store_not_present_in_stores_mapping")
    def test_if_warning_message_is_correct(self, recorded_warnings):
        assert (
            recorded_warnings[0].message.args[0] == "No replacement for store 'original_pc_store' specified. Skipping!"
        )
