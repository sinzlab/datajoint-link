import pytest
from datajoint import Part

from link.external.datajoint import dj_helpers


class TestReplaceStores:
    @pytest.fixture
    def original_definition_lines(self):
        return ["pa : attach@original_pa_store"]

    @pytest.fixture
    def original_definition(self, original_definition_lines):
        return "\n".join(original_definition_lines)

    @pytest.fixture
    def expected_definition_lines(self):
        return ["pa : attach@replacement_pa_store"]

    @pytest.fixture
    def expected_definition(self, expected_definition_lines):
        return "\n".join(expected_definition_lines)

    @pytest.fixture
    def stores(self):
        return dict(replacement_pa_store="original_pa_store")

    @pytest.fixture
    def add_store(self, original_definition_lines, expected_definition_lines, stores):
        original_definition_lines.append("pb: attach@original_pb_store")
        expected_definition_lines.append("pb: attach@replacement_pb_store")
        stores["replacement_pb_store"] = "original_pb_store"

    @pytest.fixture
    def add_store_name_outside_of_attached_attribute(
        self, original_definition_lines, expected_definition_lines, stores
    ):
        original_definition_lines.append("pc: int # original_pc_store")
        expected_definition_lines.append("pc: int # original_pc_store")
        stores["original_pc_store"] = "replacement_pc_store"

    @pytest.fixture
    def add_store_not_present_in_stores_mapping(self, original_definition_lines, expected_definition_lines):
        original_definition_lines.append("pc: attach@original_pc_store")
        expected_definition_lines.append("pc: attach@original_pc_store")

    @pytest.fixture
    def replacement_matches_expectation(self, original_definition, expected_definition, stores):
        def _replacement_matches_expectation():
            return dj_helpers.replace_stores(original_definition, stores) == expected_definition

        return _replacement_matches_expectation

    def test_if_single_store_is_replaced(self, replacement_matches_expectation):
        assert replacement_matches_expectation()

    @pytest.mark.usefixtures("add_store")
    def test_if_multiple_stores_are_replaced(self, replacement_matches_expectation):
        assert replacement_matches_expectation()

    @pytest.mark.usefixtures("add_store_name_outside_of_attached_attribute")
    def test_if_store_names_outside_of_attached_attributes_are_ignored(self, replacement_matches_expectation):
        assert replacement_matches_expectation()

    @pytest.mark.usefixtures("add_store_not_present_in_stores_mapping")
    def test_if_stores_not_present_in_stores_mapping_are_ignored(self, replacement_matches_expectation):
        assert replacement_matches_expectation()

    @pytest.mark.usefixtures("add_store_not_present_in_stores_mapping")
    def test_if_warning_is_raised_if_stores_missing_in_stores_mapping_are_encountered(
        self, replacement_matches_expectation
    ):
        with pytest.warns(UserWarning) as record:
            replacement_matches_expectation()
        assert len(record) == 1

    @pytest.mark.usefixtures("add_store_not_present_in_stores_mapping")
    def test_if_warning_message_is_correct(self, replacement_matches_expectation):
        with pytest.warns(UserWarning) as record:
            replacement_matches_expectation()
        assert record[0].message.args[0] == "No replacement for store 'original_pc_store' specified. Skipping!"


class TestGetPartTableClasses:
    @pytest.fixture
    def part_table_classes(self):
        return dict(PartA=type("PartA", (Part,), dict()))

    @pytest.fixture
    def other_attrs(self):
        return dict()

    @pytest.fixture
    def attrs(self, part_table_classes, other_attrs):
        return {**part_table_classes, **other_attrs}

    @pytest.fixture
    def table_cls(self, attrs):
        class Table:
            pass

        for name, attr in attrs.items():
            setattr(Table, name, attr)
        return Table

    @pytest.fixture
    def add_part_table_class(self, part_table_classes):
        part_table_classes["PartB"] = type("PartB", (Part,), dict())

    @pytest.fixture
    def ignored_parts(self):
        return []

    @pytest.fixture
    def add_ignored_part_table(self, other_attrs, ignored_parts):
        name = "IgnoredPart"
        other_attrs[name] = type(name, (Part,), dict())
        ignored_parts.append(name)

    @pytest.fixture
    def add_non_part_class(self, other_attrs):
        name = "NotAPart"
        other_attrs[name] = type(name, tuple(), dict())

    @pytest.fixture
    def add_non_class_attr(self, other_attrs):
        other_attrs["NotAClass"] = "NotAClass"

    @pytest.fixture
    def correct_part_tables_returned(self, table_cls, part_table_classes, ignored_parts):
        return dj_helpers.get_part_table_classes(table_cls, ignored_parts=ignored_parts) == part_table_classes

    @pytest.fixture
    def add_lowercase_part_table_class(self, other_attrs):
        name = "lowercase_part"
        other_attrs[name] = type(name, (Part,), dict())

    def test_if_ignored_parts_argument_is_optional(self, table_cls, part_table_classes):
        assert dj_helpers.get_part_table_classes(table_cls) == part_table_classes

    def test_if_single_part_table_is_found(self, correct_part_tables_returned):
        assert correct_part_tables_returned

    @pytest.mark.usefixtures("add_part_table_class")
    def test_if_multiple_part_tables_are_found(self, correct_part_tables_returned):
        assert correct_part_tables_returned

    @pytest.mark.usefixtures("add_ignored_part_table")
    def test_if_ignored_part_tables_are_ignored(self, correct_part_tables_returned):
        assert correct_part_tables_returned

    @pytest.mark.usefixtures("add_non_part_class")
    def test_if_non_part_classes_are_ignored(self, correct_part_tables_returned):
        assert correct_part_tables_returned

    @pytest.mark.usefixtures("add_non_class_attr")
    def test_if_non_class_attrs_are_ignored(self, correct_part_tables_returned):
        assert correct_part_tables_returned

    @pytest.mark.usefixtures("add_lowercase_part_table_class")
    def test_if_lowercase_part_table_classes_are_ignored(self, correct_part_tables_returned):
        assert correct_part_tables_returned
