import pytest

from link.entities.repository import Entity


class TestEntity:
    @pytest.fixture
    def flags(self):
        return dict(flag=True)

    def test_if_identifier_is_set_as_instance_attribute(self, identifier):
        assert Entity(identifier).identifier == identifier

    def test_if_flags_are_set_as_instance_attribute(self, identifier, flags):
        assert Entity(identifier, flags=flags).flags == flags

    def test_if_flags_instance_attribute_is_set_to_empty_dict_if_no_flags_are_provided(self, identifier):
        assert Entity(identifier).flags == dict()

    def test_repr(self, identifier, flags):
        assert repr(Entity(identifier, flags=flags)) == "Entity(identifier='identifier0', flags={'flag': True})"
