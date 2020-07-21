from link.entities.representation import represent


def test_represent():
    class SomeClass:
        __qualname__ = "SomeClass"
        some_attr = "some_value"
        another_attr = 5

    assert represent(SomeClass(), ["some_attr", "another_attr"]) == "SomeClass(some_attr='some_value', another_attr=5)"
