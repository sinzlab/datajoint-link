from link.base import Base


def test_repr():
    class Employee(Base):
        def __init__(self, name, age, is_human=True):
            self.name = name
            self.age = age
            self.is_human = is_human
            self.is_bald = False

    assert repr(Employee("John", 23)) == "Employee(name='John', age=23, is_human=True)"
