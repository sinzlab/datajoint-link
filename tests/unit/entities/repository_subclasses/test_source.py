from link.entities import source
from link.entities import repository


def test_if_source_repository_is_subclass_of_read_only_repository():
    assert issubclass(source.SourceRepository, repository.ReadOnlyRepository)


def test_if_source_repository_is_not_subclass_of_repository():
    assert not issubclass(source.SourceRepository, repository.Repository)
