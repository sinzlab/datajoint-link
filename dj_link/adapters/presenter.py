"""Contains the presenter class and related classes/functions."""

from dj_link.use_cases.use_cases import DeleteResponseModel, ProcessResponseModel, PullResponseModel


class DJPresenter:
    """DataJoint-specific presenter."""

    def pull(self, response: PullResponseModel) -> None:
        """Present information about a finished pull use-case."""

    def delete(self, response: DeleteResponseModel) -> None:
        """Present information about a finished delete use-case."""

    def process(self, response: ProcessResponseModel) -> None:
        """Present information about a finished process use-case."""
