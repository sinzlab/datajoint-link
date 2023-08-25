"""Contains the presenter class and related classes/functions."""

from dj_link.use_cases.use_cases import DeleteResponseModel as DJDeleteResponseModel
from dj_link.use_cases.use_cases import PullResponseModel as DJPullResponseModel


class DJPresenter:
    """DataJoint-specific presenter."""

    def pull(self, response: DJPullResponseModel) -> None:
        """Present information about a finished pull use-case."""

    def delete(self, response: DJDeleteResponseModel) -> None:
        """Present information about a finished delete use-case."""
