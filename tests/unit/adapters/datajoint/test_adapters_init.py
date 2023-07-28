from unittest.mock import MagicMock

import pytest

from dj_link.adapters.datajoint import DataJointGatewayLink
from dj_link.adapters.datajoint.gateway import DataJointGateway
from dj_link.base import Base
from dj_link.globals import REPOSITORY_NAMES


@pytest.fixture(params=REPOSITORY_NAMES)
def repo_type(request):
    return request.param


class TestDataJointGatewayLink:
    @pytest.fixture()
    def gateway_stubs(self):
        gateway_stubs = {}
        for repo_type in REPOSITORY_NAMES:
            gateway_stub = MagicMock(name=repo_type + "_gateway_stub", spec=DataJointGateway)
            gateway_stubs[repo_type] = gateway_stub
        return gateway_stubs

    @pytest.fixture()
    def gateway_link(self, gateway_stubs):
        return DataJointGatewayLink(**{kind: gateway for kind, gateway in gateway_stubs.items()})

    def test_if_subclass_of_base(self):
        assert issubclass(DataJointGatewayLink, Base)

    def test_if_gateway_is_stored_as_instance_attribute(self, repo_type, gateway_link, gateway_stubs):
        assert getattr(gateway_link, repo_type) is gateway_stubs[repo_type]
