from unittest.mock import MagicMock, create_autospec

import pytest

from link.use_cases import USE_CASES, AbstractGatewayLink, initialize_use_cases


@pytest.fixture
def create_identifiers():
    def _create_identifiers(start, stop=None):
        if stop is None:
            stop = start
            start = 0
        return ["identifier" + str(i) for i in range(start, stop)]

    return _create_identifiers


@pytest.fixture
def source_identifiers(create_identifiers):
    return create_identifiers(10)


@pytest.fixture
def outbound_identifiers(create_identifiers):
    return create_identifiers(5)


@pytest.fixture
def local_identifiers(create_identifiers):
    return create_identifiers(5)


@pytest.fixture
def all_identifiers(source_identifiers, outbound_identifiers, local_identifiers):
    return {"source": source_identifiers, "outbound": outbound_identifiers, "local": local_identifiers}


@pytest.fixture
def deletion_requested_identifiers():
    return []


@pytest.fixture
def deletion_approved_identifiers():
    return []


@pytest.fixture
def source_flags(source_identifiers):
    return {i: {} for i in source_identifiers}


@pytest.fixture
def outbound_flags(outbound_identifiers, deletion_requested_identifiers, deletion_approved_identifiers):
    return {
        i: {
            "deletion_requested": i in deletion_requested_identifiers,
            "deletion_approved": i in deletion_approved_identifiers,
        }
        for i in outbound_identifiers
    }


@pytest.fixture
def local_flags(local_identifiers, deletion_requested_identifiers):
    return {i: {"deletion_requested": i in deletion_requested_identifiers} for i in local_identifiers}


@pytest.fixture
def all_flags(source_flags, outbound_flags, local_flags):
    return {"source": source_flags, "outbound": outbound_flags, "local": local_flags}


@pytest.fixture
def config(all_identifiers, all_flags):
    return {"identifiers": all_identifiers, "flags": all_flags}


@pytest.fixture
def gateway_link_spy(config):
    spy = create_autospec(AbstractGatewayLink, instance=True)
    for name, identifiers in config["identifiers"].items():
        getattr(spy, name).identifiers = identifiers

    def make_get_flags(flags):
        def get_flags(identifier):
            return flags[identifier]

        return get_flags

    for name, repo_flags in config["flags"].items():
        getattr(spy, name).get_flags.side_effect = make_get_flags(repo_flags)
    return spy


@pytest.fixture
def output_port_spies():
    return {n: MagicMock(name=n + "_output_port_spy") for n in USE_CASES}


@pytest.fixture
def initialized_use_cases(gateway_link_spy, output_port_spies):
    return initialize_use_cases(gateway_link_spy, output_port_spies)


@pytest.fixture
def use_case(request, initialized_use_cases):
    return initialized_use_cases[request.module.USE_CASE]
