from unittest.mock import MagicMock, create_autospec

import pytest

from link.use_cases import USE_CASES, AbstractGatewayLink, initialize_use_cases


@pytest.fixture
def config():
    return {
        "identifiers": {"source": (10,), "outbound": (5,), "local": (5,)},
        "flags": {
            "source": {"deletion_requested": [], "deletion_approved": []},
            "outbound": {"deletion_requested": [], "deletion_approved": []},
            "local": {"deletion_requested": [], "deletion_approved": []},
        },
    }


@pytest.fixture
def create_identifiers():
    def _create_identifiers(start, stop=None):
        if stop is None:
            stop = start
            start = 0
        return ["identifier" + str(i) for i in range(start, stop)]

    return _create_identifiers


@pytest.fixture
def source_identifiers(config, create_identifiers):
    return create_identifiers(*config["identifiers"]["source"])


@pytest.fixture
def outbound_identifiers(config, create_identifiers):
    return create_identifiers(*config["identifiers"]["outbound"])


@pytest.fixture
def local_identifiers(config, create_identifiers):
    return create_identifiers(*config["identifiers"]["local"])


@pytest.fixture
def all_identifiers(source_identifiers, outbound_identifiers, local_identifiers):
    return {"source": source_identifiers, "outbound": outbound_identifiers, "local": local_identifiers}


@pytest.fixture
def source_deletion_requested_identifiers(config):
    return config["flags"]["source"]["deletion_requested"]


@pytest.fixture
def outbound_deletion_requested_identifiers(config):
    return config["flags"]["outbound"]["deletion_requested"]


@pytest.fixture
def local_deletion_requested_identifiers(config):
    return config["flags"]["local"]["deletion_requested"]


@pytest.fixture
def all_deletion_requested_identifiers(
    source_deletion_requested_identifiers, outbound_deletion_requested_identifiers, local_deletion_requested_identifiers
):
    return {
        "source": source_deletion_requested_identifiers,
        "outbound": outbound_deletion_requested_identifiers,
        "local": local_deletion_requested_identifiers,
    }


@pytest.fixture
def source_deletion_approved_identifiers(config):
    return config["flags"]["source"]["deletion_approved"]


@pytest.fixture
def outbound_deletion_approved_identifiers(config):
    return config["flags"]["outbound"]["deletion_approved"]


@pytest.fixture
def local_deletion_approved_identifiers(config):
    return config["flags"]["local"]["deletion_approved"]


@pytest.fixture
def all_deletion_approved_identifiers(
    source_deletion_approved_identifiers, outbound_deletion_approved_identifiers, local_deletion_approved_identifiers
):
    return {
        "source": source_deletion_approved_identifiers,
        "outbound": outbound_deletion_approved_identifiers,
        "local": local_deletion_approved_identifiers,
    }


@pytest.fixture
def all_flag_identifiers(all_deletion_requested_identifiers, all_deletion_approved_identifiers):
    return {
        "deletion_requested": all_deletion_requested_identifiers,
        "deletion_approved": all_deletion_approved_identifiers,
    }


@pytest.fixture
def create_flags(all_flag_identifiers):
    def _create_flags(identifiers, repo_type):
        return {i: {n: i in fi[repo_type] for n, fi in all_flag_identifiers.items()} for i in identifiers}

    return _create_flags


@pytest.fixture
def source_flags(create_flags, source_identifiers):
    return create_flags(source_identifiers, "source")


@pytest.fixture
def outbound_flags(create_flags, outbound_identifiers):
    return create_flags(outbound_identifiers, "outbound")


@pytest.fixture
def local_flags(create_flags, local_identifiers):
    return create_flags(local_identifiers, "local")


@pytest.fixture
def all_flags(source_flags, outbound_flags, local_flags):
    return {"source": source_flags, "outbound": outbound_flags, "local": local_flags}


@pytest.fixture
def processed_config(all_identifiers, all_flags):
    return {"identifiers": all_identifiers, "flags": all_flags}


@pytest.fixture
def gateway_link_spy(processed_config):
    spy = create_autospec(AbstractGatewayLink, instance=True)
    for name, identifiers in processed_config["identifiers"].items():
        getattr(spy, name).identifiers = identifiers

    def make_get_flags(flags):
        def get_flags(identifier):
            return flags[identifier]

        return get_flags

    for name, repo_flags in processed_config["flags"].items():
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
