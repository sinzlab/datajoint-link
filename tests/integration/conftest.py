from unittest.mock import MagicMock, create_autospec
from copy import deepcopy

import pytest

from dj_link.use_cases import REQUEST_MODELS, USE_CASES, AbstractGatewayLink, initialize_use_cases


@pytest.fixture
def config():
    return {
        "identifiers": {"source": 0, "outbound": 0, "local": 0},
        "flags": {
            "outbound": {"deletion_requested": [], "deletion_approved": []},
            "local": {"deletion_requested": []},
        },
    }


@pytest.fixture
def processed_config(config, create_identifiers):
    processed = deepcopy(config)
    for repo_name, repo_identifier_spec in processed["identifiers"].items():
        processed["identifiers"][repo_name] = create_identifiers(repo_identifier_spec)
    for repo_name, repo_flag_identifier_spec in processed["flags"].items():
        processed["flags"][repo_name] = {n: create_identifiers(v) for n, v in repo_flag_identifier_spec.items()}
    return processed


@pytest.fixture
def gateway_link_spy(processed_config):
    spy = create_autospec(AbstractGatewayLink, instance=True)
    for repo_name, identifiers in processed_config["identifiers"].items():
        getattr(spy, repo_name).identifiers = identifiers

    def make_get_flags(flags):
        def get_flags(identifier):
            return flags[identifier]

        return get_flags

    for repo_name, repo_flag_identifiers in processed_config["flags"].items():
        repo_flags = {
            i: {fn: i in fi for fn, fi in repo_flag_identifiers.items()}
            for i in processed_config["identifiers"][repo_name]
        }
        getattr(spy, repo_name).get_flags.side_effect = make_get_flags(repo_flags)
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


@pytest.fixture
def request_model(request):
    return REQUEST_MODELS[request.module.USE_CASE]
