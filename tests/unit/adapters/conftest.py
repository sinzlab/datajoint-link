import pytest


@pytest.fixture
def identifiers():
    return [
        "62aad6b1b90f0613ac14b3ed0f5ecbf1c3cca448",
        "2d78c5aafa6200eb909bfc7b4b5b8f07284ad734",
        "e359f33515accad6b2e967135ee713cd17a200c9",
        "f62ac0bf9e4f661e617b935c76076bdfb5845cf3",
        "9f1d3a454a02283d83d2da2b02ce8950fb683d14",
        "f355683595377472c79473009e2cef9259254359",
    ]


@pytest.fixture
def restriction():
    return "restriction"
