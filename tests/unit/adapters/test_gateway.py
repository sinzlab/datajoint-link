from abc import ABC

from link.adapters import gateway


class TestAbstractSourceGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractSourceGateway, ABC)


class TestAbstractNonSourceGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractNonSourceGateway, ABC)

    def test_if_subclass_of_abstract_source_gateway(self):
        assert issubclass(gateway.AbstractNonSourceGateway, gateway.AbstractSourceGateway)


class TestAbstractOutboundGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractOutboundGateway, ABC)

    def test_if_subclass_of_abstract_non_source_gateway(self):
        assert issubclass(gateway.AbstractOutboundGateway, gateway.AbstractNonSourceGateway)


class TestAbstractLocalGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractLocalGateway, ABC)

    def test_if_subclass_of_abstract_non_source_gateway(self):
        assert issubclass(gateway.AbstractLocalGateway, gateway.AbstractNonSourceGateway)
