from ergo_test import Component
from ergo_test.amqp import AMQP_HOST, EXCHANGE


class HTTPGateway(Component):
    _ergo_command = "gateway"

    @property
    def namespace(self):
        return {
            "host": AMQP_HOST,
            "exchange": EXCHANGE,
        }
