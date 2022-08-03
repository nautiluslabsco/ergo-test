from ergo_test import Component
from ergo_test.amqp import BROKER_URL, EXCHANGE


class HTTPGateway(Component):
    _ergo_command = "gateway"

    @property
    def namespace(self):
        return {
            "host": BROKER_URL,
            "exchange": EXCHANGE,
        }
