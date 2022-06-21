import pathlib
from ergo_test.amqp import AMQPComponent, start_rabbitmq_broker


CONFIG_PATH = pathlib.Path(__file__).parent.joinpath("configs")


def setup_module():
    start_rabbitmq_broker()


def test_product_amqp():
    with AMQPComponent(str(CONFIG_PATH.joinpath("test_product_amqp.yml"))) as component:
        component.send({"x": 4, "y": 5})
        assert component.output.get().data == 20.0
