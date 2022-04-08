from ergo_test.amqp import AMQPComponent


def product(x, y):
    return float(x) * float(y)


def test_product_amqp():
    with AMQPComponent(product) as component:
        component.send({"x": 4, "y": 5})
        assert component.output.get().data == 20.0
