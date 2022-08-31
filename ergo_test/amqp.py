from __future__ import annotations

import contextlib
import json
import logging
import time
from typing import Dict, List, Optional

import amqp.exceptions
import docker
import kombu
import kombu.pools
import kombu.simple
from amqp import Channel
from ergo.topic import SubTopic

from ergo_test import COMPONENT_TARGET, FunctionComponent, retries

try:
    from collections.abc import Generator
except ImportError:
    from typing import Generator

from ergo.message import Message, decodes
from ergo.topic import PubTopic

logger = logging.getLogger(__file__)

BROKER_URL = "amqp://guest:guest@localhost:5672/%2F"
CONNECTION = kombu.Connection(BROKER_URL)
EXCHANGE = "amq.topic"  # use a pre-declared exchange that we can bind to while the ergo runtime is booting
SHORT_TIMEOUT = 0.01  # seconds
LONG_TIMEOUT = 5  # seconds


def configure(broker_url=None):
    if broker_url:
        global BROKER_URL
        BROKER_URL = broker_url
        global CONNECTION
        CONNECTION = kombu.Connection(BROKER_URL)


class AMQPComponent(FunctionComponent):
    protocol = "amqp"
    instances: List[AMQPComponent] = []

    def __init__(
        self,
        target: COMPONENT_TARGET,
        **manifest
    ):
        super().__init__(target, **manifest)

        self.queue_name = f"{self.handler_path.replace('/', ':')[1:]}:{self.handler_name}"
        self.error_queue_name = f"{self.queue_name}:error"
        self.subtopic = self.manifest["subtopic"]
        self.pubtopic = self.manifest["pubtopic"]
        self._component_queue = kombu.Queue(name=self.queue_name, exchange=EXCHANGE, routing_key=str(SubTopic(self.subtopic)))
        self._in_context: bool = False
        self._running: bool = False

    @property
    def namespace(self):
        ns = {
            "protocol": "amqp",
            "host": BROKER_URL,
            "exchange": EXCHANGE,
            "subtopic": self.subtopic,
        }
        if self.pubtopic:
            ns["pubtopic"] = self.pubtopic
        return ns

    def rpc(self, payload: Dict, timeout=LONG_TIMEOUT):
        self.send(payload)
        return self.output.get(timeout=timeout)

    def send(self, payload: Dict):
        self._check_context()
        publish(payload, self.subtopic)

    def _await_startup(self, channel: Channel):
        self._check_context()
        if not self._running:
            self._await_startup_inner(channel)
            self._running = True

    def _await_startup_inner(self, channel: Channel):
        while True:
            try:
                _, _, consumers = channel.queue_declare(self.queue_name, passive=True)
                if consumers:
                    break
            except amqp.exceptions.NotFound:
                pass
            time.sleep(SHORT_TIMEOUT)

        channel.queue_purge(self.queue_name)

    def _check_context(self):
        assert self._in_context, "This method must be called from inside a 'with' block."

    def __enter__(self):
        self._in_context = True
        super().__enter__()
        self.instances.append(self)
        self.output = Queue(self.pubtopic, name=f"test:subscription:{self.pubtopic}")
        self.output.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._in_context = False
        self._running = False
        super().__exit__(exc_type, exc_val, exc_tb)
        self.instances.pop()
        with CONNECTION.channel() as channel:
            channel.queue_delete(self.queue_name)
            channel.queue_delete(self.error_queue_name)
        self.output.__exit__()


def publish(payload: dict, routing_key: str):
    with CONNECTION.channel() as channel:
        await_components(channel)
        with kombu.Producer(channel, serializer="raw") as producer:
            producer.publish(json.dumps(payload), exchange=EXCHANGE, routing_key=str(PubTopic(routing_key)))


def await_components(channel: Optional[Channel]=None):
    own_channel = channel is None
    channel = channel or CONNECTION.channel()
    try:
        for instance in AMQPComponent.instances:
            instance._await_startup(channel)
    finally:
        if own_channel:
            channel.close()


class Queue:
    def __init__(self, routing_key, name: Optional[str] = None, **kombu_opts):
        self.name = name or f"test:{routing_key}"
        self.routing_key = routing_key
        self._kombu_opts = {"auto_delete": True, "durable": False, **kombu_opts}
        self._in_context: bool = False

    def get(self, block=True, timeout=LONG_TIMEOUT) -> Message:
        assert self._in_context, "This method must be called from inside a 'with' block."
        try:
            amqp_message = self._queue.get(block=block, timeout=timeout)
        except self._queue.Empty:
            raise TimeoutError(f"timeout exceeded reading from queue {self.name}")
        return decodes(amqp_message.body)

    def __enter__(self):
        self._in_context = True
        self._channel: Channel = CONNECTION.channel()
        exchange = kombu.Exchange(EXCHANGE, type="topic", channel=self._channel)
        self._spec = kombu.Queue(self.name, exchange=exchange, routing_key=str(SubTopic(self.routing_key)), no_ack=True, **self._kombu_opts)
        self._queue = kombu.simple.SimpleQueue(self._channel, self._spec, serializer="raw")
        return self

    def __exit__(self, *exc_info):
        self._in_context = False
        self._channel.queue_delete(self.name)
        self._channel.__exit__()


class propagate_errors(contextlib.ContextDecorator):
    def __init__(self):
        self._queue = kombu.Queue("test:propagate_errors_queue", exchange=EXCHANGE, routing_key="#", auto_delete=True, no_ack=True)

    def __enter__(self):
        self._channel: Channel = CONNECTION.channel()
        self._consumer = kombu.Consumer(self._channel, queues=[self._queue], callbacks=[self._handle_message])
        self._consumer.consume()
        return self

    def __exit__(self, *exc_info):
        self._consumer.close()
        self._channel.close()

    @staticmethod
    def _handle_message(body: str, _):
        ergo_msg = decodes(body)
        if ergo_msg.error:
            raise ComponentFailure(ergo_msg.traceback)


class ComponentFailure(Exception):
    pass


def start_rabbitmq_broker():
    """
    Start a rabbitmq server if none is running, and then wait for the broker to finish booting.
    """
    docker_client = docker.from_env()
    if not docker_client.containers.list(filters={"name": "rabbitmq"}):
        logger.info("starting rabbitmq container")
        docker_client.containers.run(
            name="rabbitmq",
            image="rabbitmq:3.8.16-management-alpine",
            ports={5672: 5672, 15672: 15672},
            detach=True,
        )
    block_on_broker_startup()


def block_on_broker_startup():
    for retry in retries(200, 0.5, AssertionError):
        with retry():
            CONNECTION.ensure_connection()
