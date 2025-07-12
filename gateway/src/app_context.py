import logging
import pika
from pika.adapters.blocking_connection import BlockingConnection
from pika.channel import Channel
from pika import PlainCredentials, ConnectionParameters
import os


class AppContext:
    logger: logging.Logger
    connection: BlockingConnection
    channel: Channel
    _params: ConnectionParameters

    def __init__(self) -> None:
        rabbitmq_credentials = PlainCredentials(
            os.environ["RABBITMQ_USER"],
            os.environ["RABBITMQ_PASS"]
        )

        self._params = ConnectionParameters(
            os.environ["RABBITMQ_HOST"],
            int(os.environ["RABBITMQ_PORT"]),
            credentials=rabbitmq_credentials
        )

        self.logger = logging.getLogger("Gateway")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s %(levelname)s %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self._connect()

    def _connect(self):
        self.connection = pika.BlockingConnection(self._params)
        self.channel = self.connection.channel()
        self.logger.info("Connected to RabbitMQ")

    def safe_publish(self, exchange: str, routing_key: str, body: str):
        if self.connection.is_closed or self.channel.is_closed:
            self.logger.warning("RabbitMQ channel closed — reconnecting...")
            self._connect()

        self.channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=body)

    def close(self) -> None:
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
        finally:
            self.logger.info("Closed RabbitMQ connection")
