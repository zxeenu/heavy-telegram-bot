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

    def __init__(self) -> None:
        rabbitmq_credentials = PlainCredentials(
            os.environ["RABBITMQ_USER"],
            os.environ["RABBITMQ_PASS"])

        rabbitmq_params = ConnectionParameters(
            os.environ["RABBITMQ_HOST"],
            os.environ["RABBITMQ_PORT"],
            credentials=rabbitmq_credentials)

        self.logger = logging.getLogger("Gateway")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s %(levelname)s %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.connection = pika.BlockingConnection(rabbitmq_params)
        self.channel = self.connection.channel()
        self.logger.info("Connected to RabbitMQ")

    def close(self) -> None:
        self.connection.close()
        self.logger.info("Closed RabbitMQ connection")
