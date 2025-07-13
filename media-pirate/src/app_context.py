import aio_pika
import os
import logging
from typing import Optional
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel


class AsyncAppContext:
    logger: logging.Logger
    connection: Optional[AbstractRobustConnection]
    channel: Optional[AbstractRobustChannel]

    def __init__(self) -> None:
        self.logger = logging.getLogger("MediaPirate")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s %(levelname)s %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.connection = None
        self.channel = None

    async def connect(self) -> None:

        # Parse environment variables with sane defaults
        rabbitmq_user = os.environ.get("RABBITMQ_USER")
        rabbitmq_pass = os.environ.get("RABBITMQ_PASS")
        rabbitmq_host = os.environ.get("RABBITMQ_HOST")
        rabbitmq_port = os.environ.get(
            "RABBITMQ_PORT")  # ensures it's an integer
        rabbitmq_vhost = os.environ.get(
            "RABBITMQ_VHOST", "/")  # optional, default is "/"

        # Construct the connection URL
        rabbitmq_url = f"amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:{rabbitmq_port}{rabbitmq_vhost}"
        self.logger.info(rabbitmq_url)

        self.connection = await aio_pika.connect_robust(rabbitmq_url)
        self.channel = await self.connection.channel()
        self.logger.info("Connected to RabbitMQ (async)")

        # Optionally declare queues here if you want
        # await self.channel.declare_queue('telegram_events', durable=False)

    async def safe_publish(self, routing_key: str, body: str, exchange_name: str = '') -> None:
        if (self.connection is None or self.connection.is_closed or
                self.channel is None or self.channel.is_closed):
            self.logger.warning(
                "Connection or channel closed, reconnecting...")
            await self.connect()

        exchange: aio_pika.Exchange
        if exchange_name:
            exchange = await self.channel.get_exchange(exchange_name)
        else:
            exchange = self.channel.default_exchange  # type: ignore

        await exchange.publish(
            aio_pika.Message(body=body.encode()),
            routing_key=routing_key
        )
        self.logger.info(f"Published message to {routing_key}")

    async def close(self) -> None:
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        self.logger.info("Closed RabbitMQ connection")
