import aio_pika
import os
import logging
from typing import Optional
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel
from src.core.logging_context import get_correlation_id


class ContextualColorFormatter(logging.Formatter):
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"

    LEVEL_COLOR = {
        "DEBUG": CYAN,
        "INFO": GREEN,
        "WARNING": YELLOW,
        "ERROR": RED,
        "CRITICAL": MAGENTA,
    }

    # def format(self, record):
    #     # Inject correlation_id into log record, fallback to "-"
    #     record.correlation_id = get_correlation_id()
    #     return super().format(record)

    def format(self, record):
        color = self.LEVEL_COLOR.get(record.levelname, self.RESET)
        record.correlation_id = get_correlation_id()
        # Inject color codes around levelname and logger name
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        record.name = f"{self.BLUE}{record.name}{self.RESET}"

        return super().format(record)


class ServiceContainer:
    logger: logging.Logger
    connection: Optional[AbstractRobustConnection]
    channel: Optional[AbstractRobustChannel]

    def __init__(self, log_level: int = logging.INFO, log_name="") -> None:
        self.logger = logging.getLogger(log_name)
        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = ContextualColorFormatter(
                "[%(name)s] [%(asctime)s] [%(levelname)s] [corr_id=%(correlation_id)s] %(message)s"
            )
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

        if not all([rabbitmq_user, rabbitmq_pass, rabbitmq_host, rabbitmq_port]):
            self.logger.error(
                "Missing one or more required RabbitMQ environment variables")
            raise ValueError("Incomplete RabbitMQ configuration")

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
