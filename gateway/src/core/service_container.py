import aio_pika
import os
import logging
from typing import Optional
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel
from src.core.logging_context import get_correlation_id
import redis.asyncio as redis
from redis.asyncio.client import Redis


class ContextualColorFormatter(logging.Formatter):
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"
    GRAY = "\033[90m"

    LEVEL_COLOR = {
        "DEBUG": CYAN,
        "INFO": GREEN,
        "WARNING": YELLOW,
        "ERROR": RED,
        "CRITICAL": MAGENTA,
    }

    STANDARD_ATTRS = logging.LogRecord(
        name="", level=0, pathname="", lineno=0, msg="", args=(), exc_info=None
    ).__dict__.keys()

    def format(self, record):
        # Add correlation ID
        record.correlation_id = get_correlation_id()

        # Colorize levelname and logger name
        color = self.LEVEL_COLOR.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        record.name = f"{self.BLUE}{record.name}{self.RESET}"

        # Get base log message
        base_message = super().format(record)

        # Extract custom extras
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in self.STANDARD_ATTRS and k != "message"
        }

        # Format extras nicely
        if extras:
            max_key_len = max(len(k) for k in extras)
            extra_lines = "\n".join(
                f"    {self.GRAY}{k.ljust(max_key_len)}{self.RESET} = {v!r}" for k, v in extras.items()
            )
            return f"{base_message}\n{self.CYAN}Extras:{self.RESET}\n{extra_lines}"
        else:
            return base_message


class ServiceContainer:
    logger: logging.Logger
    connection: Optional[AbstractRobustConnection]
    channel: Optional[AbstractRobustChannel]
    redis: Optional[Redis]

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.connection = None
        self.channel = None
        self.redis = None

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

    @classmethod
    async def create(cls, log_level: int = logging.INFO, log_name="") -> "ServiceContainer":
        logger = logging.getLogger(log_name)
        logger.setLevel(log_level)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = ContextualColorFormatter(
                "[%(name)s] [%(asctime)s] [%(levelname)s] [corr_id=%(correlation_id)s] %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        self = cls(logger)

        # --- RabbitMQ Setup ---
        rabbitmq_user = os.environ.get("RABBITMQ_USER")
        rabbitmq_pass = os.environ.get("RABBITMQ_PASS")
        rabbitmq_host = os.environ.get("RABBITMQ_HOST")
        rabbitmq_port = os.environ.get("RABBITMQ_PORT")
        rabbitmq_vhost = os.environ.get("RABBITMQ_VHOST", "/")

        if not all([rabbitmq_user, rabbitmq_pass, rabbitmq_host, rabbitmq_port]):
            logger.error(
                "Missing one or more required RabbitMQ environment variables")
            raise ValueError("Incomplete RabbitMQ configuration")

        rabbitmq_url = f"amqp://{rabbitmq_user}:{rabbitmq_pass}@{rabbitmq_host}:{rabbitmq_port}{rabbitmq_vhost}"
        logger.info(f"Connecting to RabbitMQ: {rabbitmq_url}")

        self.connection = await aio_pika.connect_robust(rabbitmq_url)
        self.channel = await self.connection.channel()
        logger.info("Connected to RabbitMQ")

        # --- Redis Setup ---
        redis_host = os.environ.get("REDIS_HOST")
        redis_port = int(os.environ.get("REDIS_PORT"))
        redis_password = os.environ.get("REDIS_PASSWORD")

        if not all([redis_host, redis_port, redis_password]):
            logger.error(
                "Missing one or more required Redis environment variables")
            raise ValueError("Incomplete Redis configuration")

        redis_db = 0
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
        )

        try:
            pong = await self.redis.ping()
            if pong:
                logger.info(
                    f"Connected to Redis at {redis_host}:{redis_port} [db={redis_db}]")
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            self.redis = None

        return self
