import logging
from injector import CallableProvider, singleton
from src.core.logger import get_logger
from src.core.rabbit_mq_client import RabbitMqClient
from injector import Module


class AppModule(Module):
    def configure(self, binder):
        binder.bind(
            logging.Logger,
            to=CallableProvider(lambda: get_logger(
                name="app", level=logging.INFO)),
            scope=singleton,
        )

        def rabbit_mq_factory(injector):
            logger = injector.get(logging.Logger)
            return RabbitMqClient(logger)

        binder.bind(
            RabbitMqClient,
            to=CallableProvider(lambda: rabbit_mq_factory(binder.injector)),
            scope=singleton,
        )
