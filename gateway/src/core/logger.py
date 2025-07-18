import logging
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

    def format(self, record):
        color = self.LEVEL_COLOR.get(record.levelname, self.RESET)
        record.correlation_id = get_correlation_id()
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        record.name = f"{self.BLUE}{record.name}{self.RESET}"
        return super().format(record)


def get_logger(name: str = "app", level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(ContextualColorFormatter(
            "[%(name)s] [%(levelname)s] [%(asctime)s] [corr_id=%(correlation_id)s] %(message)s"
        ))
        logger.addHandler(handler)

    return logger
