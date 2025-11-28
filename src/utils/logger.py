import logging
import os
import sys

from loguru import logger as loguru_logger
import structlog


def configure_dev_logging():
    """
    Development = loguru + structlog console output
    """
    loguru_logger.remove()
    loguru_logger.add(sys.stderr, level="DEBUG", colorize=True, backtrace=True, diagnose=True)

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
    )

    # intercept Python logging -> loguru
    class InterceptHandler(logging.Handler):
        def emit(self, record):
            loguru_logger.opt(depth=6, exception=record.exc_info).log(record.levelname, record.getMessage())

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    return loguru_logger  # DEV 只用 loguru


def rename_keys(_, __, event_dict):
    if "timestamp" in event_dict:
        event_dict["ts"] = event_dict.pop("timestamp")
    if "event" in event_dict:
        event_dict["msg"] = event_dict.pop("event")
    return event_dict


def configure_prod_logging():
    """
    Production = pure structlog JSON
    No loguru.
    """
    # Clean up loguru
    loguru_logger.remove()  # 不使用 loguru 输出

    # Configure standard logging → JSON
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG,
        format="%(message)s",  # 必须添加这一行！
        force=True,
    )

    # Configure structlog JSON → Loki
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,  # merge bind() contextvars
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            rename_keys,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
    )

    return structlog.get_logger()  # PROD 只用 structlog


def setup_logging():
    env = os.getenv("ENV", "development").lower()
    if env == "development":
        return configure_dev_logging()
    else:
        return configure_prod_logging()


logger = setup_logging()
