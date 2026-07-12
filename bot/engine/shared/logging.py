"""Logger factory — consistent prefix so engine logs are grep-able in the bot container."""

import logging


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"engine.{name}")
    if not logger.handlers and not logging.getLogger().handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("[%(name)s] %(levelname)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
