import sys

from loguru import logger


def config_logging() -> None:

    logger.remove()  # gets rid of default logger
    logger.add(sys.stderr, level="INFO", format="<cyan>{time:YYYY-MM-DD HH:mm:ss}</cyan> | <magenta>{level: <8}</magenta> | <blue>{message}</blue>")
