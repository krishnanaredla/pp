import logging
import sys
from logging import Logger
from logging.handlers import TimedRotatingFileHandler
import yaml
from typing import Dict, List
import os


class Error(Exception):
    pass


class PreProcessorException(Error):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


def load_config(path: str = None) -> Dict:
    logger = PreProcessorLogger(name="preprocessor_utils")
    if not path:
        path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "resources/config.yaml")
        )
    with open(path, "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.error(exc)
            raise FileNotFoundError(exc)
    return config


class PreProcessorLogger(Logger):
    def __init__(
        self,
        log_file=None,
        log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        *args,
        **kwargs
    ):
        self.formatter = logging.Formatter(log_format)
        self.log_file = log_file

        Logger.__init__(self, *args, **kwargs)

        self.addHandler(self.get_console_handler())
        if log_file:
            self.addHandler(self.get_file_handler())
        self.propagate = False

    def get_console_handler(self):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.formatter)
        return console_handler

    def get_file_handler(self):
        file_handler = TimedRotatingFileHandler(self.log_file, when="midnight")
        file_handler.setFormatter(self.formatter)
        return file_handler
