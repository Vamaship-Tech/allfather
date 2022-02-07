import logging
import os
from os.path import exists
from pathlib import Path
from datetime import date


class Logger:
    @staticmethod
    def get_logger() -> logging:
        today = date.today()
        cwd = os.getcwd()
        filename = f"{cwd}/logs/service-{today.strftime('%Y-%m-%d')}.log"
        if not exists(filename):
            Path(filename).touch()
        logging.basicConfig(
            filename=filename, encoding="utf-8", level=logging.INFO)
        return logging
