import logging
from datetime import date


class Logger:
    @staticmethod
    def getLogger() -> logging:
        today = date.today()
        logging.basicConfig(
            filename=f"logs/service-{today.strftime('%Y-%m-%d')}.log", encoding="UTF8", level=logging.INFO)
        return logging
