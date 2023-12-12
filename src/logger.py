import logging
from typing import Final, Type

logging.basicConfig(filename="kalauz.log",
                    encoding="utf-8",
                    format="%(asctime)s – %(levelname)s @ %(name)s @ %(funcName)s: %(message)s",
                    level=logging.DEBUG)


class LoggerMixin:

    def __init__(self):
        self.logger = logging.getLogger()  # type: ignore
        self.logger.addHandler(
            logging.StreamHandler()
        )

        self.logger.info("Logger initialized!")
        
    def class_created(self, class_object) -> None:
        self.logger.info(f"{class_object.__class__.__name__} initialized!")
