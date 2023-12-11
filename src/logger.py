import logging

logging.basicConfig(filename="kalauz.log",
                    encoding="utf-8",
                    format="%(asctime)s – %(levelname)s @ %(name)s: %(message)s",
                    level=logging.DEBUG)


class LoggerMixin:

    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.addHandler(
            logging.StreamHandler()
        )

        self.logger.info("Logger initialized!")
