import logging

logging.basicConfig(filename="kalauz.log",
                    encoding="utf-8",
                    format="%(asctime)s – %(levelname)s @ %(name)s @ %(funcName)s: %(message)s",
                    level=logging.DEBUG)


class LoggerMixin:

    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.addHandler(
            logging.StreamHandler()
        )

        self.logger.info("Logger initialized!")
        
    def _log_created_class(self, class_object) -> None:
        self.logger.info(f"{class_object.__class__.__name__} initialized!")
