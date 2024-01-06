from abc import ABC, abstractmethod
from io import BytesIO
import logging
from typing import Final

import pandas as pd
import requests

from src.process_new_email.database_connection import Database


class HelperTableUpdater(Database, ABC):
    def __init__(self) -> None:
        super().__init__()

        self.logger = logging.getLogger(__name__)

        self.DATA_URL: str = NotImplemented
        self.TABLE_NAME: str = NotImplemented
        self._data_to_process: BytesIO = NotImplemented

    def download_data(self, url: str) -> BytesIO:
        response = requests.get(
            url=url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Safari/605.1.15"
            },
        )
        response.raise_for_status()
        self.logger.info(f"File successfully downloaded from {url}!")
        return BytesIO(response.content)

    @abstractmethod
    def process_data(self) -> None:
        pass

    @abstractmethod
    def store_data(self) -> None:
        pass


class UICTableUpdater(HelperTableUpdater, ABC):
    def __init__(self) -> None:
        super().__init__()

        self.data: pd.DataFrame = NotImplemented

        self.DATA_BASE_URL: Final = "https://uic.org/spip.php?action=telecharger&arg="

        self.logger.info(f"{self.__class__.__name__} initialized!")

    @abstractmethod
    def process_data(self) -> None:
        pass

    @abstractmethod
    def store_data(self) -> None:
        pass
