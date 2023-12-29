from abc import ABC, abstractmethod
from io import BytesIO
import logging
from typing import Final

import requests

from src.process_new_email.database_connection import DatabaseServer


class HelperTableUpdater(ABC):
    def __init__(self, database_connection: DatabaseServer, data_url: str) -> None:
        self.logger = logging.getLogger(__name__)

        self._DATA_URL: Final = data_url
        self._DATA_TO_PROCESS: Final[BytesIO] = self.download_data(data_url)

        self.database: Final = database_connection

    def download_data(self, url: str) -> BytesIO:
        response = requests.get(
            url,
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
