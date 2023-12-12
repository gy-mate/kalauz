from abc import ABC, abstractmethod
import os
from typing import Final

from dotenv import load_dotenv
from io import BytesIO
import mysql.connector
import requests

from src.logger import LoggerMixin


class HelperTableUpdater(LoggerMixin, ABC):

    def __init__(self, data_url: str):
        super().__init__()
        load_dotenv()
        
        self.DATA_URL: Final = data_url
        self.DATA_TO_PROCESS: Final[BytesIO] = self.download_data(data_url)
        
        self.CONNECTION_TO_DATABASE: Final = mysql.connector.connect(
            host="localhost",
            user="root",
            password=os.getenv("DATABASE_PASSWORD"),
            database="kalauz"
        )
        self.CURSOR: Final = self.CONNECTION_TO_DATABASE.cursor()
    
    def download_data(self, url: str) -> BytesIO:
        response = requests.get(
            url,
            headers={
                "User-Agent":
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Safari/605.1.15"
            }
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
