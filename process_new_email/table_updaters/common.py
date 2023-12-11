from abc import ABC, abstractmethod
import os

from dotenv import load_dotenv
from io import BytesIO
import mysql.connector
import requests

from logger import LoggerMixin


class HelperTableUpdater(LoggerMixin, ABC):

    def __init__(self, data_url: str):
        super().__init__()
        
        load_dotenv()
        
        self.data_url = data_url
        self.data_to_process: BytesIO = self.download_data(data_url)
        
        self.connection_to_database = mysql.connector.connect(
            host="localhost",
            user="root",
            password=os.getenv("DATABASE_PASSWORD"),
            database="kalauz"
        )
        self.cursor = self.connection_to_database.cursor()
    
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
