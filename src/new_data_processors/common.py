from abc import ABC, abstractmethod
from datetime import datetime
import logging
from typing import Any, ClassVar, Final

from pandas import DataFrame
import requests
from requests import HTTPError
from sqlalchemy import MetaData, Table

# future: remove the comment below when stubs for the library below are available
import xlrd  # type: ignore

from src.new_data_processors.database_connection import Database


class DataProcessor(ABC):
    TODAY = datetime.today().date()

    def __init__(self) -> None:
        super().__init__()

        self.logger = logging.getLogger(__name__)
        self.database = Database()


class TableUpdater(DataProcessor, ABC):
    TABLE_NAME: ClassVar[str] = NotImplemented
    database_metadata: ClassVar[MetaData] = NotImplemented
    table: ClassVar[Table] = NotImplemented

    def __init__(self) -> None:
        super().__init__()

        self.DATA_URL: str = NotImplemented

        self.data: Any = NotImplemented

    def run(self) -> None:
        self.process_data()
        self.store_data()

        self.logger.info(f"Table `{self.TABLE_NAME}` sucessfully updated!")

    @abstractmethod
    def process_data(self) -> None:
        pass

    def store_data(self) -> None:
        self.create_table_if_not_exists()
        self.add_data()

    def create_table_if_not_exists(self) -> None:
        self.table.create(
            bind=self.database.engine,
            checkfirst=True,
        )
        self.logger.debug(f"Table `{self.TABLE_NAME}` sucessfully created (if needed)!")

    @abstractmethod
    def add_data(self) -> None:
        pass


class DataDownloader(TableUpdater, ABC):
    def __init__(self) -> None:
        super().__init__()

        self.data: DataFrame = NotImplemented

        self._dowload_session = requests.Session()

    def get_data(self, url: str) -> bytes:
        try:
            response = self._dowload_session.get(
                url=url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Safari/605.1.15"
                },
            )
            response.raise_for_status()
            self.logger.debug(f"File successfully downloaded from {url}!")
            return bytes(response.content)
        except HTTPError:
            self.logger.critical(f"Failed to download file from {url}!")
            raise

    def process_data(self) -> None:
        self.correct_column_names()
        self.delete_data()
        self.correct_data()

    def correct_column_names(self) -> None:
        self.rename_columns_manually()

    @abstractmethod
    def rename_columns_manually(self) -> None:
        pass

    def delete_data(self) -> None:
        pass

    def correct_data(self) -> None:
        self.correct_data_manually()
        self.correct_boolean_values()

    @abstractmethod
    def correct_data_manually(self) -> None:
        pass

    @abstractmethod
    def correct_boolean_values(self) -> None:
        pass


class UICTableUpdater(DataDownloader, ABC):
    def __init__(self) -> None:
        super().__init__()

        self.DATA_BASE_URL: Final = "https://uic.org/spip.php?action=telecharger&arg="

        self._data_to_process: bytes = NotImplemented

    def rename_columns_manually(self) -> None:
        pass

    def correct_data_manually(self) -> None:
        pass

    def correct_boolean_values(self) -> None:
        pass
