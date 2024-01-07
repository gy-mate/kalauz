from abc import ABC, abstractmethod
from io import BytesIO
import logging
from typing import Any, Final

import pandas as pd
import requests

# future: remove the comment below when stubs for the library below are available
import xlrd  # type: ignore

from src.process_new_email.database_connection import Database


class TableUpdater(ABC):
    def __init__(self) -> None:
        super().__init__()

        self.logger = logging.getLogger(__name__)
        self.database = Database()
        self._dowload_session = requests.Session()

        self.DATA_URL: str = NotImplemented
        self.TABLE_NAME: str = NotImplemented

        self.data: Any = NotImplemented
        self._data_to_process: bytes = NotImplemented

    def download_data(self, url: str) -> bytes:
        response = self._dowload_session.get(
            url=url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Safari/605.1.15"
            },
        )
        response.raise_for_status()
        self.logger.info(f"File successfully downloaded from {url}!")
        return bytes(response.content)

    @abstractmethod
    def process_data(self) -> None:
        pass

    @abstractmethod
    def store_data(self) -> None:
        pass


class UICTableUpdater(TableUpdater, ABC):
    def __init__(self) -> None:
        super().__init__()

        self.DATA_BASE_URL: Final = "https://uic.org/spip.php?action=telecharger&arg="

    @abstractmethod
    def process_data(self) -> None:
        pass

    @abstractmethod
    def store_data(self) -> None:
        pass


class ExcelProcessor(TableUpdater):
    def __init__(self) -> None:
        super().__init__()

        self.data: pd.DataFrame = NotImplemented

    def process_data(self) -> None:
        try:
            # future: remove the line below when https://youtrack.jetbrains.com/issue/PY-55260/ is fixed
            # noinspection PyTypeChecker
            self.data = pd.read_excel(BytesIO(self._data_to_process))
        except xlrd.compdoc.CompDocError:
            workbook = xlrd.open_workbook(
                file_contents=self._data_to_process,
                ignore_workbook_corruption=True,
            )
            self.data = pd.read_excel(workbook)

        self._correct_column_names()

        self._delete_data()
        self._correct_data()

    def _correct_column_names(self):
        self._rename_columns_manually()

    @abstractmethod
    def _rename_columns_manually(self):
        pass

    @abstractmethod
    def _delete_data(self):
        pass

    def _correct_data(self):
        self._correct_boolean_values()
        self._correct_na_values_for_database()

    def _correct_boolean_values(self):
        boolean_columns = [
            "freight",
            "passenger",
            "infrastructure",
            "holding",
            "integrated",
            "other",
        ]
        for column in boolean_columns:
            self.data[column] = self.data[column].apply(lambda x: x == "x" or x == "X")

    def _correct_na_values_for_database(self):
        self.data.replace(
            to_replace={
                pd.NA: None,
                pd.NaT: None,
            },
            inplace=True,
        )
