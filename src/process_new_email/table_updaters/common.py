from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
import logging
from typing import Any, Final

import numpy as np
import pandas as pd
import requests
from sqlalchemy import MetaData

# future: remove the comment below when stubs for the library below are available
import xlrd  # type: ignore

from src.process_new_email.database_connection import Database


class TableUpdater(ABC):
    TABLE_NAME: str = NotImplemented
    database_metadata: MetaData = NotImplemented

    def __init__(self) -> None:
        super().__init__()

        self.logger = logging.getLogger(__name__)
        self.database = Database()

        self.DATA_URL: str = NotImplemented

        self.data: Any = NotImplemented
        self._data_to_process: bytes = NotImplemented

    @abstractmethod
    def process_data(self) -> None:
        pass

    def store_data(self) -> None:
        self._create_table_if_not_exists()
        self._add_data()

    @abstractmethod
    def _create_table_if_not_exists(self) -> None:
        pass

    @abstractmethod
    def _add_data(self) -> None:
        pass


class DataDownloader(TableUpdater, ABC):
    def __init__(self) -> None:
        super().__init__()
        
        self._dowload_session = requests.Session()

    def get_data(self, url: str) -> bytes:
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


class UICTableUpdater(DataDownloader, ABC):
    def __init__(self) -> None:
        super().__init__()

        self.DATA_BASE_URL: Final = "https://uic.org/spip.php?action=telecharger&arg="


class ExcelProcessor(TableUpdater, ABC):
    TODAY = datetime.today().date()

    def __init__(self) -> None:
        super().__init__()

        self.data: pd.DataFrame = NotImplemented

    def process_data(self) -> None:
        try:
            # future: remove the line below when https://youtrack.jetbrains.com/issue/PY-55260/ is fixed
            # noinspection PyTypeChecker
            # future: report bug (false positive) to mypy developers
            self.data = pd.read_excel(BytesIO(self._data_to_process))  # type: ignore
        except xlrd.compdoc.CompDocError:
            workbook = xlrd.open_workbook(
                file_contents=self._data_to_process,
                ignore_workbook_corruption=True,
            )
            self.data = pd.read_excel(workbook)

        self._correct_column_names()

        self._delete_data()
        self._correct_data()

    def _correct_column_names(self) -> None:
        self._rename_columns_manually()

    @abstractmethod
    def _rename_columns_manually(self) -> None:
        pass

    def _delete_data(self) -> None:
        pass

    def _correct_data(self) -> None:
        self._correct_data_manually()
        self._correct_boolean_values()
        self._correct_na_values_for_database()

    def _correct_data_manually(self) -> None:
        pass

    @abstractmethod
    def _correct_boolean_values(self) -> None:
        pass

    def _correct_na_values_for_database(self) -> None:
        self.data.replace(
            to_replace={
                pd.NA: None,
                pd.NaT: None,
                # future: remove the line below when https://github.com/pandas-dev/pandas/issues/32265 is fixed
                np.NaN: None,
            },
            inplace=True,
        )
