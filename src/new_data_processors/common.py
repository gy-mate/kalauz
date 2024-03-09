from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO
import logging
from typing import Any, ClassVar, Final

import numpy as np
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
import pandas as pd
import requests
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

    @abstractmethod
    def process_data(self) -> None:
        pass

    def store_data(self) -> None:
        self._create_table_if_not_exists()
        self._add_data()

    def _create_table_if_not_exists(self) -> None:
        self.table.create(
            bind=self.database.engine,
            checkfirst=True,
        )
        self.logger.debug(f"Table `{self.TABLE_NAME}` sucessfully created (if needed)!")

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
        self.logger.debug(f"File successfully downloaded from {url}!")
        return bytes(response.content)


class UICTableUpdater(DataDownloader, ABC):
    def __init__(self) -> None:
        super().__init__()

        self.DATA_BASE_URL: Final = "https://uic.org/spip.php?action=telecharger&arg="

        self._data_to_process: bytes = NotImplemented


class ExcelProcessor(TableUpdater, ABC):
    def __init__(self) -> None:
        super().__init__()

        self.data: pd.DataFrame = NotImplemented

    def process_data(self) -> None:
        self._import_data()

        self._correct_column_names()

        self._delete_data()
        self._correct_data()

    @abstractmethod
    def _import_data(self) -> None:
        pass

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

    @abstractmethod
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
                # TODO: remove the line below in production
                NotImplemented: None,
            },
            inplace=True,
        )


class ExcelSimpleProcessor(ExcelProcessor, ABC):
    def __init__(self) -> None:
        super().__init__()

        self._data_to_process: bytes = NotImplemented

    def _import_data(self) -> None:
        try:
            # future: remove the line below when https://youtrack.jetbrains.com/issue/PY-70308/ is fixed
            # noinspection PyTypeChecker
            self.data = pd.read_excel(BytesIO(self._data_to_process))
        except xlrd.compdoc.CompDocError:
            workbook = xlrd.open_workbook(
                file_contents=self._data_to_process,
                ignore_workbook_corruption=True,
            )
            self.data = pd.read_excel(workbook)

    def _correct_data_manually(self) -> None:
        pass


class ExcelDeepProcessor(ExcelProcessor, ABC):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self._file_to_be_imported: str = NotImplemented
        self._data_to_process: list[Worksheet] = NotImplemented

    def _import_data(self) -> None:
        self._data_to_process = self._get_worksheets(self._file_to_be_imported)

    def _get_worksheets(self, xlsx_file_location: str) -> list[Worksheet]:
        try:
            self.logger.info(f"Loading {xlsx_file_location} started!")
            xlsx_workbook = load_workbook(
                filename=xlsx_file_location,
                data_only=True,
            )
            self.logger.info(f"{xlsx_file_location} loaded!")
            return list(xlsx_workbook.worksheets)
        finally:
            self.logger.info(f"All worksheets imported from {xlsx_file_location}!")

    def _rename_columns_manually(self) -> None:
        pass
