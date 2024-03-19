from abc import ABC, abstractmethod
from io import BytesIO

import numpy as np
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

import pandas as pd

# future: remove the comment below when stubs for the library below are available
import xlrd  # type: ignore

from src.new_data_processors.common import TableUpdater


class ExcelProcessor(TableUpdater, ABC):
    def __init__(self) -> None:
        super().__init__()

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
        self._correct_df_na_values_for_database()

    @abstractmethod
    def _correct_data_manually(self) -> None:
        pass

    @abstractmethod
    def _correct_boolean_values(self) -> None:
        pass

    @abstractmethod
    def _correct_df_na_values_for_database(self) -> None:
        pass


class ExcelProcessorSimple(ExcelProcessor, ABC):
    def __init__(self) -> None:
        super().__init__()

        self._data_to_process: bytes = NotImplemented

    def _import_data(self) -> None:
        try:
            # future: remove the line below when https://youtrack.jetbrains.com/issue/PY-70308/ is fixed
            # noinspection PyTypeChecker
            self.data = pd.read_excel(BytesIO(self._data_to_process))
        except xlrd.compdoc.CompDocError:
            self.logger.debug("Excel data is corrupted, ignoring it...")
            workbook = xlrd.open_workbook(
                file_contents=self._data_to_process,
                ignore_workbook_corruption=True,
            )
            self.data = pd.read_excel(workbook)

    def _correct_data_manually(self) -> None:
        pass

    def _correct_df_na_values_for_database(self) -> None:
        self.data.replace(
            to_replace={
                pd.NA: None,
                pd.NaT: None,
                np.NaN: None,  # future: remove this line when https://github.com/pandas-dev/pandas/issues/32265 is fixed
                NotImplemented: None,  # TODO: remove this line in production
            },
            inplace=True,
        )


class ExcelProcessorWithFormatting(ExcelProcessor, ABC):
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

    def _correct_df_na_values_for_database(self) -> None:
        pass
