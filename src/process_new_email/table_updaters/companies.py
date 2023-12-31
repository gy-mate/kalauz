from abc import ABC
import re

import pandas as pd

from src.process_new_email.table_updaters.common import HelperTableUpdater


class CompaniesUpdater(HelperTableUpdater, ABC):
    def __init__(self, database, data_url) -> None:
        super().__init__(database, data_url)

        self.data: pd.DataFrame = pd.DataFrame()

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def process_data(self) -> None:
        # TODO: report bug to JetBrains
        # noinspection PyTypeChecker
        self.data = pd.read_excel(self._data_to_process)

        # TODO: report wrong display of newlines in DataFrame view to pandas developers
        self.data.rename(
            columns=lambda x: re.sub(
                r"[\n ]",
                "_",
                x,
            ).lower(),
            inplace=True,
        )
        self.data.rename(
            columns={"infra-_structure": "infrastructure"},
            inplace=True,
        )

        self.data.dropna(
            subset=[
                "allocation_date",
                "infrastructure",
            ],
            inplace=True,
        )
        self.data.drop(
            columns=[
                "request_date",
                "recent",
            ],
            inplace=True,
        )
        pass

    def store_data(self) -> None:
        pass
