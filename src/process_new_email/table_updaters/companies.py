from abc import ABC

import pandas

from src.process_new_email.table_updaters.common import HelperTableUpdater


class CompaniesUpdater(HelperTableUpdater, ABC):
    def __init__(self, database_connection, data_url) -> None:
        super().__init__(database_connection, data_url)

        self.data: pandas.DataFrame = pandas.DataFrame()

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def process_data(self) -> None:
        self.data = pandas.read_excel(self._DATA_TO_PROCESS)
        self.data.dropna(subset=["allocation date"], inplace=True)
        self.data.drop(
            columns=[
                "request date",
                "begin of validity",
                "end of validity",
                "recent",
            ],
            inplace=True,
        )

    def store_data(self) -> None:
        self.data.to_sql(
            name="companies",
            self.database.engine,
            if_exists="replace",
            index=False,
            method="multi",
        )
