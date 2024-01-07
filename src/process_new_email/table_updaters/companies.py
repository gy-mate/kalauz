from abc import ABC
from datetime import datetime
import re
from typing import final

from sqlalchemy import text

from src.process_new_email.table_updaters.common import ExcelProcessor, UICTableUpdater


@final
# TODO: add logging
class CompaniesUpdater(ExcelProcessor, UICTableUpdater, ABC):
    def __init__(self) -> None:
        super().__init__()

        self.DATA_URL = f"{self.DATA_BASE_URL}3023"

        self._data_to_process = self.download_data(self.DATA_URL)

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def _correct_column_names(self):
        self._replace_nonword_with_underscore()
        self._rename_columns_manually()

    def _replace_nonword_with_underscore(self):
        # TODO: report wrong display of newlines in DataFrame view to pandas developers
        self.data.rename(
            columns=lambda x: re.sub(
                pattern=r"[\n ]",
                repl="_",
                string=x,
            ).lower(),
            inplace=True,
        )

    def _rename_columns_manually(self):
        self.data.rename(
            columns={
                "code": "UIC_code",
                "full_name": "name",
                "country": "country_ISO_code",
                "infra-_structure": "infrastructure",
                "url": "URL",
            },
            inplace=True,
        )

    def _delete_data(self):
        self._remove_invalid_and_irrelevant_companies()
        self._drop_unnecessary_columns()
        self._remove_invalid_companies()

    def _remove_invalid_and_irrelevant_companies(self):
        self.data.dropna(
            subset=[
                "allocation_date",
                "infrastructure",
            ],
            inplace=True,
        )

    def _drop_unnecessary_columns(self):
        self.data.drop(
            columns=[
                "request_date",
                "recent",
            ],
            inplace=True,
        )

    def _remove_invalid_companies(self):
        # noinspection PyUnusedLocal
        today = datetime.today()
        self.data.query(
            expr="(begin_of_validity.isnull() or begin_of_validity <= @today) and (end_of_validity.isnull() or @today <= end_of_validity)",
            inplace=True,
        )

    def store_data(self) -> None:
        with self.database.engine.begin() as connection:
            query = """
            create table if not exists companies (
                UIC_code int(4) not null,
                short_name varchar(255),
                name varchar(255) not null,
                country_ISO_code varchar(2) not null,
                allocation_date date,
                modified_date date,
                begin_of_validity date,
                end_of_validity date,
                freight boolean not null,
                passenger boolean not null,
                infrastructure boolean not null,
                holding boolean not null,
                integrated boolean not null,
                other boolean not null,
                URL varchar(255),
                
                index (UIC_code),
                primary key (UIC_code),
                foreign key (country_ISO_code) references countries(ISO_code)
            )
            """

            connection.execute(text(query))

        with self.database.engine.begin() as connection:
            for index, row in self.data.iterrows():
                queries = [
                    """
                insert ignore into companies (
                    UIC_code,
                    short_name,
                    name,
                    country_ISO_code,
                    allocation_date,
                    modified_date,
                    begin_of_validity,
                    end_of_validity,
                    freight,
                    passenger,
                    infrastructure,
                    holding,
                    integrated,
                    other,
                    URL
                )
                values (
                    :UIC_code,
                    :short_name,
                    :name,
                    :country_ISO_code,
                    :allocation_date,
                    :modified_date,
                    :begin_of_validity,
                    :end_of_validity,
                    :freight,
                    :passenger,
                    :infrastructure,
                    :holding,
                    :integrated,
                    :other,
                    :URL
                )
                """,
                    """
                update companies
                set
                    short_name = :short_name,
                    name = :name,
                    country_ISO_code = :country_ISO_code,
                    allocation_date = :allocation_date,
                    modified_date = :modified_date,
                    begin_of_validity = :begin_of_validity,
                    end_of_validity = :end_of_validity,
                    freight = :freight,
                    passenger = :passenger,
                    infrastructure = :infrastructure,
                    holding = :holding,
                    integrated = :integrated,
                    other = :other,
                    URL = :URL
                where
                    UIC_code = :UIC_code
                """,
                ]

                for query in queries:
                    connection.execute(
                        text(query),
                        row.to_dict(),
                    )
