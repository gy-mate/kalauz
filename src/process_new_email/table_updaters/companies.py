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
        self.TABLE_NAME = "companies"

        self._data_to_process = self.download_data(self.DATA_URL)

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def _correct_column_names(self) -> None:
        self._replace_nonword_with_underscore()
        self._rename_columns_manually()

    def _replace_nonword_with_underscore(self) -> None:
        # TODO: report wrong display of newlines in DataFrame view to pandas developers
        self.data.rename(
            columns=lambda x: re.sub(
                pattern=r"[\n ]",
                repl="_",
                string=x,
            ).lower(),
            inplace=True,
        )

    def _rename_columns_manually(self) -> None:
        self.data.rename(
            columns={
                "code": "code_uic",
                "full_name": "name",
                "country": "country_code_iso",
                "infra-_structure": "infrastructure",
                "url": "url",
            },
            inplace=True,
        )

    def _delete_data(self) -> None:
        self._remove_invalid_and_irrelevant_companies()
        self._drop_unnecessary_columns()
        self._remove_invalid_companies()

    def _remove_invalid_and_irrelevant_companies(self) -> None:
        self.data.dropna(
            subset=[
                "allocation_date",
                "infrastructure",
            ],
            inplace=True,
        )

    def _drop_unnecessary_columns(self) -> None:
        self.data.drop(
            columns=[
                "request_date",
                "recent",
            ],
            inplace=True,
        )

    def _remove_invalid_companies(self) -> None:
        # future: report bug below to JetBrains or pandas developers
        # noinspection PyUnusedLocal
        today = datetime.today()
        self.data.query(
            expr="(begin_of_validity.isnull() or begin_of_validity <= @today) and (end_of_validity.isnull() or @today <= end_of_validity)",
            inplace=True,
        )

    def _correct_boolean_values(self) -> None:
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

    def _create_table_if_not_exists(self) -> None:
        with self.database.engine.begin() as connection:
            query = """
            create table if not exists :table_name (
                code_uic int(4) not null,
                short_name varchar(255),
                name varchar(255) not null,
                country_code_iso varchar(2) not null,
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
                url varchar(255),
                
                index (code_uic),
                primary key (code_uic),
                foreign key (country_code_iso) references countries(code_iso)
            )
            """
            connection.execute(
                text(query),
                {"table_name": self.TABLE_NAME},
            )

    def _add_data(self) -> None:
        with self.database.engine.begin() as connection:
            queries = [
                """
                insert ignore into companies (
                    code_uic,
                    short_name,
                    name,
                    country_code_iso,
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
                    url
                )
                values (
                    :code_uic,
                    :short_name,
                    :name,
                    :country_code_iso,
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
                    :url
                )
                """,
                """
                update companies
                set
                    short_name = :short_name,
                    name = :name,
                    country_code_iso = :country_code_iso,
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
                    url = :url
                where
                    code_uic = :code_uic
                """,
            ]

            for index, row in self.data.iterrows():
                for query in queries:
                    connection.execute(
                        text(query),
                        row.to_dict(),
                    )
