from datetime import datetime
from io import BytesIO
import re
from typing import final

import pandas as pd
from sqlalchemy import text

from src.process_new_email.table_updaters.common import UICTableUpdater


@final
# FIXME: extract methods as needed
class CompaniesUpdater(UICTableUpdater):
    def __init__(self) -> None:
        super().__init__()

        self.DATA_URL = f"{self.DATA_BASE_URL}3023"

        self._data_to_process: BytesIO = super().download_data(self.DATA_URL)

        self.logger.info(f"{self.__class__.__name__} initialized!")

    def process_data(self) -> None:
        # future: remove the line below when https://youtrack.jetbrains.com/issue/PY-55260/ is fixed
        # noinspection PyTypeChecker
        self.data = pd.read_excel(self._data_to_process)

        # TODO: report wrong display of newlines in DataFrame view to pandas developers
        self.data.rename(
            columns=lambda x: re.sub(
                pattern=r"[\n ]",
                repl="_",
                string=x,
            ).lower(),
            inplace=True,
        )
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

        # noinspection PyUnusedLocal
        today = datetime.today()
        self.data.query(
            expr="(begin_of_validity.isnull() or begin_of_validity <= @today) and (end_of_validity.isnull() or @today <= end_of_validity)",
            inplace=True,
        )

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

        self.data.replace(
            to_replace={
                pd.NA: None,
                pd.NaT: None,
            },
            inplace=True,
        )

    def store_data(self) -> None:
        with self.engine.begin() as connection:
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

        with self.engine.begin() as connection:
            for index, row in self.data.iterrows():
                # noinspection PyListCreation
                queries = []

                queries.append(
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
                """
                )
                queries.append(
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
                """
                )

                for query in queries:
                    connection.execute(
                        text(query),
                        row.to_dict(),
                    )
