from datetime import datetime
import re
from typing import ClassVar, final

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    ForeignKey,
    MetaData,
    SmallInteger,
    String,
    Table,
    text,
)

from src.new_data_processors.common import (
    UICTableUpdater,
)
from src.new_data_processors.common_excel_processors import ExcelProcessorSimple
from src.new_data_processors.helper_table_updaters.countries import CountriesUpdater


@final
# TODO: add logging
class CompaniesUpdater(ExcelProcessorSimple, UICTableUpdater):
    TABLE_NAME: ClassVar[str] = "companies"
    database_metadata: ClassVar[MetaData] = MetaData()

    table: ClassVar[Table] = Table(
        TABLE_NAME,
        database_metadata,
        Column(
            name="code_uic",
            type_=SmallInteger,
            nullable=False,
            index=True,
            primary_key=True,
        ),
        Column(name="short_name", type_=String(255)),
        Column(name="name", type_=String(255), nullable=False),
        Column(
            "country_code_iso",
            String(2),
            ForeignKey(CountriesUpdater.table.c.code_iso),
            nullable=False,
        ),
        Column(name="allocation_date", type_=Date),
        Column(name="modified_date", type_=Date),
        Column(name="begin_of_validity", type_=Date),
        Column(name="end_of_validity", type_=Date),
        Column(name="freight", type_=Boolean, nullable=False),
        Column(name="passenger", type_=Boolean, nullable=False),
        Column(name="infrastructure", type_=Boolean, nullable=False),
        Column(name="holding", type_=Boolean),
        Column(name="integrated", type_=Boolean),
        Column(name="other", type_=Boolean, nullable=False),
        Column(name="url", type_=String(255)),
    )

    def __init__(self) -> None:
        super().__init__()

        self.DATA_URL = f"{self.DATA_BASE_URL}3023"

        self._data_to_process = self.get_data(self.DATA_URL)

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
