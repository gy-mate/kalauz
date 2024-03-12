from abc import ABC
from datetime import date
from typing import ClassVar

# future: remove the comment below when stubs for the library below are available
import roman  # type: ignore
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    ForeignKey,
    Integer,
    MetaData,
    SmallInteger,
    String,
    Table,
    text,
)

from src.new_data_processors.common import (
    ExcelProcessor,
)
from src.new_data_processors.helper_table_updaters.companies import CompaniesUpdater
from src.new_data_processors.helper_table_updaters.countries import CountriesUpdater


class SRUpdater(ExcelProcessor, ABC):
    TABLE_NAME: ClassVar[str] = "speed_restrictions"
    database_metadata: ClassVar[MetaData] = MetaData()

    # TODO: estabilish one-to-many relationships between SRs and switches
    table: ClassVar[Table] = Table(
        TABLE_NAME,
        database_metadata,
        Column(
            name="id", type_=String(255), nullable=False, index=True, primary_key=True
        ),
        Column(
            "country_code_iso",
            String(2),
            ForeignKey(CountriesUpdater.table.c.code_iso),
            nullable=False,
        ),
        Column(
            "company_code_uic",
            SmallInteger,
            ForeignKey(CompaniesUpdater.table.c.code_uic),
            nullable=False,
        ),
        Column(name="internal_id", type_=String(255)),
        Column(name="decision_id", type_=String(255)),
        Column(name="in_timetable", type_=Boolean, nullable=False),
        Column(name="due_to_railway_features", type_=Boolean, nullable=False),
        Column(name="line", type_=String(255), nullable=False),
        Column(name="metre_post_from", type_=Integer, nullable=False),
        Column(name="metre_post_to", type_=Integer, nullable=False),
        Column(name="station_from", type_=String(255), nullable=False),
        Column(name="station_to", type_=String(255)),
        Column(name="on_main_track", type_=Boolean, nullable=False),
        Column(name="main_track_side", type_=String(255)),
        Column(name="station_track_switch_source_text", type_=String(255)),
        Column(name="station_track_from", type_=String(255)),
        Column(name="station_switch_from", type_=String(255)),
        Column(name="station_switch_to", type_=String(255)),
        Column(name="operating_speed", type_=Integer, nullable=False),
        Column(name="reduced_speed", type_=Integer, nullable=False),
        Column(name="reduced_speed_for_mus", type_=Integer),
        Column(name="not_signalled_from_start_point", type_=Boolean, nullable=False),
        Column(name="not_signalled_from_end_point", type_=Boolean, nullable=False),
        Column(name="cause_source_text", type_=String(255)),
        Column(name="cause_category_1", type_=String(255)),
        Column(name="cause_category_2", type_=String(255)),
        Column(name="cause_category_3", type_=String(255)),
        Column(name="time_from", type_=Date, nullable=False),
        Column(name="maintenance_planned", type_=Boolean),
        Column(name="time_to", type_=Date),
        Column(name="work_to_be_done", type_=String(255)),
        Column(name="comment", type_=String(255)),
    )

    def __init__(self, company: str, source_extension: str) -> None:
        super().__init__()

        self.COMPANY = company
        self.COMPANY_CODE_UIC = self._get_company_uic_code(self.COMPANY)
        self.COUNTRY_CODE_ISO = self._get_country_iso_code(self.COMPANY)
        self.LIST_TYPE = "ASR"
        self.SOURCE_EXTENSION = source_extension

        # TODO: implement company name detection from filename
        # future: delete `match` section below in production
        match self.COMPANY:
            case "MÁV":
                self.TODAY = date(2024, 1, 18)
            case "GySEV/Raaberbahn":
                self.TODAY = date(2023, 5, 13)
            case _:
                raise ValueError(f"Unknown company: {self.COMPANY}!")

        self._file_to_be_imported = f"data/02_converted/{self.COMPANY}_{self.TODAY}_{self.LIST_TYPE}.{self.SOURCE_EXTENSION}"

    def _get_company_uic_code(self, company: str) -> int:
        with self.database.engine.begin() as connection:
            query = """
                select code_uic
                from companies
                where short_name = :company
            """
            result = connection.execute(
                text(query),
                {"company": company},
            ).fetchone()

            try:
                assert result is not None
            except AssertionError as exception:
                self.logger.critical(exception)
                raise
            return int(result[0])

    def _get_country_iso_code(self, company: str) -> str:
        with self.database.engine.begin() as connection:
            query = """
                select country_code_iso
                from companies
                where short_name = :company
            """
            result = connection.execute(
                text(query),
                {"company": company},
            ).fetchone()

            try:
                assert result is not None
            except AssertionError as exception:
                self.logger.critical(exception)
                raise
            return result[0]
