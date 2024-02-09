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
    Time,
)

from src.process_new_email.table_updaters.common import (
    ExcelProcessor,
)
from src.process_new_email.table_updaters.companies import CompaniesUpdater
from src.process_new_email.table_updaters.countries import CountriesUpdater


class SRUpdater(ExcelProcessor, ABC):
    TABLE_NAME: ClassVar[str] = "speed_restrictions"
    database_metadata: ClassVar[MetaData] = MetaData()

    # TODO: estabilish one-to-many relationships between SRs and switches
    table: ClassVar[Table] = Table(
        TABLE_NAME,
        database_metadata,
        Column(
            "country_iso",
            String(2),
            ForeignKey(CountriesUpdater.table.c.code_iso),
            nullable=False,
        ),
        Column(
            "company_uic",
            SmallInteger,
            ForeignKey(CompaniesUpdater.table.c.code_uic),
            nullable=False,
        ),
        Column(name="internal_id", type_=String(255)),
        Column(name="in_timetable", type_=Boolean, nullable=False),
        Column(name="due_to_railway_features", type_=Boolean, nullable=False),
        Column(name="line", type_=String(255), nullable=False),
        Column(name="metre_post_from", type_=Integer, nullable=False),
        Column(name="metre_post_to", type_=Integer, nullable=False),
        Column(name="open_line_tracks_left_or_right", type_=Boolean),
        Column(name="station_from", type_=String(255), nullable=False),
        Column(name="station_to", type_=String(255)),
        Column(name="on_main_track", type_=Boolean, nullable=False),
        Column(name="station_track_switch_source_text", type_=String(255)),
        Column(name="station_track_switch_from", type_=String(255)),
        Column(name="station_track_switch_to", type_=String(255)),
        Column(name="operating_speed", type_=Integer, nullable=False),
        Column(name="reduced_speed", type_=Integer, nullable=False),
        Column(name="reduced_speed_for_mus", type_=Integer),
        Column(name="cause", type_=String(255)),
        Column(name="date_from", type_=Date, nullable=False),
        Column(name="time_from", type_=Time),
        Column(name="maintenance_planned", type_=Boolean),
        Column(name="date_to", type_=Date),
        Column(name="time_to", type_=Time),
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

        # future: delete `if` section below in production
        if self.COMPANY == "MÁV":
            self.TODAY = date(2023, 7, 26)
        elif self.COMPANY == "GYSEV":
            self.TODAY = date(2023, 8, 4)
        else:
            raise ValueError(f"Unknown company: {self.COMPANY}!")

        self._file_to_be_imported = f"data/01_import/{self.COMPANY}_{self.TODAY}_{self.LIST_TYPE}.{self.SOURCE_EXTENSION}"

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
