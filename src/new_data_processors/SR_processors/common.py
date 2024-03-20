from abc import ABC
from datetime import date, datetime
import re
from typing import ClassVar
from zoneinfo import ZoneInfo

# future: remove the comment below when stubs for the library below are available
import roman  # type: ignore
import sqlalchemy
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

from src.SR import SR
from src.new_data_processors.common_excel_processors import ExcelProcessor
from src.new_data_processors.helper_table_updaters.companies import CompaniesUpdater
from src.new_data_processors.helper_table_updaters.countries import CountriesUpdater


def get_end_time(text_to_search: str) -> str:
    return text_to_search[17:22]


def datetime_format_is_dmy(text_to_search) -> bool:
    if re.findall(
        pattern=re.compile(r"\d{1,2}/\d{1,2}/\d{1,2}"),
        string=text_to_search,
    ):
        return True
    else:
        return False


def datetime_format_is_iso(text_to_search) -> bool:
    return len(text_to_search) == 19


def datetime_format_is_yyyymmdd_hhmm(text_to_search) -> bool:
    return len(text_to_search) == 16


def contains_both_start_and_end_time(text_to_search) -> bool:
    return len(text_to_search) in range(21, 22 + 1)


def datetime_format_yyyy(text_to_search) -> bool:
    return len(text_to_search) == 4


def contains_both_start_and_end_years(text_to_search) -> bool:
    if re.findall(
        pattern=re.compile(r"\d{4} ?- ?\d{4}"),
        string=text_to_search,
    ):
        return True
    else:
        return False


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
        Column(name="not_signalled_from_start_point", type_=Boolean),
        Column(name="not_signalled_from_end_point", type_=Boolean),
        Column(name="cause_source_text", type_=String(255)),
        Column(name="cause_category_1", type_=String(255)),
        Column(name="cause_category_2", type_=String(255)),
        Column(name="cause_category_3", type_=String(255)),
        Column(name="time_from", type_=Date, nullable=False),
        Column(name="work_to_be_done", type_=String(255)),
        Column(name="time_to", type_=Date),
        Column(name="comment", type_=String(255)),
    )

    def __init__(self, company: str, source_extension: str) -> None:
        super().__init__()

        self.COMPANY = company
        self.COMPANY_CODE_UIC = self.get_company_uic_code(self.COMPANY)
        self.COUNTRY_CODE_ISO = self.get_country_iso_code(self.COMPANY)
        self.LIST_TYPE = "ASR"
        self.SOURCE_EXTENSION = source_extension

        # TODO: implement company name detection from filename
        # future: delete date mocking below in production
        match self.COMPANY:
            case "MÁV":
                self.TODAY = date(2024, 1, 18)
            case "GYSEV":
                self.TODAY = date(2022, 5, 13)
            case _:
                raise ValueError(f"Unknown company: {self.COMPANY}!")

        self._file_to_be_imported = f"data/02_converted/{self.COMPANY}_{self.TODAY}_{self.LIST_TYPE}.{self.SOURCE_EXTENSION}"

        self.data: list[SR] = NotImplemented
        self.existing_sr_ids = self.get_existing_sr_ids()
        self.current_sr_ids: list[str] = []

    def get_company_uic_code(self, company: str) -> int:
        with self.database.engine.begin() as connection:
            query = """
                select code_uic
                from companies
                where short_name like :company
            """
            result = connection.execute(
                text(query),
                {"company": company + "%"},
            ).fetchone()

            try:
                assert result is not None
            except AssertionError as exception:
                self.logger.critical(exception)
                raise
            return int(result[0])

    def get_country_iso_code(self, company: str) -> str:
        with self.database.engine.begin() as connection:
            query = """
                select country_code_iso
                from companies
                where short_name like :company
            """
            result = connection.execute(
                text(query),
                {"company": company + "%"},
            ).fetchone()

            try:
                assert result is not None
            except AssertionError as exception:
                self.logger.critical(exception)
                raise
            return result[0]
    
    def get_existing_sr_ids(self) -> list[str]:
        with self.database.engine.connect() as connection:
            query = """
            select id
            from speed_restrictions
            """
            try:
                result = connection.execute(
                    text(query),
                ).fetchall()
                return [row[0] for row in result]
            except sqlalchemy.exc.ProgrammingError:
                return []

    def extract_number(self, text_to_search: str) -> int | str:
        """
        Initializes *regex search expressions* for arabic and roman numbers with several combinations that could be found in the database.

        Searches for an _arabic number._
        If found, returns it as an int.
        If not found, searches for a _roman number._

        If found, converts it to an _arabic number_ via `fromroman` and returns it as an int.
        If this doesn't succeed, returns an `InvalidRomanNumeralError`.
        If not found, searches for an _arabic number with letters._
        """
        arabic_regex = re.compile(
            """
            (
            ^|(?<=[ .(])
            )
            
            \\d+
            
            (?=\\D)
            """,
            re.VERBOSE | re.MULTILINE,
        )
        roman_mix_regex = re.compile(
            """
            (
            ^|
            (?<=[ .(])
            )
            
            (
            (M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))|
            (M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))/[a-z]|
            (M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))[a-z]
            )
            
            (?=[ .])
            """,
            re.VERBOSE | re.MULTILINE,
        )
        arabic_letter_regex = re.compile(
            """
            (
            ^|(?<=[ .(])
            )
            
            \\w\\d+
            (?=\\D)
            """,
            re.VERBOSE | re.MULTILINE,
        )

        try:
            if arabic_regex.search(text_to_search):
                result = arabic_regex.search(text_to_search)
                assert result
                return int(result[0])
            else:
                result = roman_mix_regex.search(text_to_search)
                try:
                    assert result
                    return int(roman.fromRoman(result[0]))
                except AssertionError:
                    result = arabic_letter_regex.search(text_to_search)
                    assert result
                    return result[0]
        except AssertionError:
            self.logger.debug(f"Number not found in {text_to_search}!")
            raise
        except roman.InvalidRomanNumeralError:
            self.logger.debug(f"Invalid roman numeral in {text_to_search}!")
            raise

    def get_date(self, text_to_search: str | None) -> str:
        try:
            assert text_to_search
            return text_to_search[:10]
        except AssertionError:
            self.logger.critical(f"`date_from` not found!")
            raise

    def get_utc_time(self, text_to_search: str | None) -> datetime | None:
        try:
            assert text_to_search
            if datetime_format_is_dmy(text_to_search):
                return (
                    datetime.strptime(text_to_search, "%d/%m/%y")
                    .replace(tzinfo=ZoneInfo(key="Europe/Budapest"))
                    .astimezone(ZoneInfo(key="UTC"))
                )
            elif datetime_format_is_iso(text_to_search):
                return (
                    datetime.fromisoformat(text_to_search)
                    .replace(tzinfo=ZoneInfo(key="Europe/Budapest"))
                    .astimezone(ZoneInfo(key="UTC"))
                )
            elif datetime_format_is_yyyymmdd_hhmm(text_to_search):
                return (
                    datetime.strptime(text_to_search, "%Y.%m.%d %H:%M")
                    .replace(tzinfo=ZoneInfo(key="Europe/Budapest"))
                    .astimezone(ZoneInfo(key="UTC"))
                )
            elif contains_both_start_and_end_time(text_to_search):
                return self.get_utc_time(
                    f"{self.get_date(text_to_search)} {get_end_time(text_to_search)}"
                )
            elif datetime_format_yyyy(text_to_search):
                return self.get_utc_time(f"{text_to_search}-12-31 23:59:59")
            elif contains_both_start_and_end_years(text_to_search):
                return self.get_utc_time(text_to_search[:4])
            else:
                return None
        except AssertionError:
            self.logger.critical(f"`time_from` not found!")
            raise
