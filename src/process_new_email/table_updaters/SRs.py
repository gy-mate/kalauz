from abc import ABC
from datetime import date, datetime
import re
from zoneinfo import ZoneInfo

from openpyxl.cell import Cell
from pandas import DataFrame

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

from src.process_new_email.table_updaters.common import (
    ExcelDeepProcessor,
    ExcelProcessor,
)
from src.process_new_email.table_updaters.companies import CompaniesUpdater
from src.process_new_email.table_updaters.countries import CountriesUpdater


class SRUpdater(ExcelProcessor, ABC):
    TABLE_NAME = "speed_restrictions"
    database_metadata = MetaData()

    table = Table(
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
        Column(name="station_uic_from", type_=Integer, nullable=False),
        Column(name="station_uic_to", type_=Integer),
        Column(name="station_track_switch_source_text", type_=String(255)),
        Column(name="station_track_switch_from", type_=String(255)),
        Column(name="station_track_switch_to", type_=String(255)),
        Column(name="operating_speed", type_=Integer, nullable=False),
        Column(name="reduced_speed", type_=Integer, nullable=False),
        Column(name="reduced_speed_for_mus", type_=Integer),
        Column(name="cause", type_=String(255)),
        Column(name="time_from", type_=Date, nullable=False),
        Column(name="maintenance_planned", type_=Boolean),
        Column(name="time_to", type_=Date),
        Column(name="work_to_be_done", type_=String(255)),
        Column(name="comment", type_=String(255)),
    )

    def __init__(self, company: str, source_extension: str) -> None:
        super().__init__()

        self.COMPANY = company
        self.COMPANY_UIC_CODE = self._get_company_uic_code(self.COMPANY)
        self.LIST_TYPE = "ASR"
        self.SOURCE_EXTENSION = source_extension

        # future: delete `if` section below in production
        if self.COMPANY == "MÁV":
            self.TODAY = date(2023, 7, 26)
        elif self.COMPANY == "GYSEV":
            self.TODAY = date(2023, 8, 4)

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


def _is_tsr(cell: Cell) -> bool:
    return cell.font.bold


def _convert_cells_to_values(row: list[Cell]) -> list:
    new_row: list = []
    for cell in row:
        row.append(cell)
    return new_row


def _header_or_footer(row: list) -> bool:
    return "Vonal" in str(row[0]) or "Összes korlátozás:" in str(row[1])


def remove_space_after_hyphen(data: str) -> str:
    return re.sub(r"(?<=\w)- (?=\w)", "-", str(data))


def get_station_uic(country_uic, station_name):
    """
    Receives:
    the country's UIC code;
    a station name.

    Gets the station's non-standard UIC code from the station database.
    If not found, asks for an input.
    Replaces 'HU' with the UIC country code from its beginning.

    Returns the station's UIC code as an int.
    """
    try:
        return int(
            str(country_uic)
            + re.search(r"(?<=HU).*", station_database[station_name])[0]
        )
    except KeyError:
        self.logger.warning(
            f"Error: {station_name} could not be converted to UIC code!"
        )
        # return int(input(f'Please input {station_name}\'s UIC code!'))


def extract_number(data):
    """
    Receives a data.

    Converts the data to a str.
    Initializes *regex search expressions* for arabic and roman numbers with several combinations that could be found in the database.

    Searches for an _arabic number._
    If found, returns it as an int.
    If not found, searches for a _roman number._

    If found, converts it to an _arabic number_ via `fromroman` and returns it as an int.
    If this doesn't succeed, returns an `InvalidRomanNumeralError`.
    If not found, searches for an _arabic number with letters._

    If found, returns it as an int.
    If not found, returns a 'TypeError'.
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

    text = str(data)

    try:
        if arabic_regex.search(text) is not None:
            return int(arabic_regex.search(text)[0])
        else:
            if roman_mix_regex.search(text) is not None:
                if roman_mix_regex.search(text)[0]:
                    return int(roman.fromRoman(roman_mix_regex.search(text)[0]))
                else:
                    return arabic_letter_regex.search(text)[0]
            else:
                return arabic_letter_regex.search(text)[0]
    except TypeError:
        return "TypeError"
    except roman.InvalidRomanNumeralError:
        return "InvalidRomanNumeralError"


def _get_operating_speed(text_to_search: str) -> int:
    return _get_number_between_brackets(text_to_search)


def _get_number_between_brackets(text_to_search):
    return round(int(re.findall(r"(?<=\().*(?=\))", text_to_search)[0]))


def get_reduced_speeds(text_to_search : str) -> int | tuple[int, int]:
    """
    Receives a text.

    Returns the reduced speeds in km/h for MUs and locomotives.
    """
    if text_to_search.find("/") == -1:
        return round(int(re.findall(r".*(?= \()", text_to_search)[0])) * 2
    else:
        return round(int(re.findall(r".*(?=/)", text_to_search)[0])), round(
            int(re.findall(r"(?<=/).*(?= )", text_to_search)[0])
        )


def convert_date_to_iso(text_to_search: str) -> datetime:
    """
    Receives a date (and time) as a str.
    Returns the text converted to ISO format.
    """
    if type(text_to_search) is str:
        try:
            return (
                datetime.strptime(text_to_search, "%Y.%m.%d %H:%M")
                .replace(tzinfo=ZoneInfo(key="Europe/Budapest"))
            )
        except TypeError:
            return "TypeError"
        except ValueError:
            return convert_date_to_iso(re.findall(r".*(?=-)", text_to_search)[0])
    elif type(text_to_search) is datetime:
        try:
            return text_to_search.date()
        except TypeError:
            return "TypeError"
        except ValueError:
            return "ValueError"


class MavUpdater(SRUpdater, ExcelDeepProcessor):
    def __init__(self) -> None:
        super().__init__(
            company="MÁV",
            source_extension="xlsx",
        )

    def _correct_data_manually(self) -> None:
        # future: remove the comment below when https://youtrack.jetbrains.com/issue/PY-62633 is fixed
        # noinspection PyTypeChecker
        columns = [column.name for column in self.table.columns]
        self.data = DataFrame(columns=columns)
        for worksheet in self._data_to_process:
            for row_of_cells in [list(cell) for cell in worksheet.iter_rows()]:
                is_temporary = _is_tsr(row_of_cells[0])
                row = [cell.value for cell in row_of_cells]

                if not _header_or_footer(row):
                    try:
                        assert isinstance(row[5], int)
                    except AssertionError:
                        self.logger.critical(
                            f"Error: The value of `metre_post_from` ({row[5]}) could not be converted to int!"
                        )
                        raise

                    metre_post_to: int | None = None
                    try:
                        assert isinstance(row[6], int)
                        metre_post_to = int(row[6] * 100)
                    except AssertionError:
                        pass
                    
                    try:
                        assert isinstance(row[8], str)
                    except AssertionError:
                        self.logger.critical(
                            f"Error: The value of `operating_speed` ({row[8]}) could not be converted to str!"
                        )
                        raise
                    
                    reduced_speed, reduced_speed_for_mus = get_reduced_speeds(row[8])

                    row_to_add = {
                        "country_iso": "HU",
                        "company_uic": self.COMPANY_UIC_CODE,
                        "internal_id": None,
                        "in_timetable": not is_temporary,
                        "due_to_railway_features": None,
                        "line": row[0],
                        "metre_post_from": int(row[5] * 100),
                        "metre_post_to": metre_post_to,
                        "open_line_tracks_left_or_right": None,
                        "station_uic_from": None,
                        "station_uic_to": None,
                        "station_track_switch_source_text": row[4],
                        "station_track_switch_from": None,
                        "station_track_switch_to": None,
                        "operating_speed": _get_operating_speed(row[8]),
                        "reduced_speed": None,
                        "reduced_speed_for_mus": None,
                        "cause": row[12],
                        "time_from": convert_date_to_iso(row[11]),
                        "maintenance_planned": None,
                        "time_to": None,
                        "work_to_be_done": None,
                        "comment": row[14],
                    }

                    current_sr.station_from_name = remove_space_after_hyphen(row[1])
                    current_sr.station_from_uic = get_station_uic(
                        current_sr.country_uic, current_sr.station_from_name
                    )

                    current_sr.on_station = not row[2]  # true if cell is empty
                    if current_sr.on_station:
                        if "kitérő" in str(row[4]):
                            current_sr.applied_to = "switch"
                            current_sr.switch_station = extract_number(row[4])
                        else:
                            current_sr.applied_to = "track"
                            current_sr.track_station = extract_number(row[4])
                    else:
                        station_to_name = remove_space_after_hyphen(row[2])
                        current_sr.station_to_uic = get_station_uic(
                            current_sr.country_uic, station_to_name
                        )
                        current_sr.applied_to = "track"
                        if row[3] is None:
                            current_sr.number_of_open_line_tracks = 1
                        elif row[3] == "bal vágány":
                            current_sr.number_of_open_line_tracks = 2
                            current_sr.track_open_line = "left"
                        elif row[3] == "jobb vágány":
                            current_sr.number_of_open_line_tracks = 2
                            current_sr.track_open_line = "right"
                        else:
                            current_sr.number_of_open_line_tracks = (
                                current_sr.track_open_line
                            ) = "UnknownError"

                    current_sr.time_to = (
                        convert_date_to_iso(row[13]) if row[13] else None
                    )

    def _correct_boolean_values(self) -> None:
        pass

    def _create_table_if_not_exists(self) -> None:
        pass

    def _add_data(self) -> None:
        pass


class GysevUpdater(SRUpdater):
    def __init__(self) -> None:
        super().__init__(
            company="GYSEV",
            source_extension="xlsx",
        )

    def _get_data(self) -> None:
        pass

    def _import_data(self) -> None:
        pass

    def _correct_data_manually(self) -> None:
        pass

    def _rename_columns_manually(self) -> None:
        pass

    def _correct_boolean_values(self) -> None:
        pass

    def _create_table_if_not_exists(self) -> None:
        pass

    def _add_data(self) -> None:
        pass
