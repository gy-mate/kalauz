from abc import ABC, abstractmethod
from datetime import date, datetime
import re
from zoneinfo import ZoneInfo

from openpyxl.reader.excel import load_workbook

# future: remove the comment below when stubs for the library below are available
import roman  # type: ignore

from src.process_new_email.table_updaters.SpeedRestriction import SpeedRestriction
from src.process_new_email.table_updaters.common import ExcelProcessor


def check_permanence(cell):
    return cell.font.bold


def should_be_imported(is_it_temporary):
    return is_it_temporary is False


def convert_openpyxl_cells_to_values(row):
    for idx_02, cell in enumerate(row):
        row[idx_02] = cell.value


class SRUpdater(ExcelProcessor, ABC):
    TABLE_NAME = "speed_restrictions"

    def __init__(self, company: str, source_extension: str) -> None:
        super().__init__()

        self.COMPANY = company
        self.LIST_TYPE = "ASR"
        self.SOURCE_EXTENSION = source_extension

        # future: delete if section below in production
        if self.COMPANY == "MÁV":
            self.TODAY = date(2023, 7, 26)
        elif self.COMPANY == "GYSEV":
            self.TODAY = date(2023, 8, 4)

        self._file_to_be_imported = f"data/01_import/{self.COMPANY}_{self.TODAY}_{self.LIST_TYPE}.{self.SOURCE_EXTENSION}"

    @abstractmethod
    def get_data(self) -> None:
        pass


class MavSRUpdater(SRUpdater):
    def __init__(self) -> None:
        super().__init__(
            company="MÁV",
            source_extension="xlsx",
        )

    def get_data(self) -> None:
        current_sr_file_openpyxl = self._import_worksheets(self._file_to_be_imported)

        for excel_worksheet in current_sr_file_openpyxl:
            for row in excel_worksheet.iter_rows():
                row = list(row)  # convert from tuples
                is_it_temporary = check_permanence(row[0])
                if should_be_imported(is_it_temporary):
                    convert_openpyxl_cells_to_values(row)
                    if "Vonal" not in str(row[0]) and "Összes korlátozás:" not in str(
                        row[1]
                    ):  # except headers and footers
                        self.add_data(
                            row, sr_database_of_today, is_it_temporary, station_database
                        )
        self.logger.info(
            f"{self._file_to_be_imported} imported to sr_database_of_today!"
        )

        self.logger.info(
            f"{len(sr_database_of_today)} SRs from all .xlsx files imported to sr_database_of_today!"
        )

    def add_data(self, row, database, in_timetable, station_database):
        def remove_space_after_hyphen(data):
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

        def get_length(metre_post_from, metre_post_to):
            return abs(metre_post_from - metre_post_to)

        def get_operating_speed(text):
            """
            Receives a text.
            Returns the int between two brackets.
            """
            return round(int(re.findall(r"(?<=\().*(?=\))", text)[0]))

        def get_reduced_speeds(text):
            """
            Receives a text.

            Returns the reduced speeds in km/h for MUs and locomotives.
            """
            if text.find("/") == -1:
                return [round(int(re.findall(r".*(?= \()", text)[0]))] * 2
            else:
                return round(int(re.findall(r".*(?=/)", text)[0])), round(
                    int(re.findall(r"(?<=/).*(?= )", text)[0])
                )

        def convert_date_to_iso(text):
            """
            Receives a date (and time) as a str.
            Returns the text converted to ISO format.
            """
            if type(text) is str:
                try:
                    return (
                        datetime.strptime(text, "%Y.%m.%d %H:%M")
                        .replace(tzinfo=ZoneInfo(key="Europe/Budapest"))
                        .isoformat()
                    )
                except TypeError:
                    return "TypeError"
                except ValueError:
                    return convert_date_to_iso(re.findall(r".*(?=-)", text)[0])
            else:  # if type(text) is datetime
                try:
                    return text.date().isoformat()
                except TypeError:
                    return "TypeError"
                except ValueError:
                    return "ValueError"

        current_sr = SpeedRestriction()

        current_sr.country_iso = "HU"
        current_sr.country_uic = country_uic_database[current_sr.country_iso]
        current_sr.company_short_name = "MÁV"
        current_sr.company_uic = company_uic_database[current_sr.company_short_name]

        current_sr.line = row[0]
        current_sr.station_from_name = remove_space_after_hyphen(row[1])
        current_sr.station_from_uic = get_station_uic(
            current_sr.country_uic, current_sr.station_from_name
        )

        current_sr.on_station = not row[2]  # true if cell is empty
        current_sr.track_switch_source_text = row[4]
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

        current_sr.in_timetable = in_timetable
        current_sr.metre_post_from = int(row[5] * 100)
        current_sr.metre_post_to = int(row[6] * 100)
        current_sr.length = get_length(
            current_sr.metre_post_from, current_sr.metre_post_to
        )
        current_sr.operating_speed = get_operating_speed(row[8])
        (
            current_sr.reduced_speed_for_locomotives,
            reduced_speed_for_mus,
        ) = get_reduced_speeds(row[8])
        current_sr.internal_id = row[10]
        current_sr.time_from = convert_date_to_iso(row[11])
        current_sr.cause = row[12]
        current_sr.time_to = convert_date_to_iso(row[13]) if row[13] else None
        current_sr.comment = row[14]

        database.add(current_sr)

    def _import_worksheets(self, excel_file_location):
        self.logger.info(f"Loading {excel_file_location} started!")
        excel_workbook = load_workbook(excel_file_location, data_only=True)
        self.logger.info(f"{excel_file_location} loaded!")

        current_sr_file_openpyxl = list(excel_workbook.worksheets)
        self.logger.info(
            f"All worksheets imported from {excel_file_location} to current_sr_file_openpyxl!"
        )
        return current_sr_file_openpyxl

    def _rename_columns_manually(self) -> None:
        pass

    def _correct_boolean_values(self) -> None:
        pass

    def _create_table_if_not_exists(self) -> None:
        pass

    def _add_data(self) -> None:
        pass


class GysevSRUpdater(SRUpdater):
    def __init__(self) -> None:
        super().__init__(
            company="GYSEV",
            source_extension="xlsx",
        )

    def get_data(self) -> None:
        pass

    def _rename_columns_manually(self) -> None:
        pass

    def _correct_boolean_values(self) -> None:
        pass

    def _create_table_if_not_exists(self) -> None:
        pass

    def _add_data(self) -> None:
        pass
