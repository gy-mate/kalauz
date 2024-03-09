from datetime import datetime
from hashlib import md5
import re
from typing import Any, final
from zoneinfo import ZoneInfo

from openpyxl.cell import Cell
from pandas import DataFrame
from sqlalchemy import text

# future: remove the comment below when stubs for the library below are available
import roman  # type: ignore
import sqlalchemy.exc

from src.new_data_processors.SR_processors.common import SRUpdater
from src.new_data_processors.common import ExcelDeepProcessor


def _is_tsr(cell: Cell) -> bool:
    return _is_text_in_cell_bold(cell)


def _on_main_track(row: list[str | None]) -> bool:
    if row[3] or (not row[3] and not row[4]):
        return True
    else:
        return False


def _is_text_in_cell_bold(cell: Cell) -> bool:
    return cell.font.bold


def _is_usable(row: list) -> bool:
    if len(row) != 15:
        return False
    elif "Vonal" in str(row[0]):
        return False
    elif "Összes korlátozás:" in str(row[1]):
        return False
    elif row[0] is None or row[1] is None:
        return False
    else:
        return True


def _get_reduced_speeds(
    text_to_search: str | None,
) -> tuple[int, int] | tuple[None, None]:
    try:
        assert text_to_search
        if text_to_search.find("/") == -1:
            reduced_speed = reduced_speed_for_mus = round(
                int(re.findall(r".*(?= \()", text_to_search)[0])
            )
        else:
            reduced_speed = round(int(re.findall(r".*(?=/)", text_to_search)[0]))
            reduced_speed_for_mus = round(
                int(re.findall(r"(?<=/).*(?= )", text_to_search)[0])
            )

        return reduced_speed, reduced_speed_for_mus
    except AssertionError:
        return None, None


def _get_number_between_brackets(text_to_search: str) -> int:
    return round(int(re.findall(r"(?<=\().*(?=\))", text_to_search)[0]))


def _get_end_time(text_to_search: str) -> str:
    return text_to_search[17:22]


@final
class MavUpdater(SRUpdater, ExcelDeepProcessor):
    def __init__(self) -> None:
        super().__init__(
            company="MÁV",
            source_extension="xlsx",
        )

    def _correct_data_manually(self) -> None:
        self.existing_sr_ids = self._get_existing_sr_ids()
        self.current_sr_ids: list[str] = []

        rows_to_add: list[dict[str, Any]] = []
        for worksheet_id, worksheet in enumerate(self._data_to_process):
            for row_id, row_of_cells in enumerate(
                [list(cell) for cell in worksheet.iter_rows()]
            ):
                row: list[str | None] = []
                for cell in row_of_cells:
                    if cell.value is None:
                        row.append(cell.value)
                    else:
                        row.append(str(cell.value))

                if _is_usable(row):
                    metre_post_to = self._get_metre_post(row[6])
                    reduced_speed, reduced_speed_for_mus = _get_reduced_speeds(row[8])

                    row_to_add = {
                        "country_code_iso": self.COUNTRY_CODE_ISO,
                        "company_code_uic": self.COMPANY_CODE_UIC,
                        "internal_id": None,
                        "in_timetable": not _is_tsr(row_of_cells[0]),
                        "due_to_railway_features": NotImplemented,
                        "line": self._get_line(row[0], metre_post_to),
                        "metre_post_from": self._get_metre_post(row[5]),
                        "metre_post_to": metre_post_to,
                        "station_from": self._remove_space_after_hyphen(row[1]),
                        "station_to": (
                            self._remove_space_after_hyphen(row[2]) if row[2] else None
                        ),
                        "on_main_track": _on_main_track(row),
                        "main_track_side": self._get_track_side(row[3]),
                        "station_track_switch_source_text": row[4],
                        "station_track_from": self._get_station_track_switch_from(
                            row[4]
                        ),
                        "station_switch_from": NotImplemented,
                        "station_switch_to": NotImplemented,
                        "operating_speed": self._get_operating_speed(row[8]),
                        "reduced_speed": reduced_speed,
                        "reduced_speed_for_mus": reduced_speed_for_mus,
                        "not_signalled_from_start_point": NotImplemented,
                        "not_signalled_from_end_point": NotImplemented,
                        "cause_source_text": row[11],
                        "cause_category_1": NotImplemented,
                        "cause_category_2": NotImplemented,
                        "cause_category_3": NotImplemented,
                        "time_from": self._get_utc_time(row[10]),
                        "maintenance_planned": None,
                        "time_to": (self._get_utc_time(row[13]) if row[13] else None),
                        "work_to_be_done": None,
                        "comment": row[14],
                    }

                    string_to_hash = "; ".join(
                        [
                            str(row_to_add["company_code_uic"]),
                            str(row_to_add["line"]),
                            str(row_to_add["metre_post_from"]),
                            str(row_to_add["metre_post_to"]),
                            str(row_to_add["main_track_side"]),
                            str(row_to_add["station_track_switch_source_text"]),
                            str(row_to_add["time_from"]),
                        ]
                    ).encode()
                    current_sr_id = md5(string_to_hash).hexdigest()
                    self.current_sr_ids.append(current_sr_id)
                    row_to_add |= {
                        "id": current_sr_id,
                    }

                    rows_to_add.append(row_to_add)

        # future: report bug (false positive) to mypy developers
        self.data = DataFrame.from_dict(rows_to_add)  # type: ignore

    def _get_line(self, line_source: str, metre_post_to: int) -> str:
        internal_to_vpe_line = {
            "5a": "5K",
            "5c": "935a",
            "31": "30M",
            "125a": "125",
            "200": "1AK",
            "203": "1AR",
            "205": "1AN",
            "206": "1AL",
            "207": "1AM",
            "209": "70",
            "210": "1AT",
            "215": "1CK",
            "216": "1AO",
            "217": "1AQ",
            "218": "1AU",
            "221": "1CM",
            "261": "120S",
            "262": "80R",
            "264d": "120O",
            "264f": "100T",
            "265": "154N",
            "268": "4K",
            "275c": "93",
            "280": "66",
            "284a": "100FQ",
            "291": "25K",
            "341": "42M",
            "342": "42L",
            "350": "20P",
            "351": "919b",
            "352": "26K",
            "354": "37K",
            "370": "94L",
            "372": "80S",
            "390": "154M",
            "4002": "100FL",
        }
        if line_source in internal_to_vpe_line:
            line_corrected = internal_to_vpe_line[line_source]
            self.logger.debug(
                f"Line {line_source} replaced with {internal_to_vpe_line[line_source]}!"
            )
            
            if line_corrected == "75" and metre_post_to > 4800:  # rough metre post number of diverging lines
                return "75A"
            else:
                return line_corrected
        else:
            return line_source

    def _get_existing_sr_ids(self) -> list[str]:
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

    def _get_metre_post(self, text_to_search: str | None) -> int | None:
        try:
            assert text_to_search
            return int(float(text_to_search) * 100)
        except AssertionError:
            self.logger.critical(f"Metre post not found in {text_to_search}!")
            raise

    def _remove_space_after_hyphen(self, data: str | None) -> str | None:
        try:
            assert data
            return re.sub(r"(?<=\w)- (?=\w)", "-", str(data))
        except AssertionError:
            self.logger.critical(f"`station_from` is empty!")
            raise

    def _get_track_side(self, text_to_search: str | None) -> str | None:
        try:
            assert text_to_search
            if "bal" in text_to_search:
                return "left"
            elif "jobb" in text_to_search:
                return "right"
            elif "local" in text_to_search:
                return "local"
            else:
                raise ValueError(f"Unrecognized track side: {text_to_search}!")
        except AssertionError:
            return None
        except ValueError as exception:
            self.logger.critical(exception)
            raise

    def _get_station_track_switch_from(
        self, text_to_search: str | None
    ) -> int | str | None:
        try:
            assert text_to_search
            return self._extract_number(text_to_search)
        except AssertionError:
            return None
        except roman.InvalidRomanNumeralError:
            return "InvalidRomanNumeralError"

    def _get_operating_speed(self, text_to_search: str | None) -> int:
        try:
            assert text_to_search
            return _get_number_between_brackets(text_to_search)
        except AssertionError:
            self.logger.critical(f"Operating speed not found in {text_to_search}!")
            raise

    def _extract_number(self, text_to_search: str) -> int | str:
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

    def _get_date(self, text_to_search: str | None) -> str:
        try:
            assert text_to_search
            return text_to_search[:10]
        except AssertionError:
            self.logger.critical(f"`date_from` not found!")
            raise

    def _get_utc_time(self, text_to_search: str | None) -> datetime:
        try:
            assert text_to_search
            if len(text_to_search) == 19:
                return (
                    datetime.fromisoformat(text_to_search)
                    .replace(tzinfo=ZoneInfo(key="Europe/Budapest"))
                    .astimezone(ZoneInfo(key="UTC"))
                )
            elif len(text_to_search) == 16:
                return (
                    datetime.strptime(text_to_search, "%Y.%m.%d %H:%M")
                    .replace(tzinfo=ZoneInfo(key="Europe/Budapest"))
                    .astimezone(ZoneInfo(key="UTC"))
                )
            elif len(text_to_search) in range(21, 22 + 1):
                return self._get_utc_time(
                    f"{self._get_date(text_to_search)} {_get_end_time(text_to_search)}"
                )
            else:
                raise ValueError
        except AssertionError:
            self.logger.critical(f"`time_from` not found!")
            raise
        except ValueError:
            self.logger.critical(f"Unrecognized date format: {text_to_search}!")
            raise

    def _correct_boolean_values(self) -> None:
        pass

    def _add_data(self) -> None:
        with self.database.engine.begin() as connection:
            queries = [
                """
                insert ignore into speed_restrictions (
                    id,
                    country_code_iso,
                    company_code_uic,
                    internal_id,
                    in_timetable,
                    due_to_railway_features,
                    line,
                    metre_post_from,
                    metre_post_to,
                    station_from,
                    station_to,
                    on_main_track,
                    main_track_side,
                    station_track_switch_source_text,
                    station_track_from,
                    station_switch_from,
                    station_switch_to,
                    operating_speed,
                    reduced_speed,
                    reduced_speed_for_mus,
                    not_signalled_from_start_point,
                    not_signalled_from_end_point,
                    cause_source_text,
                    cause_category_1,
                    cause_category_2,
                    cause_category_3,
                    time_from,
                    maintenance_planned,
                    time_to,
                    work_to_be_done,
                    comment
                )
                values (
                    :id,
                    :country_code_iso,
                    :company_code_uic,
                    :internal_id,
                    :in_timetable,
                    :due_to_railway_features,
                    :line,
                    :metre_post_from,
                    :metre_post_to,
                    :station_from,
                    :station_to,
                    :on_main_track,
                    :main_track_side,
                    :station_track_switch_source_text,
                    :station_track_from,
                    :station_switch_from,
                    :station_switch_to,
                    :operating_speed,
                    :reduced_speed,
                    :reduced_speed_for_mus,
                    :not_signalled_from_start_point,
                    :not_signalled_from_end_point,
                    :cause_source_text,
                    :cause_category_1,
                    :cause_category_2,
                    :cause_category_3,
                    :time_from,
                    :maintenance_planned,
                    :time_to,
                    :work_to_be_done,
                    :comment
                )
                """,
                """
                update speed_restrictions
                set
                    maintenance_planned = :maintenance_planned,
                    time_to = :time_to,
                    work_to_be_done = :work_to_be_done,
                    comment = :comment
                where id = :id
                """,
            ]

            for index, row in self.data.iterrows():
                for query in queries:
                    connection.execute(
                        text(query),
                        row.to_dict(),
                    )

        with self.database.engine.begin() as connection:
            query = """
            update speed_restrictions
            set
                time_to = :time_to
            where id = :id and time_to is null
            """

            for sr_id in set(self.existing_sr_ids) - set(self.current_sr_ids):
                raise NotImplementedError
                # noinspection PyUnreachableCode
                connection.execute(
                    text(query),
                    {
                        "id": sr_id,
                        "time_to": self.TODAY,
                    },
                )
