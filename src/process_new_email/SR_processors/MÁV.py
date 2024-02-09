from datetime import datetime
import re
from typing import final
from zoneinfo import ZoneInfo

from openpyxl.cell import Cell
from pandas import DataFrame

# future: remove the comment below when stubs for the library below are available
import roman  # type: ignore

from src.process_new_email.SR_processors.common import SRUpdater
from src.process_new_email.table_updaters.common import ExcelDeepProcessor


def _is_tsr(cell: Cell) -> bool:
    return cell.font.bold


def _header_or_footer(row: list) -> bool:
    return "Vonal" in str(row[0]) or "Összes korlátozás:" in str(row[1])


def _get_reduced_speeds(text_to_search: str) -> tuple[int, int]:
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


def _get_metre_post(text_to_search: str) -> int | None:
    try:
        assert isinstance(text_to_search, int)
        return int(text_to_search * 100)
    except AssertionError:
        return None


def remove_space_after_hyphen(data: str) -> str:
    return re.sub(r"(?<=\w)- (?=\w)", "-", str(data))


def _get_operating_speed(text_to_search: str) -> int:
    return _get_number_between_brackets(text_to_search)


def _get_number_between_brackets(text_to_search: str) -> int:
    return round(int(re.findall(r"(?<=\().*(?=\))", text_to_search)[0]))


def _get_date(text_to_search: str) -> str:
    return text_to_search[:10]


def _get_end_time(text_to_search: str) -> str:
    return text_to_search[12:22]


@final
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
                in_timetable = not _is_tsr(row_of_cells[0])
                row = [str(cell.value) for cell in row_of_cells]

                if not _header_or_footer(row):
                    reduced_speed, reduced_speed_for_mus = _get_reduced_speeds(row[8])
                    
                    metre_post_to = _get_metre_post(row[6])

                    row_to_add = {
                        "country_iso": self.COUNTRY_CODE_ISO,
                        "company_uic": self.COMPANY_CODE_UIC,
                        "internal_id": None,
                        "in_timetable": in_timetable,
                        "due_to_railway_features": NotImplemented,
                        "line": row[0],
                        "metre_post_from": _get_metre_post(row[5]),
                        "metre_post_to": metre_post_to,
                        "station_from": remove_space_after_hyphen(row[1]),
                        "station_to": remove_space_after_hyphen(row[2]) if row[2] else None,
                        "on_main_track": True if row[3] else False,
                        "main_track_left_or_right": self._on_right_track(row[3]),
                        "station_track_switch_source_text": row[4],
                        "station_track_switch_from": NotImplemented,
                        "station_track_switch_to": NotImplemented,
                        "operating_speed": _get_operating_speed(row[8]),
                        "reduced_speed": reduced_speed,
                        "reduced_speed_for_mus": reduced_speed_for_mus,
                        "cause": row[12],
                        "date_from": _get_date(row[11]),
                        "time_from": self._convert_date_to_iso(row[11]),
                        "maintenance_planned": None,
                        "date_to": _get_date(row[13]) if row[13] else None,
                        "time_to": (
                            self._convert_date_to_iso(row[13]) if row[13] else None
                        ),
                        "work_to_be_done": None,
                        "comment": row[14],
                    }
                    
    def _on_right_track(self, text_to_search: str) -> bool:
        try:
            if "bal" in text_to_search:
                return False
            elif "jobb" in text_to_search:
                return True
            else:
                raise ValueError(f"Unrecognized track side: {text_to_search}!")
        except ValueError as exception:
            self.logger.critical(exception)
            raise

    def _convert_date_to_iso(self, text_to_search: str) -> datetime:
        try:
            if len(text_to_search) == 10:
                return datetime.strptime(text_to_search, "%Y.%m.%d").replace(
                    tzinfo=ZoneInfo(key="Europe/Budapest")
                )
            elif len(text_to_search) == 16:
                return datetime.strptime(text_to_search, "%Y.%m.%d %H:%M").replace(
                    tzinfo=ZoneInfo(key="Europe/Budapest")
                )
            elif len(text_to_search) == 22:
                return self._convert_date_to_iso(
                    f"{_get_date(text_to_search)} {_get_end_time(text_to_search)}"
                )
            else:
                raise ValueError(f"Unrecognized date format: {text_to_search}!")
        except ValueError as exception:
            self.logger.critical(exception)
            raise

    def _correct_boolean_values(self) -> None:
        pass

    def _create_table_if_not_exists(self) -> None:
        pass

    def _add_data(self) -> None:
        pass
