from datetime import datetime
from hashlib import md5
import re
from typing import final

from openpyxl.cell import Cell

# future: remove the comment below when stubs for the library below are available
import regex_spm  # type: ignore
from sqlalchemy import text

# future: remove the comment below when stubs for the library below are available
import roman  # type: ignore

from src.SR import SR
from src.new_data_processors.SR_processors.common import SRUpdater
from src.new_data_processors.common_excel_processors import ExcelProcessorWithFormatting


def is_tsr(cell: Cell) -> bool:
    return is_text_in_cell_bold(cell)


def on_main_track(row: list[str | None]) -> bool:
    if row[2] or row[3]:
        return True
    else:
        return False


def is_text_in_cell_bold(cell: Cell) -> bool:
    return cell.font.bold


def is_usable(row: list) -> bool:
    if "Vonal" in str(row[0]):
        return False
    elif "Összes korlátozás:" in str(row[1]):
        return False
    elif row[0] is None or row[1] is None:
        return False
    else:
        return True


def get_number_between_brackets(text_to_search: str) -> int:
    return round(int(re.findall(r"(?<=\().*(?=\))", text_to_search)[0]))


@final
class MavUpdater(SRUpdater, ExcelProcessorWithFormatting):
    def __init__(self) -> None:
        super().__init__(
            company="MÁV",
            source_extension="xlsx",
        )

    def correct_data_manually(self) -> None:
        srs_to_add: list[SR] = []
        for worksheet_id, worksheet in enumerate(self._data_to_process):
            for row_id, row_of_cells in enumerate(
                [list(cell) for cell in worksheet.iter_rows()]
            ):
                row: list[str | None] = []
                for column_id, cell in enumerate(row_of_cells):
                    if cell.value is None:
                        row.append(cell.value)
                    else:
                        row.append(str(cell.value))

                    if len(row_of_cells) < 16 and column_id == 10:
                        row.append(None)
                        if len(row_of_cells) == 14:
                            row.append(None)

                if is_usable(row):
                    metre_post_to = self.get_metre_post(row[6])
                    station_from = self.remove_space_after_hyphen(row[1])
                    station_to = (
                        self.remove_space_after_hyphen(row[2]) if row[2] else None
                    )
                    reduced_speed, reduced_speed_for_mus = self.get_reduced_speeds(
                        row[8]
                    )
                    time_from = self.get_utc_time(row[10])
                    assert isinstance(time_from, datetime)

                    # TODO: make overload methods from this section
                    # future: make this an SR object
                    sr_to_add = SR(
                        country_code_iso=self.COUNTRY_CODE_ISO,
                        company_code_uic=self.COMPANY_CODE_UIC,
                        internal_id=None,
                        decision_id=row[11],
                        in_timetable=not is_tsr(row_of_cells[0]),
                        due_to_railway_features=NotImplemented,
                        line=self.get_line(
                            line_source=row[0],
                            station_from=station_from,
                            station_to=station_to,
                            metre_post_to=metre_post_to,
                        ),
                        metre_post_from=self.get_metre_post(row[5]),
                        metre_post_to=metre_post_to,
                        station_from=station_from,
                        station_to=station_to,
                        on_main_track=on_main_track(row),
                        main_track_side=self.get_track_side(row[3]),
                        station_track_switch_source_text=row[4],
                        station_track_from=self.get_station_track_switch_from(row[4]),
                        station_switch_from=NotImplemented,
                        station_switch_to=NotImplemented,
                        operating_speed=self.get_operating_speed(row[8]),
                        reduced_speed=reduced_speed,
                        reduced_speed_for_mus=reduced_speed_for_mus,
                        not_signalled_from_start_point=NotImplemented,
                        not_signalled_from_end_point=NotImplemented,
                        cause_source_text=row[12],
                        cause_category_1=NotImplemented,
                        cause_category_2=NotImplemented,
                        cause_category_3=NotImplemented,
                        time_from=time_from,
                        work_to_be_done=None,
                        time_to=self.get_utc_time(row[14]) if row[14] else None,
                        comment=row[15],
                    )

                    string_to_hash = "; ".join(
                        [
                            str(sr_to_add.company_code_uic),
                            str(sr_to_add.line),
                            str(sr_to_add.metre_post_from),
                            str(sr_to_add.metre_post_to),
                            str(sr_to_add.main_track_side),
                            str(sr_to_add.station_track_switch_source_text),
                            str(sr_to_add.time_from),
                        ]
                    ).encode()
                    sr_to_add.id = md5(string_to_hash).hexdigest()
                    self.current_sr_ids.append(sr_to_add.id)

                    srs_to_add.append(sr_to_add)

        self.data = srs_to_add

    def add_data(self) -> None:
        with self.database.engine.begin() as connection:
            queries = [
                """
                insert ignore into speed_restrictions (
                    id,
                    country_code_iso,
                    company_code_uic,
                    internal_id,
                    decision_id,
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
                    work_to_be_done,
                    time_to,
                    comment
                )
                values (
                    :id,
                    :country_code_iso,
                    :company_code_uic,
                    :internal_id,
                    :decision_id,
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
                    :work_to_be_done,
                    :time_to,
                    :comment
                )
                """,
                """
                update speed_restrictions
                set
                    work_to_be_done = :work_to_be_done,
                    time_to = :time_to,
                    comment = :comment
                where id = :id
                """,
            ]

            for sr in self.data:
                for query in queries:
                    connection.execute(
                        text(query),
                        sr.__dict__,
                    )

        with self.database.engine.begin() as connection:
            query = """
            update speed_restrictions
            set
                time_to = :time_to
            where id = :id and time_to is null
            """

            for terminated_sr_id in set(self.existing_sr_ids) - set(
                [sr.id for sr in self.data]
            ):
                connection.execute(
                    text(query),
                    {
                        "id": terminated_sr_id,
                        "time_to": self.TODAY,
                    },
                )

    def get_metre_post(self, text_to_search: str | None) -> int:
        try:
            assert text_to_search
            return int(float(text_to_search) * 100)
        except AssertionError:
            self.logger.critical(f"Metre post not found in {text_to_search}!")
            raise

    def get_reduced_speeds(
        self,
        text_to_search: str | None,
    ) -> tuple[int, int]:
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
            self.logger.critical(f"Reduced speeds not found in {text_to_search}!")
            raise

    def get_line(
        self,
        line_source: str | None,
        station_to: str | None,
        station_from: str | None,
        metre_post_to: int,
    ) -> str:
        try:
            assert line_source
            internal_to_vpe_line = {
                "5a": "5K",
                "5b": "5L",
                "5c": "935a",
                "11a": "11B",
                "31": "30M",
                "51": "50K",
                "74": "78L",
                "125a": "125",
                "200": "1AK",  # future: '1AV' from 'Angyalföld elágazás'
                "202": "1AK",
                "203": "1AR",
                "204": "1BL",
                "205": "1AN",
                "206": "1AL",
                "207": "1AM",
                "209": "70",
                "210": "1AT",
                "215": "1CK",
                "216": "1AO",
                "217": "1AQ",
                "218": "1AU",
                "219": "1AW",
                "220": "1AY",
                "221": "1CM",
                "222": "1AL",
                "224": "1AKO",
                "225": "1AKN",
                "227": "1BLA",
                "261": "120S",
                "262": "80R",
                "263": "140N",
                "264a": "100S",
                "264b": "100R",
                "264c": "120Q",
                "264d": "120O",
                "264f": "100T",
                "264g": "100N",
                "264j": "100EL",
                "265": "154N",
                "268": "4K",
                "269": "146K",
                "275c": "93",
                "280": "66",
                "281": "25",
                "284a": "100FQ",
                "284c": "100FM",
                "284d": "100FP",
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
                "4001": "100FO",
                "4002": "100FL",
            }
            if line_source in internal_to_vpe_line:
                line_corrected = internal_to_vpe_line[line_source]

                if (
                    line_corrected == "75" and metre_post_to > 4800
                ):  # rough metre post number of diverging lines
                    return "75A"

                return line_corrected
            else:
                if line_source == "113":
                    first_part_of_the_line = (station_to or station_from) in [
                        "Nyíregyháza",
                        "Nagykálló",
                        "Nagykállói elágazás",  # sic!
                        "Kállósemjén",
                        "Máriapócs",  # sic!
                        "Nyírbátor",
                    ]
                    if first_part_of_the_line:
                        return "113 (1)"
                    else:
                        return "113 (2)"
                return line_source
        except AssertionError:
            self.logger.critical("Line not found!")
            raise

    def remove_space_after_hyphen(self, data: str | None) -> str:
        try:
            assert data
            return re.sub(r"(?<=\w)- (?=\w)", "-", str(data))
        except AssertionError:
            self.logger.critical(f"`station_from` is empty!")
            raise

    def get_track_side(self, text_to_search: str | None) -> str | None:
        try:
            assert text_to_search
            # future: convert this to an enum class
            match regex_spm.match_in(text_to_search):
                case "bal":
                    return "left"
                case "jobb":
                    return "right"
                case "local":
                    return "local"
                case _:
                    raise ValueError(f"Unrecognized track side: {text_to_search}!")
        except AssertionError:
            return None
        except ValueError as exception:
            self.logger.critical(exception)
            raise

    def get_station_track_switch_from(self, text_to_search: str | None) -> str | None:
        try:
            assert text_to_search
            return str(self.extract_number(text_to_search))
        except AssertionError:
            return None
        except roman.InvalidRomanNumeralError:
            return "InvalidRomanNumeralError"

    def get_operating_speed(self, text_to_search: str | None) -> int:
        try:
            assert text_to_search
            return get_number_between_brackets(text_to_search)
        except AssertionError:
            self.logger.critical(f"Operating speed not found in {text_to_search}!")
            raise
