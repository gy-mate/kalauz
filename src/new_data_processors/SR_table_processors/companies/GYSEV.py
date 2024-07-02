import contextlib
from datetime import datetime
from hashlib import md5
from io import BytesIO
from typing import final, override

import numpy as np
from pandas import DataFrame
import pandas as pd

# future: remove the comment below when stubs for the library below are available
import regex_spm  # type: ignore
from sqlalchemy import text

from src.SR import SR
from src.new_data_processors.SR_table_processors.category_prediction import (
    CategoryPredictor,
)
from src.new_data_processors.SR_table_processors.common import (
    SRUpdater,
    datetime_format_is_dmy,
)


def get_metre_post(text_to_search: str) -> int:
    hectometres = int(text_to_search.split("+")[0])
    metres = int(text_to_search.split("+")[1])
    return hectometres * 100 + metres


def get_bounding_stations(text_to_search: str) -> tuple[str, str | None]:
    if " - " in text_to_search:
        bounding_stations = text_to_search.split(" - ")
        return bounding_stations[0], bounding_stations[-1]
    else:
        return text_to_search, None


def on_main_track(text_to_search: str) -> bool:
    if (
        "átmenő fővágányán" in text_to_search
        or "nyiltvonalon" in text_to_search  # sic!
        or "bejárati jelző és kitérők között" in text_to_search
    ):
        return True
    else:
        return False


def get_main_track_side(text_to_search: str) -> str | None:
    match regex_spm.match_in(text_to_search):
        case "bal":
            return "left"
        case "jobb":
            return "right"
        case _:
            return None


@final
class GysevUpdater(SRUpdater):
    def __init__(self, category_predictor: CategoryPredictor) -> None:
        super().__init__(
            company="GYSEV",
            source_extension="xlsx",
            category_predictor=category_predictor,
        )
        self._data_to_process: DataFrame = NotImplemented

    def import_data(self) -> None:
        with open(self._file_to_be_imported, "rb") as file:
            excel_file = file.read()

        self._data_to_process = pd.read_excel(
            io=BytesIO(excel_file),
            header=1,
        )
        self._data_to_process.drop(
            columns=[
                "Hossz",
                "Eltérés (perc)",
                "Vonalra eng. seb",
            ],
            inplace=True,
        )
        self._data_to_process.rename(
            columns={
                "Sorszám": "internal_id",
                "Vonal": "line",
                "Állomás(köz)": "stations_between",
                "Helye": "on_main_track_source_text",
                "Állomási vágány/kitérő száma": "station_track_switch_source_text",
                "Szelvénytől": "metre_post_from",
                "Szelvényig": "metre_post_to",
                "Iránya": "direction",
                "Bevezetés kezdete": "time_from",
                "Megszüntetés ideje": "time_to",
                "Sebességkorlátozás": "reduced_speed",
                "Sebességkorlátozás oka": "cause_source_text",
                "Típus": "in_timetable",
                "Megszüntetés tervezett dátuma": "time_to_planned",
                "Megszüntetés tervezett módja": "work_to_be_done",
                "Szakaszra eng. seb.": "operating_speed",
            },
            inplace=True,
        )

    def correct_data_manually(self) -> None:
        pass

    def correct_boolean_values(self) -> None:
        pass

    @override
    def correct_df_na_values_for_database(self) -> None:
        self._data_to_process.replace(
            to_replace={
                pd.NA: None,
                pd.NaT: None,
                np.nan: None,  # future: remove this line when https://github.com/pandas-dev/pandas/issues/32265 is fixed
                NotImplemented: None,  # TODO: remove this line in production
            },
            inplace=True,
        )

    def add_data(self) -> None:
        srs_to_add: list[SR] = []
        for row in self._data_to_process.itertuples():
            assert isinstance(row.operating_speed, int)

            bounding_stations = get_bounding_stations(str(row.stations_between))
            reduced_speed = int(str(row.reduced_speed).replace(" km/h", ""))

            with contextlib.suppress(AssertionError):
                station_track_from = str(
                    self.extract_number(str(row.station_track_switch_source_text))
                    if row.station_track_switch_source_text
                    else None
                )
            
            cause_categories = (
                self.CATEGORY_PREDICTOR.predict_category(str(row.cause_source_text))
                if str(row.cause_source_text) != ""
                else None
            )
            time_from = self.get_utc_time(str(row.time_from))
            assert isinstance(time_from, datetime)

            sr_to_add = SR(
                country_code_iso=self.COUNTRY_CODE_ISO,
                company_code_uic=self.COMPANY_CODE_UIC,
                internal_id=str(row.internal_id),
                decision_id=None,
                in_timetable=True if row.in_timetable == "állandó" else False,
                due_to_railway_features=NotImplemented,
                line=self.get_line(str(row.line)),
                metre_post_from=get_metre_post(str(row.metre_post_from)),
                metre_post_to=get_metre_post(str(row.metre_post_to)),
                station_from=bounding_stations[0],
                station_to=bounding_stations[1],
                on_main_track=on_main_track(str(row.on_main_track_source_text)),
                main_track_side=get_main_track_side(str(row.on_main_track_source_text)),
                station_track_switch_source_text=str(
                    row.station_track_switch_source_text
                ),
                station_track_from=station_track_from,
                station_switch_from=NotImplemented,
                station_switch_to=NotImplemented,
                operating_speed=row.operating_speed,
                reduced_speed=reduced_speed,
                reduced_speed_for_mus=reduced_speed,
                not_signalled_from_start_point=None,
                not_signalled_from_end_point=None,
                cause_source_text=str(row.cause_source_text),
                cause_categories=cause_categories,
                time_from=time_from,
                work_to_be_done=str(row.work_to_be_done),
                time_to=self.get_time_to(str(row.time_to), str(row.time_to_planned)),
                comment=None,
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

    def get_line(self, line_source: str) -> str:
        try:
            assert line_source
            lines_to_be_manually_corrected = {
                "17": "17 (1)",
                "8E": "8",
            }
            if line_source in lines_to_be_manually_corrected:
                line_corrected = lines_to_be_manually_corrected[line_source]
                return line_corrected
            else:
                return line_source
        except AssertionError:
            self.logger.critical("Line not found!")
            raise

    def get_time_to(self, exact_time: str, estimated_time: str) -> datetime | None:
        if exact_time:
            return self.get_utc_time(exact_time)
        elif datetime_format_is_dmy(estimated_time):
            return self.get_utc_time(estimated_time)
        else:
            return None
