from datetime import datetime


class SR:
    def __init__(
        self,
        country_code_iso: str,
        company_code_uic: int,
        internal_id: str | None,
        decision_id: str | None,
        in_timetable: bool,
        due_to_railway_features: bool,
        line: str,
        metre_post_from: int,
        metre_post_to: int,
        station_from: str,
        station_to: str | None,
        on_main_track: bool,
        main_track_side: str | None,
        station_track_switch_source_text: str | None,
        station_track_from: str | None,
        station_switch_from: str | None,
        station_switch_to: str | None,
        operating_speed: int,
        reduced_speed: int,
        reduced_speed_for_mus: int,
        not_signalled_from_start_point: bool | None,
        not_signalled_from_end_point: bool | None,
        cause_source_text: str | None,
        cause_category_1: str | None,
        cause_category_2: str | None,
        cause_category_3: str | None,
        time_from: datetime,
        work_to_be_done: str | None,
        time_to: datetime | None,
        comment: str | None,
        sr_id: str | None = None,
    ):
        self.id = sr_id
        self.country_code_iso = country_code_iso
        self.company_code_uic = company_code_uic
        self.internal_id = internal_id
        self.decision_id = decision_id
        self.in_timetable = in_timetable
        self.due_to_railway_features = (
            due_to_railway_features if not NotImplemented else None
        )
        self.line = line
        self.metre_post_from = metre_post_from
        self.metre_post_to = metre_post_to
        self.station_from = station_from
        self.station_to = station_to
        self.on_main_track = on_main_track
        self.main_track_side = main_track_side
        self.station_track_switch_source_text = station_track_switch_source_text
        self.station_track_from = station_track_from
        self.station_switch_from = station_switch_from if not NotImplemented else None
        self.station_switch_to = station_switch_to if not NotImplemented else None
        self.operating_speed = operating_speed
        self.reduced_speed = reduced_speed
        self.reduced_speed_for_mus = reduced_speed_for_mus
        self.not_signalled_from_start_point = (
            not_signalled_from_start_point if not NotImplemented else None
        )
        self.not_signalled_from_end_point = (
            not_signalled_from_end_point if not NotImplemented else None
        )
        self.cause_source_text = cause_source_text
        self.cause_category_1 = cause_category_1 if not NotImplemented else None
        self.cause_category_2 = cause_category_2 if not NotImplemented else None
        self.cause_category_3 = cause_category_3 if not NotImplemented else None
        self.time_from = time_from
        self.work_to_be_done = work_to_be_done
        self.time_to = time_to
        self.comment = comment
