from datetime import date


class SR:
    def __init__(
        self,
        id: str,
        country_code_iso: str,
        company_code_uic: int,
        internal_id: str,
        decision_id: str,
        in_timetable: bool,
        due_to_railway_features: bool,
        line: str,
        metre_post_from: int,
        metre_post_to: int,
        station_from: str,
        station_to: str,
        on_main_track: bool,
        main_track_side: str,
        station_track_switch_source_text: str,
        station_track_from: str,
        station_switch_from: str,
        station_switch_to: str,
        operating_speed: int,
        reduced_speed: int,
        reduced_speed_for_mus: int,
        not_signalled_from_start_point: bool,
        not_signalled_from_end_point: bool,
        cause_source_text: str,
        cause_category_1: str,
        cause_category_2: str,
        cause_category_3: str,
        time_from: date,
        maintenance_planned: bool,
        time_to: date,
        work_to_be_done: str,
        comment: str,
    ):
        self.id = id
        self.country_code_iso = country_code_iso
        self.company_code_uic = company_code_uic
        self.internal_id = internal_id
        self.decision_id = decision_id
        self.in_timetable = in_timetable
        self.due_to_railway_features = due_to_railway_features
        self.line = line
        self.metre_post_from = metre_post_from
        self.metre_post_to = metre_post_to
        self.station_from = station_from
        self.station_to = station_to
        self.on_main_track = on_main_track
        self.main_track_side = main_track_side
        self.station_track_switch_source_text = station_track_switch_source_text
        self.station_track_from = station_track_from
        self.station_switch_from = station_switch_from
        self.station_switch_to = station_switch_to
        self.operating_speed = operating_speed
        self.reduced_speed = reduced_speed
        self.reduced_speed_for_mus = reduced_speed_for_mus
        self.not_signalled_from_start_point = not_signalled_from_start_point
        self.not_signalled_from_end_point = not_signalled_from_end_point
        self.cause_source_text = cause_source_text
        self.cause_category_1 = cause_category_1
        self.cause_category_2 = cause_category_2
        self.cause_category_3 = cause_category_3
        self.time_from = time_from
        self.maintenance_planned = maintenance_planned
        self.time_to = time_to
        self.work_to_be_done = work_to_be_done
        self.comment = comment
