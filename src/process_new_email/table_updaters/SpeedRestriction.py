class SpeedRestriction:
    """
    Stores all data of a speed restriction.
    """
    
    def __init__(self,
                 country_iso=None,
                 country_uic=None,
                 company_short_name=None,
                 company_uic=None,
                 line=None,
                 station_from_name=None,
                 station_from_uic=None,
                 station_to_name=None,
                 station_to_uic=None,
                 on_station=None,
                 track_switch_source_text=None,
                 applied_to=None,
                 number_of_open_line_tracks=None,
                 track_open_line=None,
                 track_station=None,
                 switch_station=None,
                 in_timetable=None,
                 metre_post_from=None,
                 metre_post_to=None,
                 length=None,
                 operating_speed=None,
                 reduced_speed_for_locomotives=None,
                 reduced_speed_for_mus=None,
                 internal_id=None,
                 time_from=None,
                 cause=None,
                 time_to=None,
                 comment=None):
        self.country_iso = country_iso
        self.country_uic = country_uic
        self.company_short_name = company_short_name
        self.company_uic = company_uic
        self.line = line
        self.station_from_name = station_from_name
        self.station_from_uic = station_from_uic
        self.station_to_name = station_to_name
        self.station_to_uic = station_to_uic
        self.on_station = on_station
        self.track_switch_source_text = track_switch_source_text
        self.applied_to = applied_to
        self.number_of_open_line_tracks = number_of_open_line_tracks
        self.track_open_line = track_open_line
        self.track_station = track_station
        self.switch_station = switch_station
        self.in_timetable = in_timetable
        self.metre_post_from = metre_post_from
        self.metre_post_to = metre_post_to
        self.length = length
        self.operating_speed = operating_speed
        self.reduced_speed_for_locomotives = reduced_speed_for_locomotives
        self.reduced_speed_for_mus = reduced_speed_for_mus
        self.internal_id = internal_id
        self.time_from = time_from
        self.cause = cause
        self.time_to = time_to
        self.comment = comment
    
    def __hash__(self):
        """
        The least number of columns selected for hashing to increase processing speed.
        These columns can indicate different SRs by themselves.
        Returns the hash of those columns.
        """
        return hash((self.country_uic,
                     self.company_uic,
                     self.line,
                     self.metre_post_from,
                     self.metre_post_to,
                     self.track_switch_source_text,
                     self.track_open_line,
                     self.time_from,
                     self.time_to,
                     self.comment))
    
    def __eq__(self, other):
        """
        Receives two objects.
        The least number of columns selected for testing equality to increase processing speed.
        These columns can indicate different SRs by themselves.
        Returns if the two objects are SpeedRestriction objects and identical.
        """
        if isinstance(other, SpeedRestriction):
            return (self.country_uic,
                    self.company_uic,
                    self.line,
                    self.metre_post_from,
                    self.metre_post_to,
                    self.track_switch_source_text,
                    self.track_open_line,
                    self.time_from,
                    self.time_to,
                    self.comment) == \
                (other.country_uic,
                 other.company_uic,
                 other.line,
                 other.metre_post_from,
                 other.metre_post_to,
                 other.track_switch_source_text,
                 other.track_open_line,
                 other.time_from,
                 other.time_to,
                 other.comment)
        else:
            return False
    
    def __iter__(self):
        """
        Returns an iterator which iterates through all attributes of the object.
        Used for creating a list of an object.
        """
        return iter([self.country_iso,
                     self.country_uic,
                     self.company_short_name,
                     self.company_uic,
                     self.line,
                     self.station_from_name,
                     self.station_from_uic,
                     self.station_to_name,
                     self.station_to_uic,
                     self.on_station,
                     self.track_switch_source_text,
                     self.applied_to,
                     self.number_of_open_line_tracks,
                     self.track_open_line,
                     self.track_station,
                     self.switch_station,
                     self.in_timetable,
                     self.metre_post_from,
                     self.metre_post_to,
                     self.length,
                     self.operating_speed,
                     self.reduced_speed_for_locomotives,
                     self.reduced_speed_for_mus,
                     self.internal_id,
                     self.time_from,
                     self.cause,
                     self.time_to,
                     self.comment])
    
    # def get_attributes(self):
    #     """
    #     Returns all attributes of the class.
    #     Used for adding a header when exporting a .csv file.
    #     """
    #     attribute_list = []
    #     for attribute, value in self.__dict__.items():
    #         attribute_list.append(attribute)
    #     return attribute_list
    