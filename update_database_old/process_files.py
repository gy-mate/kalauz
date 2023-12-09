import csv
import logger
from openpyxl import load_workbook
import re
import roman
from datetime import datetime
from zoneinfo import ZoneInfo

import file_management


def import_csv(file_location, index_col_loc, values_col_loc):
    """
    Receives:
    a .csv file's _location_;
    the index _column's number_;
    the values' _column's number_.

    Opens the .csv file and iterates through rows.

    Returns a _dictionary_ of the corresponding indexes and values in the .csv file.
    """
    with open(file_location, mode='r') as file:
        dictionary = {}
        for row in csv.reader(file, delimiter=';'):
            key = file_management.auto_type(row[index_col_loc])
            value = file_management.auto_type(row[values_col_loc])
            dictionary[key] = value
    logging.info(f'{file_location} was found and imported!')
    return dictionary


def import_worksheets(excel_file_location):
    """
    Receives an _Excel file location_ and a _database_.

    Imports an _Excel file_ using `openpyxl`.
    Appends _all worksheets_ from the _Excel file_ to the _database_.

    Returns the _database_.
    """
    logging.info(f'Loading {excel_file_location} started!')
    excel_workbook = load_workbook(excel_file_location, data_only=True)
    logging.info(f'{excel_file_location} loaded!')

    current_sr_file_openpyxl = list(excel_workbook.worksheets)
    logging.info(f'All worksheets imported from {excel_file_location} to current_sr_file_openpyxl!')
    return current_sr_file_openpyxl


def check_permanence(cell):
    """
    Receives an openpyxl object's _cell_.
    Returns if that cell is _bold_.
    """
    return cell.font.bold


def should_be_imported(import_all, is_it_temporary):
    """
    Receives two bool parameters:
    if all lines should be imported;
    if the SR is temporary.

    Returns if the current SR to import should already have been imported, therefore if it should be done now.
    """
    return import_all is True or is_it_temporary is False


def convert_openpyxl_cells_to_values(row):
    """
    Receives an openpyxl object's row.

    Returns the same row but containing just each cell's _values_.
    """
    for idx_02, cell in enumerate(row):
        row[idx_02] = cell.value


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


def add_data(row,
             database,
             in_timetable,
             country_uic_database,
             company_uic_database,
             station_database):
    """
    Receives:
    a _row_ containing a SR's all data;
    a set _database_ where the data should be added to;
    if the SR is _in the timetables_;
    a _dict_ of UIC member countries;
    a _dict_ of UIC member railway companies;
    a _dict_ of all stations.

    Adds all data in the row to a `SpeedRestriction` object.

    Adds the object to the _database_.
    """

    def remove_space_after_hyphen(data):
        """
        Receives a data in any format.

        Converts the data to str.

        Returns the same str but with spaces removed after hyphens.
        """
        return re.sub(r'(?<=\w)- (?=\w)', '-', str(data))

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
            return int(str(country_uic) + re.search(r'(?<=HU).*', station_database[station_name])[0])
        except KeyError:
            logging.warning(f'Error: {station_name} could not be converted to UIC code!')
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
        arabic_regex = re.compile("""
            (
            ^|(?<=[ .(])
            )
            
            \\d+
            
            (?=\\D)
            """, re.VERBOSE | re.MULTILINE)

        roman_mix_regex = re.compile("""
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
            """, re.VERBOSE | re.MULTILINE)

        arabic_letter_regex = re.compile("""
            (
            ^|(?<=[ .(])
            )
            
            \\w\\d+
            (?=\\D)
            """, re.VERBOSE | re.MULTILINE)

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
            return 'TypeError'
        except roman.InvalidRomanNumeralError:
            return 'InvalidRomanNumeralError'

    def get_length(metre_post_from, metre_post_to):
        """
        Receives two metre post locations.
        Returns the distance between them in metres.
        """
        return abs(metre_post_from - metre_post_to)

    def get_operating_speed(text):
        """
        Receives a text.
        Returns the int between two brackets.
        """
        return round(int(re.findall(r'(?<=\().*(?=\))', text)[0]))

    def get_reduced_speeds(text):
        """
        Receives a text.

        Returns the reduced speeds in km/h for MUs and locomotives.
        """
        if text.find('/') == -1:
            return \
                    [round(
                        int(
                            re.findall(r'.*(?= \()', text)[0]))] \
                    * 2
        else:
            return \
                round(
                    int(
                        re.findall(r'.*(?=/)', text)[0])), \
                    round(
                        int(
                            re.findall(r'(?<=/).*(?= )', text)[0]))

    def convert_date_to_iso(text):
        """
        Receives a date (and time) as a str.
        Returns the text converted to ISO format.
        """
        if type(text) is str:
            try:
                return datetime.strptime(text, '%Y.%m.%d %H:%M').replace(
                    tzinfo=ZoneInfo(key='Europe/Budapest')).isoformat()
            except TypeError:
                return 'TypeError'
            except ValueError:
                return convert_date_to_iso(re.findall(r'.*(?=-)', text)[0])
        else:  # if type(text) is datetime
            try:
                return text.date().isoformat()
            except TypeError:
                return 'TypeError'
            except ValueError:
                return 'ValueError'

    current_sr = SpeedRestriction()

    current_sr.country_iso = 'HU'
    current_sr.country_uic = country_uic_database[current_sr.country_iso]
    current_sr.company_short_name = 'MÁV'
    current_sr.company_uic = company_uic_database[current_sr.company_short_name]

    current_sr.line = row[0]
    current_sr.station_from_name = remove_space_after_hyphen(row[1])
    current_sr.station_from_uic = get_station_uic(current_sr.country_uic, current_sr.station_from_name)

    current_sr.on_station = not row[2]  # true if cell is empty
    current_sr.track_switch_source_text = row[4]
    if current_sr.on_station:
        if 'kitérő' in str(row[4]):
            current_sr.applied_to = 'switch'
            current_sr.switch_station = extract_number(row[4])
        else:
            current_sr.applied_to = 'track'
            current_sr.track_station = extract_number(row[4])
    else:
        station_to_name = remove_space_after_hyphen(row[2])
        current_sr.station_to_uic = get_station_uic(current_sr.country_uic, station_to_name)
        current_sr.applied_to = 'track'
        if row[3] is None:
            current_sr.number_of_open_line_tracks = 1
        elif row[3] == 'bal vágány':
            current_sr. number_of_open_line_tracks = 2
            current_sr.track_open_line = 'left'
        elif row[3] == 'jobb vágány':
            current_sr.number_of_open_line_tracks = 2
            current_sr.track_open_line = 'right'
        else:
            current_sr.number_of_open_line_tracks = current_sr.track_open_line = 'UnknownError'

    current_sr.in_timetable = in_timetable
    current_sr.metre_post_from = int(row[5] * 100)
    current_sr.metre_post_to = int(row[6] * 100)
    current_sr.length = get_length(current_sr.metre_post_from, current_sr.metre_post_to)
    current_sr.operating_speed = get_operating_speed(row[8])
    current_sr.reduced_speed_for_locomotives, reduced_speed_for_mus = get_reduced_speeds(row[8])
    current_sr.internal_id = row[10]
    current_sr.time_from = convert_date_to_iso(row[11])
    current_sr.cause = row[12]
    current_sr.time_to = convert_date_to_iso(row[13]) if row[13] else None
    current_sr.comment = row[14]

    database.add(current_sr)
