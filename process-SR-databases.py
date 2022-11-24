"""
A program for **making** MÁV's internal, really **dirty database** of speed restrictions **computer-processable.**

Receives:
several _speed restriction databases_ in Excel formats;
a .csv _database of stations,_ incl. their names and UIC codes;
a .csv _database of UIC member railway companies,_ incl. their names and UIC codes;
a .csv _database of UIC member countries,_ incl. their names, ISO and UIC codes.

Returns a neat, computer-processable _.csv database of all speed restrictions_ with extended contents.

Abbreviations:
_SR = speed restriction_;
_TSR = temporary speed restriction_;
_ASR = all speed restrictions_.
"""

from openpyxl import load_workbook
import pandas as pd
from pandas import DataFrame
import re
import roman
import numpy as np
from natsort import index_realsorted


def import_csv(file_location, index_col_loc):
    """
    Receives a .csv file's location and the index column's number.
    Returns the .csv file's content as a `pandas` `DataFrame`.
    """
    print(file_location + ' will now be imported!')
    return pd.read_csv(file_location, sep=';', index_col=index_col_loc, dtype=str)


def import_worksheets(excel_file_location):
    """
    Receives an _Excel file location_ and a _database_.

    Imports an _Excel file_ using `openpyxl`.
    Appends _all worksheets_ from the _Excel file_ to the _database_.

    Returns the _database_.
    """
    excel_workbook = load_workbook(excel_file_location)
    print(excel_file_location + ' loaded!')

    current_sr_file_openpyxl = []
    for excel_worksheet in excel_workbook.worksheets:
        current_sr_file_openpyxl.append(excel_worksheet)
    print('All worksheets imported from' + excel_file_location + ' to current_sr_file_openpyxl!')
    return current_sr_file_openpyxl


def check_permanence(cell):
    """
    Receives an Excel file's _cell_ imported via `openpyxl`.
    Returns _if that cell is bold_ using `openpyxl`.
    """
    return cell.font.bold


def should_be_imported(import_all, is_it_temporary):
    """
    Checks if the current SR to import should already have been imported, therefore if it should be done now.
    """
    return import_all is True or is_it_temporary is False


class SpeedRestriction:
    """
    Stores all data of a speed restriction.
    """
    def __init__(self,
                 country_iso,
                 country_uic,
                 company_name,
                 company_uic,
                 line,
                 station_from_name,
                 station_from_uic,
                 station_to_name,
                 station_to_uic,
                 on_station,
                 sr_applied_to,
                 track_open_line,
                 location_old,
                 track_station,
                 switch_station,
                 in_timetable,
                 location_from,
                 location_to,
                 length,
                 speed,
                 length_time,
                 internal_id,
                 time_from,
                 cause,
                 time_to,
                 comment):
        self.country_iso = country_iso
        self.country_uic = country_uic
        self.company_name = company_name
        self.company_uic = company_uic
        self.line = line
        self.station_from_name = station_from_name
        self.station_from_uic = station_from_uic
        self.station_to_name = station_to_name
        self.station_to_uic = station_to_uic
        self.on_station = on_station
        self.sr_applied_to = sr_applied_to
        self.track_open_line = track_open_line
        self.location_old = location_old
        self.track_station = track_station
        self.switch_station = switch_station
        self.in_timetable = in_timetable
        self.location_from = location_from
        self.location_to = location_to
        self.length = length
        self.speed = speed
        self.length_time = length_time
        self.internal_id = internal_id
        self.time_from = time_from
        self.cause = cause
        self.time_to = time_to
        self.comment = comment


def add_data(row,
             pre_df_db,
             in_timetable,
             country_uic_database,
             company_uic_database,
             station_database):
    """
    Receives a _SR's row,_
    a _database_,
    if the SR is _in the timetables,_
    the .csv _database of UIC member countries,_
    the .csv _database of UIC member railway companies,_
    the .csv _database of stations,_.

    Processes all data in the row.

    Appends the _processed row_ to the _database._
    """
    convert_cells_to_values(row)

    country_iso = 'HU'
    row[0:0] = [country_iso]

    country_uic = lookup_data(country_uic_database, country_iso, 'Numerical code')
    row[1:1] = [country_uic]

    company_short_name = 'MÁV'
    row[2:2] = [company_short_name]

    company_uic = lookup_data(company_uic_database, company_short_name, 'code')
    row[3:3] = [company_uic]

    row[5] = re.sub(r'(?<=\w)- (?=\w)', '-', str(row[5]))

    try:
        row[6:6] = re.search(r'(?<=HU).*',
                             lookup_data(station_database, row[5], 'PLC kód'))[0]
    except TypeError:
        row[6:6] = 'TypeError'

    on_station = not row[7]
    station_to_uic = track_station = switch_station = None
    if on_station is False:
        row[7] = re.sub(r'(?<=\w)- (?=\w)', '-', str(row[7]))
        try:
            station_to_uic = re.search(r'(?<=HU).*',
                                       lookup_data(station_database, row[7], 'PLC kód'))[0]
        except TypeError:
            station_to_uic = 'TypeError'
        sr_applied_to = 'track'
        if row[8] == 'bal vágány':
            row[8] = 'left'
        elif row[8] == 'jobb vágány':
            row[8] = 'right'
        elif row[8] is None:
            row[8] = 'single'
        else:
            row[8] = 'error'

        # TODO: save number of tracks to separate column

    else:
        row[8] = None
        if str(row[9]).find('kitérő') != -1:
            sr_applied_to = 'switch'
            switch_station = extract_number(str(row[9]))
        else:
            sr_applied_to = 'track'
            track_station = extract_number(str(row[9]))

    # TODO: save roman numerals as well
    # TODO: track's UIC code
    # TODO: SR's direction
    # TODO: SR's permanence

    row[8:8] = [station_to_uic]
    row[9:9] = [on_station]
    row[10:10] = [sr_applied_to]

    row[13:13] = [track_station]
    row[14:14] = [switch_station]
    row[15:15] = [in_timetable]

    # TODO: megszüntetés (tervezett) dátuma

    pre_df_db.append(row)
    print('All rows added to pre_df_db!')


def convert_cells_to_values(row):
    """
    Receives _cells_ of an Excel file's row imported via `openpyxl`.
    Returns the same row but with just _each cell's values_.
    """
    for idx_02, cell in enumerate(row):
        row[idx_02] = cell.value


def lookup_data(database, old_data, column_name):
    """
    Receives a _text,_ a _database,_ and a _column name_ to look for.
    Returns the _text_ in the same row's searched column where the input text was _found._
    Raises an error if searched data could not be found.
    """
    try:
        return str(database.at[old_data, column_name])
    except KeyError:
        print('Error: ' + old_data + ' could not be converted to ' + column_name + '!')
        return None


def extract_number(text):
    """
    Receives a text.

    Initializes *regex search expressions* for arabic and roman numbers with several combinations
    that could be found in the database.

    Searches for an _arabic number._
    If found, returns it.
    If not found, searches for a _roman number._

    If found, converts it to an _arabic number_ via `roman` and returns it.
    If this doesn't succeed, returns an `InvalidRomanNumeralError`.
    If not found, searches for an _arabic number with letters._

    If found, returns it.
    If not found, returns a `TypeError`.
    """
    arabic_number_regex = r'((?:^)|(?<= |\.|\())' \
                          r'\d+' \
                          r'(?=\D)'
    find_roman_number_regex = r'((?:^)|(?<= |\.|\())' \
                              r'((M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))' \
                              r'|(M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))\/[a-z]' \
                              r'|(M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))[a-z])' \
                              r'(?=\.| )'
    get_roman_number_regex = r'M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})'
    letter_and_arabic_number_regex = r'((?:^)|(?<= |\.|\())\w\d+(?=\D)'

    # TODO: InvalidRomanNumeralError:
    #  "Chinoin" vontató vágány és a kapcsolódó
    #  VII/a. sz. vágány
    #  XIVa.vg.
    #  B II/a. vágány

    # TODO: TypeError:
    #  "T" vágány
    #  AUDI kihúzóvágány
    #  Duna vontatóvágány
    #  "Amerikai" kihúzó vágány
    #  "Újkezelő" vágány
    #  Józsefvárosi összekötő vágány
    #  "Házi" csonka vágány
    #  Homlokrakodó csonka vágány
    #  MOL kihúzó
    #  Fűtőház vágány

    try:
        if re.search(arabic_number_regex, text, re.MULTILINE) is not None:
            return re.search(arabic_number_regex, text, re.MULTILINE)[0]
        else:
            if re.search(find_roman_number_regex, text, re.MULTILINE) is not None:
                if re.search(find_roman_number_regex, text, re.MULTILINE)[0] != '':
                    roman_number = re.search(find_roman_number_regex, text, re.MULTILINE)[0]
                    return roman.fromRoman(roman_number)
                else:
                    return re.search(letter_and_arabic_number_regex, text, re.MULTILINE)[0]
            else:
                return re.search(letter_and_arabic_number_regex, text, re.MULTILINE)[0]
    except TypeError:
        return 'TypeError'
    except roman.InvalidRomanNumeralError:
        return 'InvalidRomanNumeralError'


def create_dataframe(database):
    """
    Creates a `DataFrame` from a list database.
    """
    print('database will now be converted to a DataFrame!')
    return DataFrame(database)


def rename_columns(full_df):
    """
    Renames columns in a `DataFrame` to match updated content.
    """
    full_df.columns = ['country_ISO',
                       'country_UIC',
                       'company_name',
                       'company_UIC',
                       'line',
                       'station_from_name',
                       'station_from_uic',
                       'station_to_name',
                       'station_to_uic',
                       'on_station',
                       'sr_applied_to',
                       'track_open-line',
                       'place_old',
                       'track_station',
                       'switch_station',
                       'in_timetable',
                       'location_from',
                       'location_to',
                       'length',
                       'speed',
                       'length_time',
                       'id',
                       'time_from',
                       'cause',
                       'time_to',
                       'comment']
    print('full_df\'s columns renamed!')


def drop_columns(full_df):
    """
    Drops unnecessary columns from a `DataFrame`.
    """
    full_df.drop(columns=['length',
                          'length_time'])
    print('Unnecessary columns dropped from full_df!')


def edit_full_df(full_df):
    """
    Calls `rename_columns` and `drop_columns`.

    Removes leftover rows from the Excel import.

    Sorts the database.
    """
    rename_columns(full_df)
    drop_columns(full_df)
    full_df.query("line != 'Vonal'", inplace=True)
    full_df.query("location_from != 'Hossz (m):'", inplace=True)
    full_df['line'] = full_df['line'].astype(str)
    full_df.sort_values(by=['line', 'location_from'],
                        key=lambda x: np.argsort(
                            index_realsorted(
                                zip(full_df['line'],
                                    full_df['location_from']))),
                        inplace=True)
    print('full_df edited!')


def save_file(full_df, save_path):
    """
    Saves the input `DataFrame` in .csv to the input path.
    """
    full_df.to_csv(save_path, index=False)
    print('full_df saved to ' + save_path + '!')


def main():
    """
    Initializes external database locations.

    Imports SR databases via `import_worksheets`.
    Creates a list database from each file via `create_database`,
    taking in account if all SRs should be imported from them.
    Converts the list database to a dataframe via `create_dataframe`.
    Edits the dataframe via `edit_full_df`.
    Saves the dataframe via `save_file`.
    """
    tsr_excel_file_location = 'data/SR_lists/MÁV_220202_TSR.xlsx'
    asr_excel_file_location = 'data/SR_lists/MÁV_220202_ASR.xlsx'
    sr_file_locations = [tsr_excel_file_location, asr_excel_file_location]
    pre_df_db = []
    print('Variables initialized!')

    country_uic_database = import_csv('data/UIC_lists/country_UIC_list.csv', 1)
    company_uic_database = import_csv('data/UIC_lists/company_UIC_list.csv', 1)
    station_database = import_csv('data/station_lists/stations_HU.csv', 0)

    for i, file_location in enumerate(sr_file_locations):
        current_sr_file_openpyxl = import_worksheets(file_location)
        if i == 0:    # handle duplicated SRs
            import_all = True
        else:
            import_all = False
        for excel_worksheet in current_sr_file_openpyxl:
            for row in excel_worksheet.iter_rows(min_row=2):    # get rows after the header
                row = list(row)     # convert from tuples
                is_it_temporary = check_permanence(row[0])
                if should_be_imported(import_all, is_it_temporary):
                    add_data(row,
                             pre_df_db,
                             is_it_temporary,
                             country_uic_database,
                             company_uic_database,
                             station_database)
                    del pre_df_db[-1]   # delete footer row

    full_df = create_dataframe(pre_df_db)
    edit_full_df(full_df)
    print('full_df done!')

    save_path = 'export/_all_220202_ASR.csv'
    save_file(full_df, save_path)

    print('All done!')


main()
