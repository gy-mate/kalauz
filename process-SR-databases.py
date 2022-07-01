from openpyxl import load_workbook
import pandas as pd
from pandas import DataFrame
import re
import roman


def import_worksheets(excel_file_location):
    excel_workbook = load_workbook(excel_file_location)
    print(excel_file_location + ' loaded!')
    for excel_worksheet in excel_workbook.worksheets:
        openpyxl_db_outer.append(excel_worksheet)
    print('All worksheets imported from' + excel_file_location + ' to openpyxl_db!')
    return openpyxl_db_outer


def check_permanence(cell):
    return cell.font.bold


def convert_cells_to_values(row):
    for idx_02, cell in enumerate(row):
        row[idx_02] = cell.value


def import_csv(file_location, index_col_loc):
    print(file_location + ' imported!')
    return pd.read_csv(file_location, sep=';', index_col=index_col_loc, dtype=str)


def lookup_data(database, old_data, column_name):
    try:
        new_data = str(database.at[old_data, column_name])
    except KeyError:
        print('Error: ' + old_data + ' could not be converted to ' + column_name + '!')
        new_data = None
    return new_data


def extract_number(text):
    arabic_number_regex = r'((?:^)|(?<= |\.|\())' \
                          r'\d+' \
                          r'(?=\D)'
    find_roman_number_regex = r'((?:^)|(?<= |\.|\())' \
                              r'((M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))' \
                              r'|(M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))/[a-z]' \
                              r'|(M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3}))[a-z])' \
                              r'(?=\.| )'
    get_roman_number_regex = r'M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})'
    letter_and_arabic_number_regex = r'((?:^)|(?<= |\.|\())\w\d+(?=\D)'
    try:
        if re.search(arabic_number_regex, text, re.MULTILINE) is not None:
            arabic_number = re.search(arabic_number_regex, text, re.MULTILINE)[0]
        else:
            if re.search(find_roman_number_regex, text, re.MULTILINE) is not None:
                if re.search(find_roman_number_regex, text, re.MULTILINE)[0] != '':
                    roman_number = re.search(get_roman_number_regex, text, re.MULTILINE)[0]
                    arabic_number = roman.fromRoman(roman_number)
                else:
                    arabic_number = re.search(letter_and_arabic_number_regex, text, re.MULTILINE)[0]
            else:
                arabic_number = re.search(letter_and_arabic_number_regex, text, re.MULTILINE)[0]
    except TypeError:
        arabic_number = 'TypeError'
    except roman.InvalidRomanNumeralError:
        arabic_number = 'InvalidRomanNumeralError'
    return arabic_number


def add_data(row,
             pre_df_db,
             in_timetable,
             country_uic_database,
             company_uic_database,
             station_database):
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
    from_station_uic = lookup_data(station_database, row[5], 'statisztikai szám')
    row[6:6] = [from_station_uic]

    on_station = not row[7]
    station_to_uic = track_station = switch_station = None
    if on_station is False:
        row[7] = re.sub(r'(?<=\w)- (?=\w)', '-', str(row[7]))
        station_to_uic = lookup_data(station_database, row[7], 'statisztikai szám')
        sr_applied_to = 'track'
        if row[8] == 'bal vágány':
            row[8] = 'left'
        elif row[8] == 'jobb vágány':
            row[8] = 'right'
        else:
            row[8] = 'error'
    else:
        row[8] = None
        if str(row[9]).find('kitérő') != -1:
            sr_applied_to = 'switch'
            switch_station = extract_number(str(row[9]))
        else:
            sr_applied_to = 'track'
            track_station = extract_number(str(row[9]))

    row[8:8] = [station_to_uic]
    row[9:9] = [on_station]
    row[10:10] = [sr_applied_to]

    row[13:13] = [track_station]
    row[14:14] = [switch_station]
    row[15:15] = [in_timetable]

    pre_df_db.append(row)


def create_database(openpyxl_db,
                    pre_df_db,
                    import_all,
                    country_uic_database_file_location,
                    company_uic_database_file_location,
                    station_database_file_location):
    country_uic_database = import_csv(country_uic_database_file_location, 1)
    company_uic_database = import_csv(company_uic_database_file_location, 1)
    station_database = import_csv(station_database_file_location, 1)
    for excel_worksheet in openpyxl_db:
        # get rows after the header
        for row in excel_worksheet.iter_rows(min_row=2):
            row = list(row)
            is_it_temporary = check_permanence(row[0])
            if import_all is True or is_it_temporary is False:
                add_data(row,
                         pre_df_db,
                         is_it_temporary,
                         country_uic_database,
                         company_uic_database,
                         station_database)

        # delete footer row
        del pre_df_db[-1]
    print('All rows added to pre_df_db!')
    return pre_df_db


def create_dataframe(pre_df_db):
    full_df = DataFrame(pre_df_db)
    print('pre_df_db converted to dataframe!')
    return full_df


def rename_columns(full_df):
    column_names = ['country_ISO',
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
    full_df.columns = column_names
    print('Columns renamed!')
    return full_df


def drop_columns(full_df):
    full_df = full_df.drop(columns=['length',
                                    'length_time'])
    print('Unnecessary columns dropped from full_df!')
    return full_df


def edit_full_df(full_df):
    full_df = rename_columns(full_df)
    full_df = drop_columns(full_df)
    print('full_df edited!')
    return full_df


def save_file(full_df, save_path):
    full_df.to_csv(save_path, index=False)
    print('full_df saved to ' + save_path + '!')


folder_location = '/Users/gymate1/Library/Mobile Documents/com~apple~CloudDocs/Menetirány/lassújelek'
TSR_excel_file_location = folder_location + '/01_source/data/SR_lists/MÁV_220202_TSR.xlsx'
ASR_excel_file_location = folder_location + '/01_source/data/SR_lists/MÁV_220202_ASR.xlsx'
SR_file_locations = [TSR_excel_file_location, ASR_excel_file_location]
pre_df_db_outer = []
country_uic_database_file_location_outer = folder_location + '/01_source/data/UIC_lists/country_UIC_list.csv'
company_uic_database_file_location_outer = folder_location + '/01_source/data/UIC_lists/company_UIC_list.csv'
station_database_file_location_outer = folder_location + '/01_source/data/railway-line_lists/railway-lines_HU.csv'
print('Variables initialized!')

for idx, file_location_outer in enumerate(SR_file_locations):
    openpyxl_db_outer = []
    openpyxl_db_outer = import_worksheets(SR_file_locations[idx])
    if idx == 0:
        import_all_outer = True
    else:
        import_all_outer = False
    pre_df_db_outer = create_database(openpyxl_db_outer,
                                      pre_df_db_outer,
                                      import_all_outer,
                                      country_uic_database_file_location_outer,
                                      company_uic_database_file_location_outer,
                                      station_database_file_location_outer)

full_df_outer = create_dataframe(pre_df_db_outer)
full_df_outer = edit_full_df(full_df_outer)
print('full_df done!')

new_file_name = '/03_export/_all_220202_ASR.csv'
save_path_outer = folder_location + new_file_name
save_file(full_df_outer, save_path_outer)

print('All done!')
