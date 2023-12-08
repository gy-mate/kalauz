"""
A program for **making** MÁV's internal, really **dirty database** of speed restrictions **computer-processable.**

Abbreviations:
_SR = speed restriction_,
_TSR = temporary speed restriction_,
_ASR = all speed restrictions_;
_MU = multiple unit_.

Needs:
# TODO: add needs
"""

import logging
from datetime import datetime
import shutil

import process_files
import file_management


def initialize_logging():
    """
    Initializes logging.

    :return: None
    """
    logging.basicConfig(filename='process_SR_databases_MÁV.log',
                        encoding='utf-8',
                        format='%(asctime)s – %(levelname)s@%(name)s: %(message)s',
                        level=logging.DEBUG)


def initialize_sr_excel_file_locations():
    """
    Initializes _current SR database_ file locations.

    :return: list of file locations
    """
    tsr_excel_file_location = 'data/01_import/SR_lists/MÁV_220202_TSR.xlsx'
    asr_excel_file_location = 'data/01_import/SR_lists/MÁV_220202_ASR.xlsx'
    return [tsr_excel_file_location, asr_excel_file_location]


def import_country_uic_database():
    """
    Imports _database of UIC member countries_.

    :return: _database of UIC member countries_
    """
    return process_files.import_csv(file_location='data/01_import/UIC_lists/country_UIC_list.csv',
                                    index_col_loc=1,
                                    values_col_loc=0)


def import_company_uic_database():
    """
    Imports _database of UIC member railway companies_.

    :return: _database of UIC member railway companies_
    """
    return process_files.import_csv(file_location='data/01_import/UIC_lists/company_UIC_list.csv',
                                    index_col_loc=1,
                                    values_col_loc=0)


def import_station_database():
    """
    Imports _database of stations_.

    :return: _database of stations_
    """
    return process_files.import_csv(file_location='data/01_import/station_lists/stations_HU.csv',
                                    index_col_loc=0,
                                    values_col_loc=4)


def add_data(sr_excel_file_locations,
             sr_database_of_today,
             country_uic_database,
             company_uic_database,
             station_database):

    for i, file_location in enumerate(sr_excel_file_locations):
        current_sr_file_openpyxl = process_files.import_worksheets(file_location)

        import_all = i == 0
        for excel_worksheet in current_sr_file_openpyxl:
            for row in excel_worksheet.iter_rows():
                row = list(row)  # convert from tuples
                is_it_temporary = process_files.check_permanence(row[0])
                if process_files.should_be_imported(import_all, is_it_temporary):
                    process_files.convert_openpyxl_cells_to_values(row)
                    if 'Vonal' not in str(row[0]) and 'Összes korlátozás:' not in str(row[1]):  # except headers and footers
                        process_files.add_data(row,
                                               sr_database_of_today,
                                               is_it_temporary,
                                               country_uic_database,
                                               company_uic_database,
                                               station_database)
        logging.info(f'{file_location} imported to sr_database_of_today!')
    logging.info(f'{len(sr_database_of_today)} SRs from all .xlsx files imported to sr_database_of_today!')


def main():
    """
    Initializes variables.
    Calls `import_csv` for _helper databases_.

    Iterates through pickle dumps;
    imports a _current SR database_ from the pickle dump;
    checks if all lines of the database should be imported;
    iterates through _current SR database's rows_ (those are the _SRs_);
    checks the _SR's_ permanence via `process_files.check_permanence`;
    decides on importing the _row_ upon that;
    converts all _items_ in row from openpyxl objects to their values;
    calls `add_data` if the _row_ is not a header or footer.

    Stores _today's date_ in ISO format.
    Calls `file_management.write_object_list_to_csv` for the _current SR database_.

    If _export/_database_ASR.csv_ was found:
    reads objects from _export/_database_ASR.csv_ via `file_management.read_objects_csv`;
    calls `file_management.mark_eliminated_srs` for _sr_database_ and _sr_database_today_;
    calls `file_management.add_new_srs` for _sr_database_today_ and _sr_database_;
    saves _sr_database_ at _export/_database_ASR.csv_ via `file_management.write_object_list_to_csv`.
    If it was not found, copies this as _export/_database_ASR.csv_.

    The exported _export/_database_ASR.csv_ is an updated, neat, computer-processable _.csv database
    of all speed restrictions_ with extended contents.

    :return: None
    """
    initialize_logging()

    sr_database_of_today = set()
    logging.info('sr_database_of_today initialized!')

    add_data(initialize_sr_excel_file_locations(),
             sr_database_of_today,
             import_country_uic_database(),
             import_company_uic_database(),
             import_station_database())

    short_date_today = datetime.now().date()
    file_management.write_object_list_to_csv(file_location=f'data/02_export/{short_date_today}_ASR.csv',
                                             list_of_objects=sr_database_of_today)

    try:
        sr_database = file_management.read_objects_csv('data/02_export/_database_ASR.csv')

        file_management.mark_eliminated_srs(where=sr_database,
                                            reference=sr_database_of_today)
        file_management.add_new_srs(from_where=sr_database_of_today,
                                    to=sr_database)

        file_management.write_object_list_to_csv(file_location='data/02_export/_database_ASR.csv',
                                                 list_of_objects=sr_database)
    except FileNotFoundError:
        logging.info(f'_database_ASR.csv was not found, copying {short_date_today}_ASR.csv to serve as one...')
        shutil.copyfile(f'data/02_export/{short_date_today}_ASR.csv',
                        'data/02_export/_database_ASR.csv')
        logging.info('...copying done!')

    logging.info('All done!')


if __name__ == '__main__':
    main()
