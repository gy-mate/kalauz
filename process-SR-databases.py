"""
A program for **making** MÁV's internal, really **dirty database** of speed restrictions **computer-processable.**

Abbreviations:
_SR = speed restriction_,
_TSR = temporary speed restriction_,
_ASR = all speed restrictions_;
_MU = multiple unit_.

Needs:
_current SR databases_ in an openpyxl module workbook format, saved as a pickle dump* at _data/SR_lists/_;
_main database of SRs_ in .csv format at _export/_database_ASR.csv_ (optional);
a helper _database of stations_ in .csv format, incl. their names and UIC codes at _data/station_lists/stations_HU.csv_;
a helper _database of UIC member railway companies_ in .csv format, incl. their names and UIC codes at _data/UIC_lists/company_UIC_list.csv_;
a helper _database of UIC member countries_ in .csv format, incl. their names, ISO and UIC codes at _data/UIC_lists/country_UIC_list.csv_.
"""

from datetime import datetime  # built-in
import shutil  # built-in

import process_files
import file_management


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
    """
    tsr_excel_file_location = 'data/SR_lists/MÁV_220202_TSR.xlsx'
    asr_excel_file_location = 'data/SR_lists/MÁV_220202_ASR.xlsx'
    sr_file_locations = [tsr_excel_file_location, asr_excel_file_location]
    sr_database_today = set()
    print('Variables initialized!')

    country_uic_database = process_files.import_csv(file_location='data/UIC_lists/country_UIC_list.csv',
                                                    index_col_loc=1,
                                                    values_col_loc=0)
    company_uic_database = process_files.import_csv(file_location='data/UIC_lists/company_UIC_list.csv',
                                                    index_col_loc=1,
                                                    values_col_loc=0)
    station_database = process_files.import_csv(file_location='data/station_lists/stations_HU.csv',
                                                index_col_loc=0,
                                                values_col_loc=4)
    print('All helper databases imported!')

    for i, file_location in enumerate(sr_file_locations):
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
                                               sr_database_today,
                                               is_it_temporary,
                                               country_uic_database,
                                               company_uic_database,
                                               station_database)
        print(f'{file_location} imported to sr_database_today!')
    print(f'{len(sr_database_today)} SRs from all .xlsx files imported to sr_database_today!')

    short_date_today = datetime.now().date()
    file_management.write_object_list_to_csv(file_location=f'export/{short_date_today}_ASR.csv',
                                             list_of_objects=sr_database_today)

    try:
        sr_database = file_management.read_objects_csv('export/_database_ASR.csv')

        file_management.mark_eliminated_srs(where=sr_database,
                                            reference=sr_database_today)
        file_management.add_new_srs(from_where=sr_database_today,
                                    to=sr_database)

        file_management.write_object_list_to_csv(file_location='export/_database_ASR.csv',
                                                 list_of_objects=sr_database)
    except FileNotFoundError:
        print(f'_database_ASR.csv was not found, copying {short_date_today}_ASR.csv to serve as one...')
        shutil.copyfile(f'export/{short_date_today}_ASR.csv',
                        'export/_database_ASR.csv')
        print('...copying done!')

    print('All done!')


main()
