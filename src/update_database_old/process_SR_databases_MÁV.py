from datetime import datetime
import shutil

import process_files
import file_management


def add_data(sr_excel_file_locations,
             sr_database_of_today,
             station_database):
    pass


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

    add_data(initialize_sr_excel_file_locations(),
             sr_database_of_today,
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
