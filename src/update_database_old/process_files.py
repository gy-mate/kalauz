import csv

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
