import logger
from ast import literal_eval
import csv
from datetime import datetime

import process_files


def auto_type(text):
    """
    Receives a text.
    Returns the data with the proper data type.
    """
    try:
        return literal_eval(text)
    except ValueError:
        return text  # keep string as string
    except SyntaxError:
        return None if text == '' else text  # keep zero padded int as string


def read_objects_csv(file_location):
    """
    Receives a .csv file's location.
    Returns a set of each row as a `SpeedRestriction` object.
    """
    with open(file_location, mode='r+') as file:
        sr_database = set()
        attributes = process_files.SpeedRestriction().__dict__.keys()
        for row in csv.reader(file):
            current_sr = process_files.SpeedRestriction()
            for attribute, value in zip(attributes, row):
                value = auto_type(value)
                setattr(current_sr, attribute, value)
            sr_database.add(current_sr)
    logging.info(f'{file_location} with {len(sr_database)} items was found and imported!')
    return sr_database


def mark_eliminated_srs(where, reference):
    """
    Receives two sets of `SpeedRestriction` objects.
    Marks the first set's `SpeedRestriction` objects as eliminated if not found in the other set.
    """
    i = 0
    iso_date_today = datetime.now().date().isoformat()

    # for i, sr in enumerate(where):
    #     print(f'where{i} hash: {hash(sr)}')
    # for i, sr in enumerate(reference):
    #     print(f'reference{i} hash: {hash(sr)}')

    eliminated_srs = where - reference
    for sr in eliminated_srs:
        if sr.time_to is None:  # do not overwrite already eliminated SRs
            sr.time_to = iso_date_today
            i += 1
    logging.info(f'{i} SRs marked as eliminated!')


def add_new_srs(from_where, to):
    """
    Receives two sets.
    Adds all items from the first one to the other.
    """
    previous_length = len(to)
    for sr in from_where:
        to.add(sr)
    logging.info(f'{len(to) - previous_length} new SRs added to sr_database from sr_database_today!')


def write_object_list_to_csv(file_location, list_of_objects):
    """
    Receives a file location and a set of objects.
    Writes the set to the file location as .csv
    """
    with open(file_location, mode='w') as file:
        writer = csv.writer(file)
        for instance_of_object in list_of_objects:
            writer.writerow(list(instance_of_object))
    logging.info(f'{file_location} with {len(list_of_objects)} items saved!')
