

def upsert_stations(dburi, stations_path, report_path=None):
    """
    Load a list of stations from a CSV file located at `stations_path` and insert them
    in the database located at `dburi`. If stations already exists, they will be updated
    according to the CSV file.
    Returns a list of report messages and the ids of the updated/inserted stations.

    :param dburi: db connection string
    :param stations_path: CSV path of the input stations
    :param report_path: path of the output report
    :return: msgs, upserted_ids
    """
    # TODO
    return [], []