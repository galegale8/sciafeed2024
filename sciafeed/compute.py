"""
This module contains functions and utilities that extracts information from a `data` record
or from a set of `data` records.
`data` is a tuple of kind (metadata, datetime object, par_code, par_value, flag) .
"""
from datetime import datetime, timedelta


def sum_records_by_hour_groups(day_records, hours_interval):
    """
    Return a new set of records obtaining sum of values if they belong to the same
    groups of hours in the day. Each time group lasts `hours_interval` hours.
    It assume all the records are valid and values are not None and of the same par_code.

    :param day_records: list of records of the same day
    :param hours_interval: interval in hours of the splitting of a day
    :return: the new set of records splitted by the hour
    """
    if not day_records:
        return []
    metadata = day_records[0][0]
    day = day_records[0][1]
    par_code = day_records[0][2]
    start_time = datetime(day.year, day.month, day.day, 0, 0)
    new_records = []
    for min_hour in range(0, 24, hours_interval):
        max_hour = min_hour + hours_interval
        subrecords = [r for r in day_records if min_hour <= r[1].hour < max_hour]
        if not subrecords:
            continue
        tot = sum([s[3] for s in subrecords])
        new_time = start_time + timedelta(hours=min_hour)
        new_record = (metadata, new_time, par_code, tot, True)
        new_records.append(new_record)
    return new_records


def wet_distribution(input_records):
    """
    It returns the tuple (dry, wet_01, wet_02, wet_03, wet_04, wet_05) where:
    * dry: num of input records with PREC <= 1
    * wet_01: num of records with PREC in ]1, 5]
    * wet_02: num of records with PREC in ]5, 10]
    * wet_03: num of records with PREC in ]10, 20]
    * wet_04: num of records with PREC in ]20, 50]
    * wet_05: num of records with PREC > 50
    It assumes input records are all of PREC, same station, valid and with not null values.

    :param input_records: input records of PREC
    :return: (dry, wet_01, wet_02, wet_03, wet_04, wet_05)
    """
    dry = len([d[3] for d in input_records if d[3] <= 1])
    wet_01 = len([d[3] for d in input_records if 1 < d[3] <= 5])
    wet_02 = len([d[3] for d in input_records if 5 < d[3] <= 10])
    wet_03 = len([d[3] for d in input_records if 10 < d[3] <= 20])
    wet_04 = len([d[3] for d in input_records if 20 < d[3] <= 50])
    wet_05 = len([d[3] for d in input_records if d[3] > 50])
    return dry, wet_01, wet_02, wet_03, wet_04, wet_05


def compute_prec24(day_records, at_least_perc=0.75):
    """
    Compute "precipitazione cumulata giornaliera". It will fill the field 'prec24' of
    table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It returns the tuple (flag, val_tot, val_mx, data_mx) where:
    * flag: (ndati, wht)
    * val_tot: sum of PREC values on these records
    * val_mx: max value of PREC
    * data_mx: datetime of the max value of PREC
    If num of valid values/24 >= `at_least_perc` --> wht = 1, otherwise 0

    :param day_records: list of `data` objects for a day and a station.
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, val_tot, val_mx, data_mx)
    """
    if not day_records:
        return (0, 0), None, None, None
    valid_values = [r[3] for r in day_records if r[4] and r[3] is not None]
    ndati = len(valid_values)
    val_tot = None
    val_mx = None
    data_mx = None
    wht = 0
    if ndati:
        val_tot = sum(valid_values)
        val_mx = max(valid_values)
        data_mx = [r[1] for r in day_records if r[3] == val_mx][0]
        wht = ndati / 24 >= at_least_perc and 1 or 0
    flag = (ndati, wht)
    return flag, val_tot, val_mx, data_mx


def compute_cl_prec24(day_records):
    """
    Compute "distribuzione precipitazione cumulata giornaliera".
    It will fill the field 'cl_prec24' of table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It returns the tuple (dry, wet_01, wet_02, wet_03, wet_04, wet_05) where:
    * dry: num of records with PREC <= 1
    * wet_01: num of records with PREC in ]1, 5]
    * wet_02: num of records with PREC in ]5, 10]
    * wet_03: num of records with PREC in ]10, 20]
    * wet_04: num of records with PREC in ]20, 50]
    * wet_05: num of records with PREC > 50

    :param day_records: list of `data` objects for a day and a station.
    :return: (dry, wet_01, wet_02, wet_03, wet_04, wet_05)
    """
    valid_records = [d for d in day_records if d[4] and d[3] is not None]
    return wet_distribution(valid_records)


def compute_prec01(day_records, at_least_perc=0.75):
    """
    Compute "precipitazione cumulata su 1 ora". It will fill the field 'prec01' of
    table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It returns the tuple (flag, val_mx, data_mx) where:
    * flag: (ndati, wht)
    * val_mx: max value of PREC
    * data_mx: datetime of the max value of PREC

    :param day_records: list of `data` objects for a hour and a station
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, val_mx, data_mx)
    """
    if not day_records:
        return (0, 0), None, None
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    if not valid_records:
        return (0, 0), None, None
    ndati = len(valid_records)
    wht = ndati / 24 >= at_least_perc and 1 or 0
    flag = (ndati, wht)
    # valid_records = sum_records_by_hour_groups(valid_records, 1)
    val_mx = max([r[3] for r in valid_records])
    data_mx = [r[1] for r in valid_records if r[3] == val_mx][0]
    return flag, val_mx, data_mx


def compute_prec06(day_records, at_least_perc=0.75):
    """
    Compute "precipitazione cumulata su 6 ore". It will fill the field 'prec06' of
    table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It assumes also records are referring to date times (attribute `hour` of index 1 of each
    day record).
    It returns the tuple (flag, val_mx, data_mx) where:
    * flag: (ndati, wht)
    * val_mx: max value of PREC (cumulated in 4 groups of same time ranges)
    * data_mx: datetime of the max value of PREC

    :param day_records: list of `data` objects for a day and a station
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, val_mx, data_mx)
    """
    if not day_records:
        return (0, 0), None, None
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    if not valid_records:
        return (0, 0), None, None
    ndati = len(valid_records)
    wht = ndati / 24 <= at_least_perc and 0 or 1
    flag = (ndati, wht)
    new_records = sum_records_by_hour_groups(valid_records, 6)
    val_mx = max([r[3] for r in new_records])
    data_mx = [r[1] for r in new_records if r[3] == val_mx][0]
    return flag, val_mx, data_mx


def compute_cl_prec06(day_records):
    """
    Compute "distribuzione precipitazione cumulata su 6 ore".
    It will fill the field 'cl_prec06' of table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It returns the tuple (dry, wet_01, wet_02, wet_03, wet_04, wet_05) where:
    * dry: num of records with PREC <= 1
    * wet_01: num of records with PREC in ]1, 5]
    * wet_02: num of records with PREC in ]5, 10]
    * wet_03: num of records with PREC in ]10, 20]
    * wet_04: num of records with PREC in ]20, 50]
    * wet_05: num of records with PREC > 50

    :param day_records: list of `data` objects for a day and a station.
    :return: (dry, wet_01, wet_02, wet_03, wet_04, wet_05)
    """
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    new_records = sum_records_by_hour_groups(valid_records, 6)
    return wet_distribution(new_records)


def compute_prec12(day_records, at_least_perc=0.75):
    """
    Compute "precipitazione cumulata su 12 ore". It will fill the field 'prec12' of
    table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It assumes also records are referring to date times (attribute `hour` of index 1 of each
    day record).
    It returns the tuple (flag, val_mx, data_mx) where:
    * flag: (ndati, wht)
    * val_mx: max value of PREC (cumulated in 4 groups of same time ranges)
    * data_mx: datetime of the max value of PREC

    :param day_records: list of `data` objects for a day and a station
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, val_mx, data_mx)
    """
    if not day_records:
        return (0, 0), None, None
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    if not valid_records:
        return (0, 0), None, None
    ndati = len(valid_records)
    wht = ndati / 24 <= at_least_perc and 0 or 1
    flag = (ndati, wht)
    new_records = sum_records_by_hour_groups(valid_records, 12)
    val_mx = max([r[3] for r in new_records])
    data_mx = [r[1] for r in new_records if r[3] == val_mx][0]
    return flag, val_mx, data_mx


def compute_cl_prec12(day_records):
    """
    Compute "distribuzione precipitazione cumulata su 12 ore".
    It will fill the field 'cl_prec12' of table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It returns the tuple (dry, wet_01, wet_02, wet_03, wet_04, wet_05) where:
    * dry: num of records with PREC <= 1
    * wet_01: num of records with PREC in ]1, 5]
    * wet_02: num of records with PREC in ]5, 10]
    * wet_03: num of records with PREC in ]10, 20]
    * wet_04: num of records with PREC in ]20, 50]
    * wet_05: num of records with PREC > 50

    :param day_records: list of `data` objects for a day and a station.
    :return: (dry, wet_01, wet_02, wet_03, wet_04, wet_05)
    """
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    new_records = sum_records_by_hour_groups(valid_records, 12)
    return wet_distribution(new_records)

