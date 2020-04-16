"""
This module contains functions and utilities that extracts information from a `data` record
or from a set of `data` records.
`data` is a tuple of kind:
::

    (metadata, datetime object, par_code, par_value, flag) .
"""
from datetime import datetime, timedelta
import statistics

# -------------- GENERIC UTILITIES --------------


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
    ::

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


def compute_flag(day_records, at_least_perc):
    """
    Return (ndati, wht) where:
    ::

    * ndati: num of valid input records of the day
    * wht: 0 if num/total expected record < at_least_perc, 1 otherwise

    It assumes if dates are datetime objects we expect 24 total measures in a day, else 1.

    :param day_records: input day records
    :param at_least_perc: minimum percentage of valid data for the wht
    :return: (ndati, wht)
    """
    if not day_records:
        return 0, 0
    ndati = len([r for r in day_records if r[4] and r[3] is not None])
    num_expected = 1
    if isinstance(day_records[0][1], datetime):
        num_expected = 24
    wht = 0
    if ndati / num_expected >= at_least_perc:
        wht = 1
    return ndati, wht


# --------------   PRECIPITATION   --------------


def compute_prec24(day_records, at_least_perc=0.9, force_flag=None):
    """
    Compute "precipitazione cumulata giornaliera". It will fill the field 'prec24' of
    table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It returns the tuple (flag, val_tot, val_mx, data_mx) where:
    ::

    * flag: (ndati, wht)
    * val_tot: sum of PREC values on these records
    * val_mx: max value of PREC
    * data_mx: datetime of the max value of PREC

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of `data` objects for a day and a station.
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_tot, val_mx, data_mx)
    """
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    valid_values = [r[3] for r in day_records if r[4] and r[3] is not None]
    val_tot = None
    val_mx = None
    data_mx = None
    if valid_values:
        val_tot = sum(valid_values)
        val_mx = max(valid_values)
        data_mx = [r[1] for r in day_records if r[3] == val_mx][0]
    return flag, val_tot, val_mx, data_mx


def compute_cl_prec24(day_records):
    """
    Compute "distribuzione precipitazione cumulata giornaliera".
    It will fill the field 'cl_prec24' of table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It returns the tuple (dry, wet_01, wet_02, wet_03, wet_04, wet_05) where:
    ::

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


def compute_prec01(day_records, at_least_perc=0.9, force_flag=None):
    """
    Compute "precipitazione max cumulata su 1 ora". It will fill the field 'prec01' of
    table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It returns the tuple (flag, val_mx, data_mx) where:
    ::

    * flag: (ndati, wht)
    * val_mx: max value of PREC
    * data_mx: datetime of the max value of PREC

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of `data` objects for a hour and a station
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_mx, data_mx)
    """
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    valid_values = [r[3] for r in day_records if r[4] and r[3] is not None]
    val_mx = None
    data_mx = None
    if valid_values:
        val_mx = max(valid_values)
        data_mx = [r[1] for r in day_records if r[3] == val_mx][0]
    return flag, val_mx, data_mx


def compute_prec06(day_records, at_least_perc=0.9, force_flag=None):
    """
    Compute "precipitazione cumulata su 6 ore". It will fill the field 'prec06' of
    table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It returns the tuple (flag, val_mx, data_mx) where:
    ::

    * flag: (ndati, wht)
    * val_mx: max value of PREC (cumulated in 4 groups of same time ranges)
    * data_mx: datetime of the max value of PREC

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of `data` objects for a day and a station
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_mx, data_mx)
    """
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    if not valid_records or not isinstance(valid_records[0][1], datetime):
        return (0, 0), None, None
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
    ::

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


def compute_prec12(day_records, at_least_perc=0.9, force_flag=None):
    """
    Compute "precipitazione cumulata su 12 ore". It will fill the field 'prec12' of
    table 'sciapgg.ds__preci'.
    `day_records' is a list of `data` objects of PREC for a fixed day and a fixed station.
    It returns the tuple (flag, val_mx, data_mx) where:
    ::

    * flag: (ndati, wht)
    * val_mx: max value of PREC (cumulated in 4 groups of same time ranges)
    * data_mx: datetime of the max value of PREC

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of `data` objects for a day and a station
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_mx, data_mx)
    """
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    if not valid_records or not isinstance(valid_records[0][1], datetime):
        return (0, 0), None, None
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
    ::

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


# --------------    TEMPERATURE    --------------


def compute_temperature_flag(input_records, perc_day=0.75, perc_night=0.75,
                             daylight_hours=(9, 18)):
    """
    It compute the flag for temperature indicators, considering number of valid measures
    during the daylight and during the night. If input records have dates instead datetimes,
    it consider only the `perc_day` percentage.

    :param input_records: input records of temperature
    :param perc_day: minimum percentage of valid measures during the daylight
    :param  perc_night: minimum percentage of valid measures during the night
    :param daylight_hours: time interval of hours considered for the daylight
    :return: (ndati, wht)
    """
    valid_records = [r for r in input_records if r[4] and r[3] is not None]
    if not valid_records:
        return 0, 0
    if not isinstance(valid_records[0][1], datetime):
        return compute_flag(valid_records, at_least_perc=perc_day)
    ndati = len(valid_records)
    day_hours = range(daylight_hours[0], daylight_hours[1]+1)
    night_hours = [h for h in range(0, 24) if h not in day_hours]
    if not day_hours or not night_hours:
        # empty interval of day or night
        return ndati, 0
    day_records = [r for r in valid_records if r[1].hour in day_hours]
    night_records = [r for r in valid_records if r[1].hour in night_hours]
    data_perc_day = len(day_records) / len(day_hours)
    data_perc_night = len(night_records) / len(night_hours)
    if data_perc_day < perc_day or data_perc_night < perc_night:
        return ndati, 0
    return ndati, 1


def compute_tmdgg(day_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "media e varianza della temperatura giornaliera".
    It will fill the field 'tmdgg' of table 'sciapgg.ds__ts200'.
    It assumes day_records is of par_code='Tmedia'.
    It returns the tuple (flag, val_md, val_vr) where:
    ::

    * flag: (ndati, wht)
    * val_md: media giornaliera
    * val_vr: varianza

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of `data` objects for a day and a station.
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_md, val_vr)
    """
    flag = force_flag
    if not flag:
        flag = compute_temperature_flag(
            day_records, perc_day=at_least_perc, perc_night=at_least_perc)
    valid_values = [r[3] for r in day_records if r[4] and r[3] is not None]
    val_md = None
    val_vr = None
    if not day_records or not valid_values:
        return flag, val_md, val_vr
    val_md = statistics.mean(valid_values)
    if len(valid_values) >= 2:
        val_vr = statistics.variance(valid_values)
    return flag, val_md, val_vr


def compute_tmxgg(day_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "massimo della temperatura giornaliera".
    It will fill the field 'tmxgg' of table 'sciapgg.ds__ts200'.
    It assumes day_records is of par_code='Tmax' or par_code='Tmedia' (not both).
    It returns the tuple (flag, val_md, val_vr, val_x, data_x) where:
    ::

    * flag: (ndati, wht)
    * val_md: media giornaliera
    * val_vr: varianza
    * val_x: valore massimo giornaliero
    * data_x: data del valore massimo giornaliero

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of `data` objects for a day and a station.
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_md, val_vr, val_x, data_x)
    """
    flag = force_flag
    if not flag:
        flag = compute_temperature_flag(
            day_records, perc_day=at_least_perc, perc_night=at_least_perc)
    val_md = None
    val_vr = None
    val_x = None
    data_x = None
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    if not valid_records:
        return flag, val_md, val_vr, val_x, data_x
    values = [r[3] for r in valid_records]
    val_md = statistics.mean(values)
    if len(values) >= 2:
        val_vr = statistics.variance(values)
    val_x = max(values)
    data_x = [r[1] for r in valid_records if r[3] == val_x][0]
    return flag, val_md, val_vr, val_x, data_x


def compute_tmngg(day_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "minimo della temperatura giornaliera".
    It will fill the field 'tmngg' of table 'sciapgg.ds__ts200'.
    It assumes day_records is of par_code='Tmin' or par_code='Tmedia' (not both).
    It returns the tuple (flag, val_md, val_vr, val_x, data_x) where:
    ::

    * flag: (ndati, wht)
    * val_md: media giornaliera
    * val_vr: varianza
    * val_x: valore minimo giornaliero
    * data_x: data del valore minimo giornaliero

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of `data` objects for a day and a station.
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_md, val_vr, val_x, data_x)
    """
    flag = force_flag
    if not flag:
        flag = compute_temperature_flag(
            day_records, perc_day=at_least_perc, perc_night=at_least_perc)
    val_md = None
    val_vr = None
    val_x = None
    data_x = None
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    if not valid_records:
        return flag, val_md, val_vr, val_x, data_x
    values = [r[3] for r in valid_records]
    val_md = statistics.mean(values)
    if len(values) >= 2:
        val_vr = statistics.variance(values)
    val_x = min(values)
    data_x = [r[1] for r in valid_records if r[3] == val_x][0]
    return flag, val_md, val_vr, val_x, data_x


# --------------     PRESSURE     ---------------


def compute_press(day_records_pmedia, day_records_pmax, day_records_pmin, at_least_perc=0.75,
                  force_flag=None):
    """
    Compute "pressione atmosferica media, massima e minima".
    It will fill the field 'press' of table 'sciapgg.ds__press'.
    It assumes day_records is of the same station and day.
    It returns the tuple (flag, val_md, val_vr, val_mx, val_mn) where:
    ::
    
    * flag: (ndati, wht)
    * val_md: media giornaliera
    * val_vr: varianza
    * val_mx: valore massimo giornaliero
    * val_mn: valore minimo giornaliero

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records_pmedia: list of `data` objects of Pmedia
    :param day_records_pmax: list of `data` objects of Pmax
    :param day_records_pmin: list of `data` objects of Pmin
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_md, val_vr, val_mx, val_mn)
    """
    flag = force_flag
    if not flag:
        # flag computed from Pmedia
        flag = compute_flag(day_records_pmedia, at_least_perc)
    pmedia_values = [r[3] for r in day_records_pmedia if r[4] and r[3] is not None]
    pmax_values = [r[3] for r in day_records_pmax if r[4] and r[3] is not None]
    pmin_values = [r[3] for r in day_records_pmin if r[4] and r[3] is not None]
    val_md = None
    val_vr = None
    val_mx = None
    val_mn = None
    if pmedia_values:
        val_md = statistics.mean(pmedia_values)
        val_mx = max(pmedia_values)
        val_mn = min(pmedia_values)
        if len(pmedia_values) >= 2:
            val_vr = statistics.variance(pmedia_values)
    if pmax_values:
        val_mx = max(pmax_values)
    if pmin_values:
        val_mn = min(pmin_values)
    return flag, val_md, val_vr, val_mx, val_mn
