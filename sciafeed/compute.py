"""
This module contains functions and utilities that extracts information from a set of `data`
records.
`data` is a tuple of kind:
::

    (metadata, datetime object, par_code, par_value, flag) .
"""
from datetime import datetime, timedelta
import itertools
import operator
import statistics


ROUND_PRECISION = 1
INDICATORS_TABLES = {
    'sciapgg.ds__bagna': ['data_i', 'cod_staz', 'cod_aggr', 'bagna'],
    'sciapgg.ds__elio': ['data_i', 'cod_staz', 'cod_aggr', 'elio'],
    'sciapgg.ds__preci': ['data_i', 'cod_staz', 'cod_aggr', 'prec24',
                          'cl_prec24', 'prec01', 'prec06', 'cl_prec06',
                          'prec12', 'cl_prec12', 'ggneve', 'storm', 'ggstorm'],
    'sciapgg.ds__press': ['data_i', 'cod_staz', 'cod_aggr', 'press'],
    'sciapgg.ds__radglob': ['data_i', 'cod_staz', 'cod_aggr', 'radglob'],
    'sciapgg.ds__t200': ['data_i', 'cod_staz', 'cod_aggr', 'tmxgg',
                         'cl_tmxgg', 'tmngg', 'cl_tmngg', 'tmdgg',
                         'tmdgg1', 'deltagg', 'day_gelo', 'cl_tist',
                         't00', 't01', 't02', 't03', 't04', 't05',
                         't06', 't07', 't08', 't09', 't10', 't11',
                         't12', 't13', 't14', 't15', 't16', 't17',
                         't18', 't19', 't20', 't21', 't22', 't23'],
    'sciapgg.ds__urel': ['data_i', 'cod_staz', 'cod_aggr', 'ur', 'ur00',
                         'ur01', 'ur02', 'ur03', 'ur04', 'ur05', 'ur06',
                         'cl_ur06', 'ur07', 'ur08', 'ur09', 'ur10', 'ur11',
                         'ur12', 'cl_ur12', 'ur13', 'ur14', 'ur15', 'ur16',
                         'ur17', 'ur18', 'ur19', 'ur20', 'ur21', 'ur22', 'ur23'],
    'sciapgg.ds__vnt10': ['data_i', 'cod_staz', 'cod_aggr', 'vntmxgg',
                          'vnt', 'prs_ff', 'prs_dd', 'vntmd']
}
# -------------- GENERIC UTILITIES --------------


def group_same_day(data_record):
    """
    utility function to group all records with the same station
    and of the same day
    """
    metadata, row_date, par_code, par_value, par_flag = data_record
    station_id = (metadata.get('cod_utente'), metadata.get('cod_rete'))
    if isinstance(row_date, datetime):
        row_day = row_date.date()
    return station_id, row_day


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
    dry = len([d for d in input_records if d[3] <= 1])
    wet_01 = len([d for d in input_records if 1 < d[3] <= 5])
    wet_02 = len([d for d in input_records if 5 < d[3] <= 10])
    wet_03 = len([d for d in input_records if 10 < d[3] <= 20])
    wet_04 = len([d for d in input_records if 20 < d[3] <= 50])
    wet_05 = len([d for d in input_records if d[3] > 50])
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
    valid_values = [r[3] for r in day_records if r[4] and r[3] is not None]
    if not valid_values:
        return None
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    val_tot = None
    val_mx = None
    data_mx = None
    if valid_values:
        val_tot = round(sum(valid_values), ROUND_PRECISION)
        val_mx = max(valid_values)
        data_mx = [r[1] for r in day_records if r[3] == val_mx][0].isoformat()
        val_mx = round(val_mx, ROUND_PRECISION)
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
    if not valid_records:
        return None
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
    valid_values = [r[3] for r in day_records if r[4] and r[3] is not None]
    if not valid_values:
        return None
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    val_mx = None
    data_mx = None
    if valid_values:
        val_mx = max(valid_values)
        data_mx = [r[1] for r in day_records if r[3] == val_mx][0].isoformat()
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
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    val_mx = None
    data_mx = None
    if not valid_records or not isinstance(valid_records[0][1], datetime):
        return None
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    new_records = sum_records_by_hour_groups(valid_records, 6)
    if new_records:
        val_mx = max([r[3] for r in new_records])
        data_mx = [r[1] for r in new_records if r[3] == val_mx][0].isoformat()
        val_mx = round(val_mx)
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
    if not valid_records:
        return None
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
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    if not valid_records or not isinstance(valid_records[0][1], datetime):
        return None
    new_records = sum_records_by_hour_groups(valid_records, 12)
    if not new_records:
        return None
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    val_mx = max([r[3] for r in new_records])
    data_mx = [r[1] for r in new_records if r[3] == val_mx][0].isoformat()
    val_mx = round(val_mx, ROUND_PRECISION)
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
    if not valid_records:
        return None
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
    It will fill the field 'tmdgg' of table 'sciapgg.ds__t200'.
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
    val_vr = None
    if not valid_values:
        return None
    val_md = round(statistics.mean(valid_values), ROUND_PRECISION)
    if len(valid_values) >= 2:
        val_vr = round(statistics.stdev(valid_values), ROUND_PRECISION)
    return flag, val_md, val_vr


def compute_tmxgg(day_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "massimo della temperatura giornaliera".
    It will fill the field 'tmxgg' of table 'sciapgg.ds__t200'.
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
    val_vr = None
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    if not valid_records:
        return None
    flag = force_flag
    if not flag:
        flag = compute_temperature_flag(
            day_records, perc_day=at_least_perc, perc_night=at_least_perc)
    values = [r[3] for r in valid_records]
    val_md = round(statistics.mean(values), ROUND_PRECISION)
    if len(values) >= 2:
        val_vr = round(statistics.stdev(values), ROUND_PRECISION)
    val_x = max(values)
    data_x = [r[1] for r in valid_records if r[3] == val_x][0].isoformat()
    val_x = round(val_x, ROUND_PRECISION)
    return flag, val_md, val_vr, val_x, data_x


def compute_tmngg(day_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "minimo della temperatura giornaliera".
    It will fill the field 'tmngg' of table 'sciapgg.ds__t200'.
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
    val_vr = None
    valid_records = [r for r in day_records if r[4] and r[3] is not None]
    if not valid_records:
        return None
    flag = force_flag
    if not flag:
        flag = compute_temperature_flag(
            day_records, perc_day=at_least_perc, perc_night=at_least_perc)
    values = [r[3] for r in valid_records]
    val_md = round(statistics.mean(values), ROUND_PRECISION)
    if len(values) >= 2:
        val_vr = round(statistics.stdev(values), ROUND_PRECISION)
    val_x = min(values)
    data_x = [r[1] for r in valid_records if r[3] == val_x][0].isoformat()
    val_x = round(val_x)
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

    :param day_records_pmedia: list of `data` objects of P
    :param day_records_pmax: list of `data` objects of Pmax
    :param day_records_pmin: list of `data` objects of Pmin
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_md, val_vr, val_mx, val_mn)
    """
    pmedia_values = [r[3] for r in day_records_pmedia if r[4] and r[3] is not None]
    pmax_values = [r[3] for r in day_records_pmax if r[4] and r[3] is not None]
    pmin_values = [r[3] for r in day_records_pmin if r[4] and r[3] is not None]
    if not pmin_values and not pmax_values and not pmedia_values:
        return None
    flag = force_flag
    if not flag:
        # flag computed from P
        flag = compute_flag(day_records_pmedia, at_least_perc)
    val_md = None
    val_vr = None
    val_mx = None
    val_mn = None
    if pmedia_values:
        val_md = round(statistics.mean(pmedia_values), ROUND_PRECISION)
        val_mx = round(max(pmedia_values), ROUND_PRECISION)
        val_mn = round(min(pmedia_values), ROUND_PRECISION)
        if len(pmedia_values) >= 2:
            val_vr = round(statistics.stdev(pmedia_values), ROUND_PRECISION)
    if pmax_values:
        val_mx = round(max(pmax_values), ROUND_PRECISION)
    if pmin_values:
        val_mn = round(min(pmin_values), ROUND_PRECISION)
    return flag, val_md, val_vr, val_mx, val_mn


# ----------    BAGNATURA FOGLIARE    -----------


def compute_bagna(day_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "bagnatura fogliare giornaliera".
    It will fill the field 'bagna' of table 'sciapgg.ds__bagna'.
    It assumes day_records is of the same station and day, and values are in minutes.
    It returns the tuple (flag, val_md, val_vr, val_mx, val_mn, val_tot) where:
    ::

    * flag: (ndati, wht)
    * val_md: media giornaliera
    * val_vr: varianza
    * val_mx: valore massimo giornaliero
    * val_mn: valore minimo giornaliero
    * val_tot: totale ore

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of `data` objects of Bagnatura Fogliare
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_md, val_vr, val_mx, val_mn, val_tot)
    """
    # 'values / 60' because outputs are in hours and input in minutes
    valid_values = [r[3]/60 for r in day_records if r[4] and r[3] is not None]
    val_vr = None
    if not valid_values:
        return None
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    val_md = round(statistics.mean(valid_values), ROUND_PRECISION)
    val_mx = round(max(valid_values), ROUND_PRECISION)
    val_mn = round(min(valid_values), ROUND_PRECISION)
    val_tot = round(sum(valid_values), ROUND_PRECISION)
    if len(valid_values) >= 2:
        val_vr = round(statistics.stdev(valid_values), ROUND_PRECISION)
    return flag, val_md, val_vr, val_mx, val_mn, val_tot


# ------------      ELIOFANIA      --------------


def compute_elio(day_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "eliofania giornaliera".
    It will fill the field 'elio' of table 'sciapgg.ds__elio'.
    It assumes day_records is of the same station and day, and values are in minutes,
    and the par_code is INSOL or INSOL_00 (not both).
    It returns the tuple (flag, val_md, val_vr, val_mx) where:
    ::

    * flag: (ndati, wht)
    * val_md: somma dei valori del giorno
    * val_vr: varianza
    * val_mx: None

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of input `data` objects
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_md, val_vr, val_mx)
    """
    # 'values / 60' because outputs are in hours and input in minutes
    valid_values = [r[3]/60 for r in day_records if r[4] and r[3] is not None]
    val_vr = None
    val_mx = None
    if not valid_values:
        return None
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    val_md = round(sum(valid_values), ROUND_PRECISION)
    if len(valid_values) >= 2:
        val_vr = round(statistics.stdev(valid_values), ROUND_PRECISION)
    return flag, val_md, val_vr, val_mx


# ----------    GLOBAL RADIATION    -------------


def compute_radglob(day_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "radiazione globale media giornaliera".
    It will fill the field 'radglob' of table 'sciapgg.ds__radglob'.
    It assumes day_records is of the same station and day, and values are in cal/cm2,
    and the par_code is RADSOL.
    It returns the tuple (flag, val_md, val_vr, val_mx, val_mn) where:
    ::

    * flag: (ndati, wht)
    * val_md: media giornaliera
    * val_vr: varianza
    * val_mx: valore massimo giornaliero
    * val_mn: valore minimo giornaliero

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of input `data` objects
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_md, val_vr, val_mx, val_mn)
    """
    # 'values *0.4843' because outputs are in W/m2 and input in cal/cm2
    valid_values = [r[3]*0.4843 for r in day_records if r[4] and r[3] is not None]
    val_vr = None
    if not valid_values:
        return None
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    val_md = round(statistics.mean(valid_values), ROUND_PRECISION)
    val_mx = round(max(valid_values), ROUND_PRECISION)
    val_mn = round(min(valid_values), ROUND_PRECISION)
    if len(valid_values) >= 2:
        val_vr = round(statistics.stdev(valid_values), ROUND_PRECISION)
    return flag, val_md, val_vr, val_mx, val_mn


# -------------  RELATIVE HUMIDITY  -------------


def compute_ur(day_records_urmedia, day_records_urmax, day_records_urmin,
               at_least_perc=0.75, force_flag=None):
    """
    Compute "umidità relativa dell'aria giornaliera media, massima e minima".
    It will fill the field 'ur' of table 'sciapgg.ds__urel'.
    It assumes day_records is of the same station and day.
    It returns the tuple (flag, val_md, val_vr, flag1, val_mx, val_mn) where:
    ::

    * flag: (ndati, wht)
    * val_md: media giornaliera
    * val_vr: varianza
    * flag1: ?  TODO: ask
    * val_mx: valore massimo giornaliero
    * val_mn: valore minimo giornaliero

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records_urmedia: list of input `data` objects with par_code=UR media
    :param day_records_urmax: list of input `data` objects with par_code=UR max
    :param day_records_urmin: list of input `data` objects with par_code=UR min
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_md, val_vr, flag1, val_mx, val_mn)
    """
    urmedia_values = [r[3] for r in day_records_urmedia if r[4] and r[3] is not None]
    urmax_values = [r[3] for r in day_records_urmax if r[4] and r[3] is not None]
    urmin_values = [r[3] for r in day_records_urmin if r[4] and r[3] is not None]
    if not urmedia_values and not urmax_values and not urmin_values:
        return None
    flag = force_flag
    if not flag:
        # flag computed from UR media
        flag = compute_flag(day_records_urmedia, at_least_perc)
    val_md = None
    val_vr = None
    flag1 = (None, None)
    val_mx = None
    val_mn = None
    if urmedia_values:
        val_md = round(statistics.mean(urmedia_values), ROUND_PRECISION)
        if len(urmedia_values) >= 2:
            val_vr = round(statistics.stdev(urmedia_values), ROUND_PRECISION)
        if urmax_values:
            val_mx = round(max(urmax_values), ROUND_PRECISION)
        else:
            val_mx = round(max(urmedia_values), ROUND_PRECISION)
        if urmin_values:
            val_mn = round(min(urmin_values), ROUND_PRECISION)
        else:
            val_mn = round(min(urmedia_values), ROUND_PRECISION)
    return flag, val_md, val_vr, flag1, val_mx, val_mn

# TODO ask because sciapgg.ds__urel has more fields

# ----------------     WIND     -----------------


def compute_vntmd(day_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "velocità media giornaliera del vento".
    It will fill the field 'vntmd' of table 'sciapgg.ds__vnt10'.
    It assumes day_records is of par_code='FF'.
    It returns the tuple (flag, ff) where:
    ::

    * flag: (ndati, wht)
    * ff: media giornaliera della velocità

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_records: list of `data` objects for a day and a station.
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, ff)
    """
    valid_values = [r[3] for r in day_records if r[4] and r[3] is not None]
    if not valid_values:
        return None
    flag = force_flag
    if not flag:
        flag = compute_flag(day_records, at_least_perc)
    ff = round(statistics.mean(valid_values), ROUND_PRECISION)
    return flag, ff


def compute_wind_flag(day_ff_records, day_dd_records, at_least_perc=0.75):
    """
    Compute the flag considering a good measure if there are both FF and DD records for each time.

    :param day_ff_records: FF measures
    :param day_dd_records: DD measures
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: the flag values
    """
    all_records = sorted(day_ff_records + day_dd_records, key=operator.itemgetter(1))
    flag_records = []
    for measure_time, measures in itertools.groupby(all_records, key=operator.itemgetter(1)):
        good_measures = [m for m in measures if m[3] is not None and m[4]]
        if len(good_measures) < 2:
            flag_record = (dict(), measure_time, 'DD and FF', None, False)
        else:
            flag_record = (dict(), measure_time, 'DD and FF', 1, True)
        flag_records.append(flag_record)
    flag = compute_flag(flag_records, at_least_perc)
    return flag


def compute_vntmxgg(day_ff_records, day_dd_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "intensità e direzione massima giornaliera del vento".
    It will fill the field 'vntmxgg' of table 'sciapgg.ds__vnt10'.
    It returns the tuple (flag, ff, dd) where:
    ::

    * flag: (ndati, wht)
    * ff: massimo giornaliero della velocità
    * dd: direzione del massimo giornaliero di velocità

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_ff_records: list of `data` objects for a day and a station of par_code=FF
    :param day_dd_records: list of `data` objects for a day and a station of par_code=DD
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, ff, dd)
    """
    valid_ff_records = [r for r in day_ff_records if r[4] and r[3] is not None]
    if not valid_ff_records:
        return None
    flag = force_flag
    if not flag:
        # TODO: ask how to compute the flag
        # flag = compute_wind_flag(day_ff_records, day_dd_records, at_least_perc)
        flag = compute_flag(day_ff_records, at_least_perc)
    dd_records_times = dict([(r[1], r[3]) for r in day_dd_records if r[4] and r[3] is not None])
    ff = max([r[3] for r in valid_ff_records])
    hour_of_max = [r[1] for r in valid_ff_records if r[3] == ff][0]
    dd = dd_records_times.get(hour_of_max)
    ff = round(ff, ROUND_PRECISION)
    return flag, ff, dd


def wind_ff_distribution(input_records):
    """
    It returns the list [c1, c2, c3, c4, c5] where:
    ::

    * c1: num of records with PREC in ]0.5, 3]
    * c2: num of records with PREC in ]3, 5]
    * c3: num of records with PREC in ]5, 10]
    * c4: num of records with PREC > 10

    It assumes input records are all of FF, same station, valid and with not null values.

    :param input_records: input records of FF
    :return: [c1, c2, c3, c4, c5]
    """
    c1 = len([d for d in input_records if 0.5 < d[3] <= 3])
    c2 = len([d for d in input_records if 3 < d[3] <= 5])
    c3 = len([d for d in input_records if 5 < d[3] <= 10])
    c4 = len([d for d in input_records if d[3] > 10])
    return [c1, c2, c3, c4]


def wind_dd_partition(input_records):
    """
    It returns the list [c1, c2, c3, c4, c5,...c16] where:
    ::

        * c1: records with DD in ]0, 22.5]
        * c2: records with DD in ]22.5, 45]
        * c3: records with DD in ]45, 67.5]
        ...
        * c16: records with DD in ]337, 360]

    It assumes input records are all of DD, same station, valid and with not null values.

    :param input_records: input records of FF
    :return: [c1, c2, c3, c4, c5,...c16]
    """
    def get_sector_index(dd_record):
        if dd_record[3] == 0:
            return 15
        sector_indx = int(dd_record[3] // 22.5)
        if dd_record[3] % 22.5 == 0:
            sector_indx -= 1
        return sector_indx

    input_records = sorted(input_records, key=get_sector_index)
    ret_value = [[] for _ in range(16)]
    for sector_index, dd_measures in itertools.groupby(input_records, key=get_sector_index):
        ret_value[sector_index] = list(dd_measures)
    return ret_value


def compute_vnt(day_ff_records, day_dd_records, at_least_perc=0.75, force_flag=None):
    """
    Compute "frequenza di intensità e direzione giornaliera del vento".
    It will fill the field 'vnt' of table 'sciapgg.ds__vnt10'.
    It returns the tuple: (flag, frq_calme, ....frq_s(i)c(j) for i=[1,...16], j=[1,...4] )
    where:
    ::

    * flag: (ndati, wht)
    * frq_calme: number of events with FF <= 0.5
    * frq_s(i)c(j):  wind_distribution for events in the sector i of wind direction

    If `force_flag` is not None, returned flag is `force_flag`.

    :param day_ff_records: list of `data` objects for a day and a station of par_code=FF
    :param day_dd_records: list of `data` objects for a day and a station of par_code=DD
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, frq_calme, frq_s(i)c(j))
    """
    flag = force_flag
    if not flag:
        # flag = compute_wind_flag(day_ff_records, day_dd_records, at_least_perc)
        flag = compute_flag(day_ff_records, at_least_perc)
    valid_ff_records = [m for m in day_ff_records if m[3] is not None and m[4]]
    valid_dd_records = [m for m in day_dd_records if m[3] is not None and m[4]]
    if not valid_ff_records and not valid_dd_records:
        return None

    # compute ff_hour_map: a map between DD record's time and corresponding FF record
    # dd_measures_complete: DD records that have the corresponding FF records
    ff_hour_map = dict()
    all_records = sorted(valid_ff_records + valid_dd_records, key=operator.itemgetter(1))
    frq_calme = 0

    dd_measures_complete = []
    for measure_time, measures_it in itertools.groupby(all_records, key=operator.itemgetter(1)):
        measures = list(measures_it)
        valid_ff_measures = [m for m in measures if m[2] == 'FF']
        valid_dd_measures = [m for m in measures if m[2] == 'DD']
        if valid_ff_measures and valid_ff_measures[0][3] <= 0.5:
            frq_calme += 1
        if valid_ff_measures and valid_dd_measures:
            ff_hour_map[measure_time] = valid_ff_measures[0]
            dd_measures_complete.append(valid_dd_measures[0])

    # sectors_ff_records[i] = list of FF records with DD in the i-th sector
    sectors_dd_records = wind_dd_partition(dd_measures_complete)
    sectors_ff_records = [[] for _ in range(16)]
    for i, sector_dd_records in enumerate(sectors_dd_records):
        sectors_ff_records[i] = [ff_hour_map[s[1]] for s in sector_dd_records]

    ret_value = [flag, frq_calme]
    for i, sector_ff_records in enumerate(sectors_ff_records):
        ret_value.extend(wind_ff_distribution(sector_ff_records))
    return ret_value


def compute_day_indicators(measures):
    """
    Compute all indicators extracting values from the input measures.
    It assumes measures are all of a single station and day.
    Return the computed values in a dictionary of kind:
    ::

        {'table1': {column11: value11, column12: value12, ...},
         'table2': {column21: value21, column22: value22, ...},
         ...}

    :param measures: input measures
    :return: dictionaries of tables where to put indicators.
    """
    ret_value = dict()
    measures = [m for m in measures if m[3] is not None and m[4]]

    # PREC
    prec_day_records = [m for m in measures if m[2] == 'PREC']
    prec_flag = compute_flag(prec_day_records, at_least_perc=0.9)
    if prec_flag[0]:
        ret_value['sciapgg.ds__preci'] = {
            'prec01': compute_prec01(prec_day_records, force_flag=prec_flag),
            'prec06': compute_prec06(prec_day_records, force_flag=prec_flag),
            'prec12': compute_prec12(prec_day_records, force_flag=prec_flag),
            'prec24': compute_prec24(prec_day_records, force_flag=prec_flag),
            'cl_prec06': compute_cl_prec06(prec_day_records),
            'cl_prec12': compute_cl_prec12(prec_day_records),
            'cl_prec24': compute_cl_prec24(prec_day_records),
        }

    # TEMPERATURE
    tmedia_day_records = [m for m in measures if m[2] == 'Tmedia']
    tmin_day_records = [m for m in measures if m[2] == 'Tmin']
    tmax_day_records = [m for m in measures if m[2] == 'Tmax']
    if tmedia_day_records or tmin_day_records or tmax_day_records:
        ret_value['sciapgg.ds__t200'] = dict()
        if tmedia_day_records:
            ret_value['sciapgg.ds__t200']['tmdgg'] = compute_tmdgg(tmedia_day_records)
        if tmedia_day_records or tmin_day_records:
            ret_value['sciapgg.ds__t200']['tmngg'] = compute_tmngg(
                tmin_day_records or tmedia_day_records)
        if tmedia_day_records or tmax_day_records:
            ret_value['sciapgg.ds__t200']['tmxgg'] = compute_tmxgg(
                tmax_day_records or tmedia_day_records)

    # PRESSURE
    pmedia_day_records = [m for m in measures if m[2] == 'P']
    pmin_day_records = [m for m in measures if m[2] == 'Pmin']
    pmax_day_records = [m for m in measures if m[2] == 'Pmax']
    if pmedia_day_records or pmax_day_records or pmin_day_records:
        ret_value['sciapgg.ds__press'] = {
            'press': compute_press(pmedia_day_records, pmax_day_records, pmin_day_records)
        }

    # BAGNATURA FOGLIARE
    bagna_day_records = [m for m in measures if m[2] == 'Bagnatura_f']
    if bagna_day_records:
        ret_value['sciapgg.ds__bagna'] = {
            'bagna': compute_bagna(bagna_day_records),
        }

    # ELIOFANIA
    elio_day_records = [m for m in measures if m[2] == 'INSOL']
    elio00_day_records = [m for m in measures if m[2] == 'INSOL_00']
    if elio_day_records or elio00_day_records:
        ret_value['sciapgg.ds__elio'] = {
            'elio': compute_elio(elio_day_records or elio00_day_records)
        }

    # RADIAZIONE SOLARE
    radsol_day_records = [m for m in measures if m[2] == 'RADSOL']
    if radsol_day_records:
        ret_value['sciapgg.ds__radglob'] = {
            'radglob': compute_radglob(radsol_day_records)
        }

    # UMIDITA' RELATIVA
    urmedia_day_records = [m for m in measures if m[2] == 'UR media']
    urmin_day_records = [m for m in measures if m[2] == 'UR min']
    urmax_day_records = [m for m in measures if m[2] == 'UR max']
    if urmedia_day_records or urmax_day_records or urmin_day_records:
        ret_value['sciapgg.ds__urel'] = {
            'ur': compute_ur(urmedia_day_records, urmax_day_records, urmin_day_records)
        }

    # VENTO
    ff_day_records = [m for m in measures if m[2] == 'FF']
    dd_day_records = [m for m in measures if m[2] == 'DD']
    if ff_day_records:
        ret_value['sciapgg.ds__vnt10'] = dict()
        ret_value['sciapgg.ds__vnt10']['vntmd'] = compute_vntmd(ff_day_records)
        if dd_day_records:
            ret_value['sciapgg.ds__vnt10']['vntmxgg'] = compute_vntmxgg(
                ff_day_records, dd_day_records)
            ret_value['sciapgg.ds__vnt10']['vnt'] = compute_vnt(ff_day_records, dd_day_records)
    return ret_value


def compute_and_store(data, writers, table_map):
    """
    Extract indicators from `data` object, write on CSV files with
    a structure that simulates the database tables.
    Return the list of reporting messages and the dictionaries of computed indicators.
    `table_map` is the structure of db tables for indicators,
    of kind:
    ::

        {table1: [column1, column2, ...], table2: [column1, column2, ...], ...}

    `writers` is a dictionary of CSV writers, as returned by function utils.open_csv_writers.

    :param data: list of measures
    :param writers: dictionary of CSV writers
    :param table_map: dictionary of columns of the tables where to insert the indicators
    :return:
    """
    msgs = []
    computed_indicators = {}
    data_sorted = sorted(data, key=group_same_day)
    for ((cod_utente, cod_rete), station_day), measures_it in itertools.groupby(
            data_sorted, key=group_same_day):
        # measures are of a station and of a date
        measures = list(measures_it)
        cod_staz = "%s--%s" % (cod_utente, cod_rete)  # TODO: to convert to id_staz
        station_date_str = station_day.strftime('%Y-%m-%d')
        msg = "- computing day indicators for cod_staz=%s, day=%s" \
              % (cod_staz, station_date_str)
        msgs.append(msg)
        day_indicators = compute_day_indicators(measures)
        for table, columns in table_map.items():
            if table not in day_indicators or table not in writers:
                continue
            key_tuple = (table, station_date_str, cod_staz)
            indicators_row = day_indicators[table]
            if len([i for i in indicators_row.values() if i is not None]) == 0:
                # don't write empty rows
                continue
            writer, _ = writers[table]
            indicators_row['cod_aggr'] = 4
            indicators_row['data_i'] = station_date_str
            indicators_row['cod_staz'] = cod_staz
            writer.writerow(indicators_row)
            computed_indicators[key_tuple] = day_indicators
    return msgs, computed_indicators
