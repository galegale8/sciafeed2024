
import calendar
from datetime import datetime
import itertools

import operator
import statistics


ROUND_PRECISION = 1


def compute_flag(records, at_least_perc, num_expected=10):
    """
    Return (ndati, wht) where:
    ::

    * ndati: num of valid input records
    * wht: 0 if num/total expected record < at_least_perc, 1 otherwise

    It assumes if dates are datetime objects we expect 24 total measures in a day, else 1.

    :param records: input records
    :param at_least_perc: minimum percentage of valid data for the wht
    :param num_expected: number of expected for full coverage
    :return: (ndati, wht)
    """
    if not records:
        return 0, 0
    ndati = len([r for r in records if r[4] and r[3] is not None])
    wht = 0
    if ndati / num_expected >= at_least_perc:
        wht = 1
    return ndati, wht


def compute_bagna(records, num_expected, at_least_perc=0.75):
    """
    Compute "bagnatura fogliare" for different DMA aggregations.

    :param records: list of `data` objects of Bagnatura Fogliare
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the wht
    :return: (flag, val_md, val_vr, val_mx, val_mn, val_tot)
    """
    valid_values = [r[3] for r in records if r[4] and r[3] is not None]
    val_vr = None
    if not valid_values:
        return None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_tot = round(statistics.mean(valid_values), ROUND_PRECISION)
    val_mx = round(max(valid_values), ROUND_PRECISION)
    val_mn = round(min(valid_values), ROUND_PRECISION)
    val_md = round(sum(valid_values), ROUND_PRECISION)
    if len(valid_values) >= 2:
        val_vr = round(statistics.stdev(valid_values), ROUND_PRECISION)
    return flag, val_md, val_vr, val_mx, val_mn, val_tot


def compute_deltaidro(records, num_expected, at_least_perc=0.75):
    """
    Compute "bilancio idrico" for different DMA aggregations.

    :param records: list of `data` objects of ds__delta_idro
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the wht
    :return: flag, val_md, val_vr, val_mx, val_mn
    """
    valid_values = [r[3] for r in records if r[4] and r[3] is not None]
    val_vr = None
    if not valid_values:
        return None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_md = round(statistics.mean(valid_values), ROUND_PRECISION)
    if len(valid_values) >= 2:
        val_vr = round(statistics.stdev(valid_values), ROUND_PRECISION)
    val_mx = round(max(valid_values), ROUND_PRECISION)
    val_mn = round(min(valid_values), ROUND_PRECISION)
    return flag, val_md, val_vr, val_mx, val_mn


def compute_elio(records, num_expected, at_least_perc=0.75):
    """
    Compute "eliofania" for different DMA aggregations.

    :param records: list of input `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, val_md, val_vr, val_mx)
    """
    valid_values = [r[3] for r in records if r[4] and r[3] is not None]
    val_vr = None
    val_mx = None
    if not valid_values:
        return None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_md = round(sum(valid_values), ROUND_PRECISION)
    if len(valid_values) >= 2:
        val_vr = round(statistics.stdev(valid_values), ROUND_PRECISION)
    return flag, val_md, val_vr, val_mx


def compute_etp(records, num_expected, at_least_perc=0.75):
    """
    Compute "Evapotraspirazione potenziale" for different DMA aggregations.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the wht
    :return: flag, val_md, val_vr, val_mx, val_mn
    """
    valid_values = [r[3] for r in records if r[4] and r[3] is not None]
    val_vr = None
    if not valid_values:
        return None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_md = round(statistics.mean(valid_values), ROUND_PRECISION)
    if len(valid_values) >= 2:
        val_vr = round(statistics.stdev(valid_values), ROUND_PRECISION)
    val_mx = round(max(valid_values), ROUND_PRECISION)
    val_mn = round(min(valid_values), ROUND_PRECISION)
    return flag, val_md, val_vr, val_mx, val_mn


def compute_radglob(records, num_expected, at_least_perc=0.75):
    """
    Compute "radiazione global" for different DMA aggregations.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the wht
    :return: flag, val_md, val_vr, val_mx, val_mn
    """
    valid_values = [r[3] for r in records if r[4] and r[3] is not None]
    val_vr = None
    if not valid_values:
        return None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_md = round(statistics.mean(valid_values), ROUND_PRECISION)
    if len(valid_values) >= 2:
        val_vr = round(statistics.stdev(valid_values), ROUND_PRECISION)
    val_mx = round(max(valid_values), ROUND_PRECISION)
    val_mn = round(min(valid_values), ROUND_PRECISION)
    return flag, val_md, val_vr, val_mx, val_mn


def compute_grgg(records, num_expected, at_least_perc=0.75):
    """
    Compute 'gradi giorno' for different DMA aggregations.
    It assumes record[3] = (tot00, tot05, tot10, tot15, tot21) for each record.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the wht
    :return: flag, tot00, tot05, tot10, tot15, tot21
    """
    valid_records = [r for r in records if r[4]]
    valid_values_00 = [r[3] for r in valid_records if r[3][0] is not None]
    valid_values_05 = [r[3] for r in valid_records if r[3][1] is not None]
    valid_values_10 = [r[3] for r in valid_records if r[3][2] is not None]
    valid_values_15 = [r[3] for r in valid_records if r[3][3] is not None]
    valid_values_21 = [r[3] for r in valid_records if r[3][4] is not None]

    if not valid_records:
        return None

    flag = compute_flag(records, at_least_perc, num_expected)
    tot00 = valid_values_00 and round(statistics.mean(valid_values_00), ROUND_PRECISION) or None
    tot05 = valid_values_05 and round(statistics.mean(valid_values_05), ROUND_PRECISION) or None
    tot10 = valid_values_10 and round(statistics.mean(valid_values_10), ROUND_PRECISION) or None
    tot15 = valid_values_15 and round(statistics.mean(valid_values_15), ROUND_PRECISION) or None
    tot21 = valid_values_21 and round(statistics.mean(valid_values_21), ROUND_PRECISION) or None
    return flag, tot00, tot05, tot10, tot15, tot21


def compute_press(records, num_expected, at_least_perc=0.75):
    """
    Compute "pressione atmosferica media, massima e minima" for different DMA aggregations.
    It assumes record[3] = (val_md, val_vr, val_mx, val_mn) for each record.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, val_md, val_vr, val_mx, val_mn)
    """
    valid_records = [r for r in records if r[4]]
    pmedia_values = [r[3] for r in valid_records if r[3][0] is not None]
    pmax_values = [r[3] for r in valid_records if r[3][1] is not None]
    pmin_values = [r[3] for r in valid_records if r[3][2] is not None]
    if not pmin_values and not pmax_values and not pmedia_values:
        return None
    flag = compute_flag(records, at_least_perc, num_expected)
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


def compute_ur(records, num_expected, at_least_perc=0.75):
    """
    Compute "umidità relativa dell'aria media, massima e minima" for different DMA aggregations.
    It assumes record[3] = (val_md, val_vr, val_mx, val_mn) for each record.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, val_md, val_vr, flag1, val_mx, val_mn)
    """
    valid_records = [r for r in records if r[4]]
    urmedia_values = [r[3] for r in valid_records if r[3][0] is not None]
    urmax_values = [r[3] for r in valid_records if r[3][2] is not None]
    urmin_values = [r[3] for r in valid_records if r[3][3] is not None]
    if not urmedia_values and not urmax_values and not urmin_values:
        return None
    flag = flag1 = compute_flag(records, at_least_perc, num_expected)
    val_md = None
    val_vr = None
    val_mx = None
    val_mn = None
    if urmedia_values:
        val_md = round(statistics.mean(urmedia_values), ROUND_PRECISION)
        if len(urmedia_values) >= 2:
            val_vr = round(statistics.stdev(urmedia_values), ROUND_PRECISION)
    if urmax_values:
        val_mx = round(max(urmax_values), ROUND_PRECISION)
    if urmin_values:
        val_mn = round(min(urmin_values), ROUND_PRECISION)
    return flag, val_md, val_vr, flag1, val_mx, val_mn


def compute_vntmxgg(records, num_expected, at_least_perc=0.75):
    """
    Compute "intensità e direzione massima giornaliera del vento".
    It assumes record[3] = (ff, dd) for each record.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, ff, dd)
    """
    valid_records = [r for r in records if r[4] and r[3] is not None and len(r[3]) == 2]
    valid_dd = [r for r in valid_records if r[3][0] is not None]
    valid_ff = [r for r in valid_records if r[3][1] is not None]
    if not valid_records:
        return None
    ff = None
    dd = None
    flag = compute_flag(records, at_least_perc, num_expected)
    if valid_ff:
        ff = round(max(valid_ff), ROUND_PRECISION)
    if valid_dd:
        dd = round(max(valid_dd), ROUND_PRECISION)
    return flag, ff, dd


def compute_dma_records(table_records, field, field_funct):
    group_by_station = operator.itemgetter(0)
    group_by_year = lambda r: r[1].year
    group_by_month = lambda r: r[1].month

    def group_by_decade(r):
        if r[1].day <= 10:
            return 1
        elif r[2].day <= 20:
            return 2
        return 3

    year_items = []
    month_items = []
    decade_items = []
    for station, station_records in itertools.groupby(table_records, group_by_station):
        for year, year_records in itertools.groupby(station_records, group_by_year):
            year_records = list(year_records)
            data_i = datetime(year, 12, 31)
            year_item = {'data_i': data_i, 'cod_staz': station, 'cod_aggr': 3}
            days_in_year = calendar.isleap(year) and 366 or 365
            year_item[field] = field_funct(year_records, days_in_year)
            year_items.append(year_item)
            for month, month_records in itertools.groupby(year_records, group_by_month):
                month_records = list(month_records)
                days_in_month = calendar.monthrange(year, month)[1]
                data_i = datetime(year, month, days_in_month)
                month_item = {'data_i': data_i, 'cod_staz': station,  'cod_aggr': 2,
                              field: field_funct(month_records, days_in_month)}
                month_items.append(month_item)
                for decade, dec_records in itertools.groupby(month_records, group_by_decade):
                    dec_records = list(dec_records)
                    if decade == 3:
                        data_i = datetime(year, month, days_in_month)
                        days_in_decade = days_in_month - 20
                    else:  # decade == 1 or 2:
                        data_i = datetime(year, month, decade*10)
                        days_in_decade = 10
                    decade_item = {'data_i': data_i, 'cod_staz': station, 'cod_aggr': 1,
                                   'field': field_funct(dec_records, days_in_decade)}
                    decade_items.append(decade_item)
    data = year_items + month_items + decade_items
    return data
