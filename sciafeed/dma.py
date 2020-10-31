
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


def compute_bagna(records, at_least_perc=0.75, force_flag=None):
    """
    Compute "bagnatura fogliare" for different DMA aggregations.
    It will fill the field 'bagna' of table 'ds__bagna'.
    It assumes day_records is of the same station and in the right time interval,
    and values are in hours.
    It returns the tuple (flag, val_md, val_vr, val_mx, val_mn, val_tot) where:
    ::

    * flag: (ndati, wht)
    * val_md: totale ore
    * val_vr: varianza
    * val_mx: valore massimo
    * val_mn: valore minimo
    * val_tot: media

    If `force_flag` is not None, returned flag is `force_flag`.

    :param records: list of `data` objects of Bagnatura Fogliare
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :param force_flag: if not None, is the flag to be returned
    :return: (flag, val_md, val_vr, val_mx, val_mn, val_tot)
    """
    valid_values = [r[3] for r in records if r[4] and r[3] is not None]
    val_vr = None
    if not valid_values:
        return None
    flag = force_flag
    if not flag:
        flag = compute_flag(records, at_least_perc)
    val_tot = round(statistics.mean(valid_values), ROUND_PRECISION)
    val_mx = round(max(valid_values), ROUND_PRECISION)
    val_mn = round(min(valid_values), ROUND_PRECISION)
    val_md = round(sum(valid_values), ROUND_PRECISION)
    if len(valid_values) >= 2:
        val_vr = round(statistics.stdev(valid_values), ROUND_PRECISION)
    return flag, val_md, val_vr, val_mx, val_mn, val_tot


def compute_dma_records(table_records, fields_functions=None):
    if fields_functions is None:
        fields_functions = dict()
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
            for field, field_funct in fields_functions.items():
                year_item[field] = field_funct(year_records)
            year_items.append(year_item)
            for month, month_records in itertools.groupby(year_records, group_by_month):
                month_records = list(month_records)
                last_day = calendar.monthrange(year, month)[1]
                data_i = datetime(year, month, last_day)
                month_item = {'data_i': data_i, 'cod_staz': station,  'cod_aggr': 2}
                for field, field_funct in fields_functions.items():
                    month_item[field] = field_funct(month_records)
                month_items.append(month_item)
                for decade, dec_records in itertools.groupby(month_records, group_by_decade):
                    dec_records = list(dec_records)
                    if decade == 3:
                        data_i = datetime(year, month, last_day)
                    else:
                        data_i = datetime(year, month, decade*10)
                    decade_item = {'data_i': data_i, 'cod_staz': station, 'cod_aggr': 1}
                    for field, field_funct in fields_functions.items():
                        decade_item[field] = field_funct(dec_records)
                    decade_items.append(decade_item)
    data = year_items + month_items + decade_items
    return data
