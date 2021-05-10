"""
This module contains functions and utilities that get and update information for DMA data
"""
import calendar
from datetime import datetime
import functools
import itertools

import operator
import numpy as np
import statistics

from sciafeed import db_utils
from sciafeed import querying
from sciafeed import spring
from sciafeed import upsert
from sciafeed import utils

ROUND_PRECISION = 1


def merge_data_items(records1, records2, pkeys=('data_i', 'cod_staz', 'cod_aggr')):
    """
    return a merged version of two input lists of dictionaries; each element of the list
    is identified by the list of keys `pkeys`.

    :param records1: first input list of dictionaries
    :param records2: second input list of dictionaries
    :param pkeys: list of keys to identify a single dictionary to be merged in the list
    :return: the list of dictionaries merged
    """
    dict1 = dict()
    for record1 in records1:
        key = tuple([record1[p] for p in pkeys])
        dict1[key] = record1.copy()
    for record2 in records2:
        key = tuple([record2[p] for p in pkeys])
        if key in dict1:
            dict1[key].update(record2)
        else:
            dict1[key] = record2.copy()
    return list(dict1.values())


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
    ndati = len([r for r in records if r[4] is not None and r[4] > 0 and r[3] is not None])
    wht = 0
    if ndati / num_expected >= at_least_perc:
        wht = 1
    return ndati, wht


def compute_temp_flag(records, at_least_perc, num_expected=10):
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
    ndati, wht = compute_flag(records, at_least_perc, num_expected)
    if num_expected not in (355, 356) or not wht:
        return ndati, wht
    valid_records = [r for r in records if r[4] > 0 and r[3] is not None]
    summer_days = [r for r in valid_records if spring.is_day_in_summer(r[1])]
    winter_days = [r for r in valid_records if spring.is_day_in_winter(r[1])]
    if abs(len(summer_days) - len(winter_days)) > 20:
        wht = 0
    return ndati, wht


def compute_bagna(records, num_expected, at_least_perc=0.75):
    """
    Compute "bagnatura fogliare" for different DMA aggregations.

    :param records: list of `data` objects of Bagnatura Fogliare
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the wht
    :return: (flag, val_md, val_vr, val_mx, val_mn, val_tot)
    """
    valid_values = [r[3] for r in records if r[4] is not None and r[4] > 0 and r[3] is not None]
    val_vr = None
    if not valid_values:
        return (None, None), None, None, None, None, None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_tot = float(round(statistics.mean(valid_values), ROUND_PRECISION))
    val_mx = float(round(max(valid_values), ROUND_PRECISION))
    val_mn = float(round(min(valid_values), ROUND_PRECISION))
    val_md = float(round(sum(valid_values), ROUND_PRECISION))
    if len(valid_values) >= 2:
        val_vr = float(round(statistics.stdev(valid_values), ROUND_PRECISION))
    return flag, val_md, val_vr, val_mx, val_mn, val_tot


def compute_deltaidro(records, num_expected, at_least_perc=0.75):
    """
    Compute "bilancio idrico" for different DMA aggregations.

    :param records: list of `data` objects of ds__delta_idro
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the wht
    :return: flag, val_md, val_vr, val_mx, val_mn
    """
    valid_values = [r[3] for r in records if r[4] is not None and r[4] > 0 and r[3] is not None]
    val_vr = None
    if not valid_values:
        return (None, None), None, None, None, None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_md = float(round(statistics.mean(valid_values), ROUND_PRECISION))
    if len(valid_values) >= 2:
        val_vr = float(round(statistics.stdev(valid_values), ROUND_PRECISION))
    val_mx = float(round(max(valid_values), ROUND_PRECISION))
    val_mn = float(round(min(valid_values), ROUND_PRECISION))
    return flag, val_md, val_vr, val_mx, val_mn


def compute_elio(records, num_expected, at_least_perc=0.75):
    """
    Compute "eliofania" for different DMA aggregations.

    :param records: list of input `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, val_md, val_vr, val_mx)
    """
    valid_values = [r[3] for r in records if r[4] is not None and r[4] > 0 and r[3] is not None]
    val_vr = None
    val_mx = None
    if not valid_values:
        return (None, None), None, None, None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_md = float(round(sum(valid_values), ROUND_PRECISION))
    if len(valid_values) >= 2:
        val_vr = float(round(statistics.stdev(valid_values), ROUND_PRECISION))
    return flag, val_md, val_vr, val_mx


def compute_etp(records, num_expected, at_least_perc=0.75):
    """
    Compute "Evapotraspirazione potenziale" for different DMA aggregations.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the wht
    :return: flag, val_md, val_vr, val_mx, val_mn
    """
    valid_values = [r[3] for r in records if r[4] is not None and r[4] > 0 and r[3] is not None]
    val_vr = None
    if not valid_values:
        return (None, None), None, None, None, None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_md = float(round(statistics.mean(valid_values), ROUND_PRECISION))
    if len(valid_values) >= 2:
        val_vr = float(round(statistics.stdev(valid_values), ROUND_PRECISION))
    val_mx = float(round(max(valid_values), ROUND_PRECISION))
    val_mn = float(round(min(valid_values), ROUND_PRECISION))
    return flag, val_md, val_vr, val_mx, val_mn


def compute_radglob(records, num_expected, at_least_perc=0.75):
    """
    Compute "radiazione global" for different DMA aggregations.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the wht
    :return: flag, val_md, val_vr, val_mx, val_mn
    """
    valid_values = [r[3] for r in records if r[4] is not None and r[4] > 0 and r[3] is not None]
    val_vr = None
    if not valid_values:
        return (None, None), None, None, None, None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_md = float(round(statistics.mean(valid_values), ROUND_PRECISION))
    if len(valid_values) >= 2:
        val_vr = float(round(statistics.stdev(valid_values), ROUND_PRECISION))
    val_mx = float(round(max(valid_values), ROUND_PRECISION))
    val_mn = float(round(min(valid_values), ROUND_PRECISION))
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
    valid_records = [r for r in records if r[4] is not None and r[4] > 0]
    valid_values_00 = [r[3][0] for r in valid_records if r[3][0] is not None]
    valid_values_05 = [r[3][1] for r in valid_records if r[3][1] is not None]
    valid_values_10 = [r[3][2] for r in valid_records if r[3][2] is not None]
    valid_values_15 = [r[3][3] for r in valid_records if r[3][3] is not None]
    valid_values_21 = [r[3][4] for r in valid_records if r[3][4] is not None]

    if not valid_records:
        return (None, None), None, None, None, None, None

    flag = compute_flag(records, at_least_perc, num_expected)
    tot00 = valid_values_00 and float(
        round(statistics.mean(valid_values_00), ROUND_PRECISION)) or None
    tot05 = valid_values_05 and float(
        round(statistics.mean(valid_values_05), ROUND_PRECISION)) or None
    tot10 = valid_values_10 and float(
        round(statistics.mean(valid_values_10), ROUND_PRECISION)) or None
    tot15 = valid_values_15 and float(
        round(statistics.mean(valid_values_15), ROUND_PRECISION)) or None
    tot21 = valid_values_21 and float(
        round(statistics.mean(valid_values_21), ROUND_PRECISION)) or None
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
    valid_records = [r for r in records if r[4] is not None and r[4] > 0]
    pmedia_values = [r[3][0] for r in valid_records if r[3][0] is not None]
    pmax_values = [r[3][1] for r in valid_records if r[3][1] is not None]
    pmin_values = [r[3][2] for r in valid_records if r[3][2] is not None]
    if not pmin_values and not pmax_values and not pmedia_values:
        return (None, None), None, None, None, None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_md = None
    val_vr = None
    val_mx = None
    val_mn = None
    if pmedia_values:
        val_md = float(round(statistics.mean(pmedia_values), ROUND_PRECISION))
        val_mx = float(round(max(pmedia_values), ROUND_PRECISION))
        val_mn = float(round(min(pmedia_values), ROUND_PRECISION))
        if len(pmedia_values) >= 2:
            val_vr = float(round(statistics.stdev(pmedia_values), ROUND_PRECISION))
    if pmax_values:
        val_mx = float(round(max(pmax_values), ROUND_PRECISION))
    if pmin_values:
        val_mn = float(round(min(pmin_values), ROUND_PRECISION))
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
    valid_records = [r for r in records if r[4] is not None and r[4] > 0]
    urmedia_values = [r[3][0] for r in valid_records if r[3][0] is not None]
    urmax_values = [r[3][2] for r in valid_records if r[3][2] is not None]
    urmin_values = [r[3][3] for r in valid_records if r[3][3] is not None]
    if not urmedia_values and not urmax_values and not urmin_values:
        return (None, None), None, None, None, None, None
    flag = flag1 = compute_flag(records, at_least_perc, num_expected)
    val_md = None
    val_vr = None
    val_mx = None
    val_mn = None
    if urmedia_values:
        val_md = float(round(statistics.mean(urmedia_values), ROUND_PRECISION))
        if len(urmedia_values) >= 2:
            val_vr = float(round(statistics.stdev(urmedia_values), ROUND_PRECISION))
    if urmax_values:
        val_mx = float(round(max(urmax_values), ROUND_PRECISION))
    if urmin_values:
        val_mn = float(round(min(urmin_values), ROUND_PRECISION))
    return flag, val_md, val_vr, flag1, val_mx, val_mn


def compute_vntmxgg(records, num_expected, at_least_perc=0.75):
    """
    Compute "intensità e direzione massima del vento" for different DMA aggregations.
    It assumes record[3] = (ff, dd) for each record.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, ff, dd)
    """
    valid_records = [r for r in records if r[4] is not None and r[4] > 0 and r[3] is not None
                     and len(r[3]) == 2]
    valid_ff = [r[3][0] for r in valid_records if r[3][0] is not None]
    valid_dd = [r[3][1] for r in valid_records if r[3][1] is not None]
    if not valid_records:
        return (None, None), None, None
    ff = None
    dd = None
    flag = compute_flag(records, at_least_perc, num_expected)
    if valid_ff:
        ff = float(round(max(valid_ff), ROUND_PRECISION))
    if valid_dd:
        dd = float(round(max(valid_dd), ROUND_PRECISION))
    return flag, ff, dd


def compute_vntmd(records, num_expected, at_least_perc=0.75):
    """
    Compute "velocità media del vento" for different DMA aggregations.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, ff)
    """
    valid_values = [r[3] for r in records if r[4] is not None and r[4] > 0 and r[3] is not None]
    if not valid_values:
        return (None, None), None
    flag = compute_flag(records, at_least_perc, num_expected)
    ff = float(round(statistics.mean(valid_values), ROUND_PRECISION))
    return flag, ff


def compute_vnt(records, num_expected, at_least_perc=0.75):
    """
    Compute "frequenza di intensità e direzione del vento" for different DMA aggregations.
    It assumes record[3] = list of subfields frq_* for each record.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, frq_calme, frq_s(i)c(j))
    """
    flag = compute_flag(records, at_least_perc, num_expected)
    if not flag[1]:
        return [flag] + [None] * 65
    valid_records = [r for r in records if r[4] is not None and r[4] > 0 and r[3]]
    subfields_days = []  # list of vectors
    for valid_record in valid_records:
        subfields_day = [v is not None and float(v) or 0 for v in valid_record[3]]
        subfields_days.append(subfields_day)
    ret_subvalues = [flag] + list(np.sum(subfields_days, axis=0))  # i.e. sum of vectors
    return ret_subvalues


def compute_prec24(records, num_expected, at_least_perc=0.9):
    """
    Compute "precipitazione cumulata"  for different DMA aggregations.
    It assumes record[3] = prec24.val_tot for each input record.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, val_tot, val_mx, data_mx)
    """
    valid_records = [r for r in records if r[4] > 0 and r[3] is not None]
    valid_values = [r[3] for r in valid_records]
    if not valid_values:
        return (None, None), None, None, None
    flag = compute_flag(records, at_least_perc, num_expected)
    val_tot = float(round(sum(valid_values), ROUND_PRECISION))
    max_record = max(valid_records, key=operator.itemgetter(3))
    data_mx = max_record[1].strftime('%Y-%m-%d 00:00:00')
    val_mx = float(round(max_record[3], ROUND_PRECISION))
    return flag, val_tot, val_mx, data_mx


def compute_cl_prec24(records, *args, **kwargs):
    """
    It returns the tuple (dry, wet_01, wet_02, wet_03, wet_04, wet_05) where:
    ::

    * dry: num of input records with PREC <= 1
    * wet_01: num of records with PREC in ]1, 5]
    * wet_02: num of records with PREC in ]5, 10]
    * wet_03: num of records with PREC in ]10, 20]
    * wet_04: num of records with PREC in ]20, 50]
    * wet_05: num of records with PREC > 50

    :param records: input records of PREC
    :return: (dry, wet_01, wet_02, wet_03, wet_04, wet_05)
    """
    valid_records = [r for r in records if r[4] > 0 and r[3] is not None]
    dry = len([d for d in valid_records if d[3] <= 1])
    wet_01 = len([d for d in valid_records if 1 < d[3] <= 5])
    wet_02 = len([d for d in valid_records if 5 < d[3] <= 10])
    wet_03 = len([d for d in valid_records if 10 < d[3] <= 20])
    wet_04 = len([d for d in valid_records if 20 < d[3] <= 50])
    wet_05 = len([d for d in valid_records if d[3] > 50])
    return dry, wet_01, wet_02, wet_03, wet_04, wet_05


def compute_prec01_06_12(records, num_expected, at_least_perc=0.9):
    """
    Compute "precipitazione cumulata su 1, 6 o 12 ore" for different DMA aggregations.
    Assume r[3][0] is the value for each record.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, val_mx, data_mx)
    """
    valid_records = [r for r in records if r[4] is not None and r[4] > 0 and r[3][0] is not None]
    valid_values = [r[3][0] for r in valid_records]
    if not valid_values:
        return (None, None), None, None
    flag = compute_flag(records, at_least_perc, num_expected)
    max_record = max(valid_records, key=operator.itemgetter(3))
    data_mx = max_record[1].strftime('%Y-%m-%d 00:00:00')
    val_mx = float(round(max_record[3][0], ROUND_PRECISION))
    return flag, val_mx, data_mx


def compute_cl_prec_06_12(records, *args, **kwargs):
    """
    Compute "distribuzione precipitazione cumulata su 6 o 12 ore" for different DMA aggregations.
    Assume r[3][1:] are the values for each record.

    :param records: list of `data` objects
    :return: (dry, wet_01, wet_02, wet_03, wet_04, wet_05)
    """
    valid_values = [r[3][1:] for r in records if r[4] is not None and r[4] > 0]
    if not valid_values:
        return (0, )*6
    dry, wet_01, wet_02, wet_03, wet_04, wet_05 = [0] * 6
    for day_values in valid_values:
        cdry, cwet_01, cwet_02, cwet_03, cwet_04, cwet_05 = day_values
        if cdry is not None:
            dry += float(cdry)
        if cwet_01 is not None:
            wet_01 += float(cwet_01)
        if cwet_02 is not None:
            wet_02 += float(cwet_02)
        if cwet_03 is not None:
            wet_03 += float(cwet_03)
        if cwet_04 is not None:
            wet_04 += float(cwet_04)
        if cwet_05 is not None:
            wet_05 += float(cwet_05)
    return dry, wet_01, wet_02, wet_03, wet_04, wet_05
    # alternative:
    # valid_values = [r for r in records if r[4] is not None and r[4] > 0]
    # subfields_days = []  # list of vectors
    # for valid_record in valid_records:
    #     subfields_day = [v is not None and float(v) or 0 for v in valid_record[3][1:]]
    #     subfields_days.append(subfields_day)
    # ret_subvalues = list(np.sum(subfields_days, axis=0))  # i.e. sum of vectors
    # return ret_subvalues


def compute_ifs(records, num_expected, at_least_perc=0.75):
    """
    Compute "indice di freddo secco" for different DMA aggregations.
    It assumes record[3] = (tmedia, urel)

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, num)
    """
    flag = compute_flag(records, at_least_perc, num_expected)
    valid_values = [r[3] for r in records if r[4] > 0 and len(r[3]) == 2 and r[3][0] is not None
                    and r[3][1] is not None]
    temp_threshold = 5
    ur_threshold = 40
    num = len([r for r in valid_values if r[0] < temp_threshold and r[1] < ur_threshold])
    return flag, num


def compute_ifu(records, num_expected, at_least_perc=0.75):
    """
    Compute "indice di freddo umido" for different DMA aggregations.
    It assumes record[3] = (tmedia, urel)

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, num)
    """
    flag = compute_flag(records, at_least_perc, num_expected)
    valid_values = [r[3] for r in records if r[4] > 0 and len(r[3]) == 2 and r[3][0] is not None
                    and r[3][1] is not None]
    temp_threshold = 5
    ur_threshold = 90
    num = len([r for r in valid_values if r[0] < temp_threshold and r[1] > ur_threshold])
    return flag, num


def compute_ics(records, num_expected, at_least_perc=0.75):
    """
    Compute "indice di caldo secco" for different DMA aggregations.
    It assumes record[3] = (tmedia, urel)

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, num)
    """
    flag = compute_flag(records, at_least_perc, num_expected)
    valid_values = [r[3] for r in records if r[4] > 0 and len(r[3]) == 2 and r[3][0] is not None
                    and r[3][1] is not None]
    temp_threshold = 25
    ur_threshold = 40
    num = len([r for r in valid_values if r[0] > temp_threshold and r[1] < ur_threshold])
    return flag, num


def compute_icu(records, num_expected, at_least_perc=0.75):
    """
    Compute "indice di caldo umido" for different DMA aggregations.
    It assumes record[3] = (tmedia, urel)

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, num)
    """
    flag = compute_flag(records, at_least_perc, num_expected)
    valid_values = [r[3] for r in records if r[4] > 0 and len(r[3]) == 2 and r[3][0] is not None
                    and r[3][1] is not None]
    temp_threshold = 25
    ur_threshold = 90
    num = len([r for r in valid_values if r[0] > temp_threshold and r[1] > ur_threshold])
    return flag, num


def compute_sharl(records, num_expected, at_least_perc=0.75):
    """
    Compute "indice di Scharlau" for different DMA aggregations.
    It assumes record[3] = (tmedia, urel)

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, num_01, num_02, num_03)
    """
    flag = compute_flag(records, at_least_perc, num_expected)
    valid_values = [r[3] for r in records if r[4] > 0 and len(r[3]) == 2 and r[3][0] is not None
                    and r[3][1] is not None]
    sortedbyurel = sorted(valid_values, key=operator.itemgetter(1))
    num_01 = num_02 = num_03 = 0

    def group_by_ur(valid_value):
        temp, urel = valid_value
        if urel <= 57.5:
            return 26.2
        elif 57.5 < urel <= 62.5:
            return 24.8
        elif 62.5 < urel <= 67.5:
            return 23.4
        elif 67.5 < urel <= 72.5:
            return 22.2
        elif 72.5 < urel <= 77.5:
            return 21.1
        elif 77.5 < urel <= 82.5:
            return 20.1
        elif 82.5 < urel <= 87.5:
            return 19.1
        elif 87.5 < urel <= 92.5:
            return 18.2
        elif 92.5 < urel <= 97.5:
            return 17.3
        # else: 97.5 < urel <= 100:
        return 16.5

    for critical_temp, urel_set in itertools.groupby(sortedbyurel, group_by_ur):
        urel_set = list(urel_set)
        num_01 += len([r for r in urel_set if float(r[0]) - critical_temp > 0])
        num_02 += len([r for r in urel_set if float(r[0]) - critical_temp > 1])
        num_02 += len([r for r in urel_set if float(r[0]) - critical_temp > 2])
    return flag, num_01, num_02, num_03


def compute_ifu1(records, num_expected, at_least_perc=0.75):
    """
    Compute "indice di freddo umido" for different DMA aggregations.
    It assumes record[3] = (tmedia, urel)

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return: (flag, num_01, num_02, num_03)
    """
    flag = compute_flag(records, at_least_perc, num_expected)
    valid_values = [r[3] for r in records if r[4] > 0 and len(r[3]) == 2 and r[3][0] is not None
                    and r[3][1] is not None]
    sortedbyurel = sorted(valid_values, key=operator.itemgetter(1))
    num_01 = num_02 = num_03 = 0

    def group_by_ur(valid_value):
        temp, urel = valid_value
        if urel <= 42.5:
            return -2.5
        elif 42.5 < urel <= 47.5:
            return -1.5
        elif 47.5 < urel <= 52.5:
            return -0.5
        elif 52.5 < urel <= 57.5:
            return -0.3
        elif 57.5 < urel <= 62.5:
            return 0
        elif 62.5 < urel <= 67.5:
            return 0.5
        elif 67.5 < urel <= 72.5:
            return 1.5
        elif 72.5 < urel <= 77.5:
            return 1.8
        elif 77.5 < urel <= 82.5:
            return 2.2
        elif 82.5 < urel <= 87.5:
            return 2.8
        # else: 87.5 < urel <= 100:
        return 3.5

    for critical_temp, urel_set in itertools.groupby(sortedbyurel, group_by_ur):
        urel_set = list(urel_set)
        num_01 += len([r for r in urel_set if float(r[0]) - critical_temp > 0])
        num_02 += len([r for r in urel_set if float(r[0]) - critical_temp > 1])
        num_02 += len([r for r in urel_set if float(r[0]) - critical_temp > 2])
    return flag, num_01, num_02, num_03


def compute_prs_prec(records, num_expected, at_least_perc=0.9):
    """
    Compute 'Persistenza di siccità' and 'Persistenza di precipitazione'

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    """
    flag = compute_flag(records, at_least_perc, num_expected)
    valid_records = [r for r in records if r[4] > 0 and r[3] is not None]
    ndry_01, datadry_01, ndry_02, datadry_02, ndry_03, datadry_03 = [None] * 6
    nwet_01, totwet_01, datawet_01, nwet_02, totwet_02, datawet_02, \
        nwet_03, totwet_03, datawet_03 = [None] * 9

    def group_by_low_prec(r):
        return r[3] <= 1

    dry_sequences = []
    wet_sequences = []

    for is_low, rec_sequence in itertools.groupby(valid_records, group_by_low_prec):
        rec_sequence = list(rec_sequence)
        if is_low:
            dry_sequences.append(rec_sequence)
        else:
            wet_sequences.append(rec_sequence)
    dry_sequences.sort(key=lambda r: len(r), reverse=True)
    wet_sequences.sort(key=lambda r: len(r), reverse=True)
    if len(dry_sequences) > 1:
        longest = dry_sequences[0]
        ndry_01 = len(longest)
        datadry_01 = sorted(longest, key=operator.itemgetter(1))[0][1].strftime(
            '%Y-%m-%d 00:00:00')
    if len(dry_sequences) > 2:
        longest = dry_sequences[1]
        ndry_02 = len(longest)
        datadry_02 = sorted(longest, key=operator.itemgetter(1))[0][1].strftime(
            '%Y-%m-%d 00:00:00')
    if len(dry_sequences) > 3:
        longest = dry_sequences[1]
        ndry_03 = len(longest)
        datadry_03 = sorted(longest, key=operator.itemgetter(1))[0][1].strftime(
            '%Y-%m-%d 00:00:00')

    if len(wet_sequences) > 1:
        longest = wet_sequences[0]
        nwet_01 = len(longest)
        totwet_01 = float(sum([r[3] for r in longest]))
        datawet_01 = sorted(longest, key=operator.itemgetter(1))[0][1].strftime(
            '%Y-%m-%d 00:00:00')
    if len(wet_sequences) > 2:
        longest = wet_sequences[1]
        nwet_02 = len(longest)
        totwet_02 = float(sum([r[3] for r in longest]))
        datawet_02 = sorted(longest, key=operator.itemgetter(1))[0][1].strftime(
            '%Y-%m-%d 00:00:00')
    if len(wet_sequences) > 3:
        longest = wet_sequences[2]
        nwet_03 = len(longest)
        totwet_03 = float(sum([r[3] for r in longest]))
        datawet_03 = sorted(longest, key=operator.itemgetter(1))[0][1].strftime(
            '%Y-%m-%d 00:00:00')
    ret_value = [flag, ndry_01, datadry_01, ndry_02, datadry_02, ndry_03, datadry_03,
                 nwet_01, totwet_01, datawet_01, nwet_02, totwet_02, datawet_02,
                 nwet_03, totwet_03, datawet_03] + [None] * 9
    return ret_value


def compute_prs_t200mx(records, num_expected, at_least_perc=0.75):
    """
    Compute 'Persistenza temperatura (Tmassima)'

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    """
    flag = compute_temp_flag(records, at_least_perc, num_expected)
    valid_records = [r for r in records if r[4] and r[4] > 0 and r[3] is not None]
    numX = [0] * 11
    dataX = [None] * 11

    def group_by_trange(record):
        value = record[3]
        if value <= -5:
            return 0
        elif -5 < value <= 0:
            return 1
        elif 0 < value <= 5:
            return 2
        elif 5 < value <= 10:
            return 3
        elif 10 < value <= 15:
            return 4
        elif 15 < value <= 20:
            return 5
        elif 20 < value <= 25:
            return 6
        elif 25 < value <= 30:
            return 7
        elif 30 < value <= 35:
            return 8
        elif 35 < value <= 40:
            return 9
        # else > 40:
        return 10

    for the_index, rec_sequence in itertools.groupby(valid_records, group_by_trange):
        rec_sequence = list(rec_sequence)
        if len(rec_sequence) > numX[the_index]:
            numX[the_index] = len(rec_sequence)
            dataX[the_index] = rec_sequence[0][1].strftime('%Y-%m-%d 00:00:00')
    # fields = [numX[0], dataX[0], numX[1], dataX[1], ...]
    fields = list(functools.reduce(operator.add, zip(numX, dataX)))
    ret_value = [flag] + fields
    return ret_value


def compute_prs_t200mn(records, num_expected, at_least_perc=0.75):
    """
    Compute 'persistenza temperatura (Tminima)'

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    """
    flag = compute_temp_flag(records, at_least_perc, num_expected)
    valid_records = [r for r in records if r[4] and r[4] > 0 and r[3] is not None]
    numX = [0] * 9
    dataX = [None] * 9

    def group_by_trange(record):
        value = record[3]
        if value <= -20:
            return 0
        elif -20 < value <= -15:
            return 1
        elif -15 < value <= -10:
            return 2
        elif -10 < value <= -5:
            return 3
        elif -5 < value <= 0:
            return 4
        elif 0 < value <= 5:
            return 5
        elif 5 < value <= 10:
            return 6
        elif 10 < value <= 15:
            return 7
        # value > 15
        return 8

    for the_index, rec_sequence in itertools.groupby(valid_records, group_by_trange):
        rec_sequence = list(rec_sequence)
        if len(rec_sequence) > numX[the_index]:
            numX[the_index] = len(rec_sequence)
            dataX[the_index] = rec_sequence[0][1].strftime('%Y-%m-%d 00:00:00')
    # fields = [numX[0], dataX[0], numX[1], dataX[1], ...]
    fields = list(functools.reduce(operator.add, zip(numX, dataX)))
    ret_value = [flag] + fields
    return ret_value


def compute_tmdgg(records, num_expected, at_least_perc=0.75):
    """
    Compute 'temperatura media' for different DMA aggregations.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return flag, val_md, val_vr
    """
    flag = compute_temp_flag(records, at_least_perc, num_expected)
    valid_values = [r[3] for r in records if r[4] and r[4] > 0 and r[3] is not None]
    val_md = None
    val_vr = None
    if valid_values:
        val_md = float(round(statistics.mean(valid_values), ROUND_PRECISION))
    if len(valid_values) >= 2:
        val_vr = float(round(statistics.stdev(valid_values), ROUND_PRECISION))
    return flag, val_md, val_vr


def compute_tmxgg(records, num_expected, at_least_perc=0.75):
    """
    Compute 'temperatura massima' for different DMA aggregations.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return flag, val_md, val_vr, val_x, data_x (val_md is the max)
    """
    flag = compute_temp_flag(records, at_least_perc, num_expected)
    val_vr = None
    valid_records = [r for r in records if r[4] and r[4] > 0 and r[3] is not None]
    if not valid_records:
        return (None, None), None, None, None, None
    values = [r[3] for r in valid_records]
    val_x = float(round(statistics.mean(values), ROUND_PRECISION))
    if len(values) >= 2:
        val_vr = float(round(statistics.stdev(values), ROUND_PRECISION))

    max_record = max(valid_records, key=operator.itemgetter(3))
    data_x = max_record[1].strftime('%Y-%m-%d 00:00:00')
    val_md = float(round(max_record[3], ROUND_PRECISION))

    return flag, val_md, val_vr, val_x, data_x


def compute_tmngg(records, num_expected, at_least_perc=0.75):
    """
    Compute 'temperatura minima' for different DMA aggregations.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return flag, val_md, val_vr, val_x, data_x (val_md is the min)
    """
    flag = compute_temp_flag(records, at_least_perc, num_expected)
    val_vr = None
    valid_records = [r for r in records if r[4] and r[4] > 0 and r[3] is not None]
    if not valid_records or not flag[1]:
        return flag, None, None, None, None
    values = [r[3] for r in valid_records]
    val_x = float(round(statistics.mean(values), ROUND_PRECISION))
    if len(values) >= 2:
        val_vr = float(round(statistics.stdev(values), ROUND_PRECISION))

    min_record = min(valid_records, key=operator.itemgetter(3))
    data_x = min_record[1].strftime('%Y-%m-%d 00:00:00')
    val_md = float(round(min_record[3], ROUND_PRECISION))

    return flag, val_md, val_vr, val_x, data_x


def compute_cl_tmxgg(records, *args, **kwargs):
    """
    It returns the iterable (cl_01, cl_02, ...cl_11) according to the number of days in `records`
    with tmax inside defined intervals.

    :param records: input records
    :return: [cl_01, cl_02, ...cl_11]
    """
    valid_records = [r for r in records if r[4] and r[4] > 0 and r[3] is not None]
    cl_01 = len([d for d in valid_records if d[3] <= -5])
    cl_02 = len([d for d in valid_records if -5 < d[3] <= 0])
    cl_03 = len([d for d in valid_records if 0 < d[3] <= 5])
    cl_04 = len([d for d in valid_records if 5 < d[3] <= 10])
    cl_05 = len([d for d in valid_records if 10 < d[3] <= 15])
    cl_06 = len([d for d in valid_records if 15 < d[3] <= 20])
    cl_07 = len([d for d in valid_records if 20 < d[3] <= 25])
    cl_08 = len([d for d in valid_records if 25 < d[3] <= 30])
    cl_09 = len([d for d in valid_records if 30 < d[3] <= 35])
    cl_10 = len([d for d in valid_records if 35 < d[3] <= 40])
    cl_11 = len([d for d in valid_records if d[3] > 40])
    return cl_01, cl_02, cl_03, cl_04, cl_05, cl_06, cl_07, cl_08, cl_09, cl_10, cl_11


def compute_cl_tmngg(records, *args, **kwargs):
    """
    It returns the iterable (cl_01, cl_02, ...cl_09) according to the number of days in `records`
    with tmax inside defined intervals.

    :param records: input records
    :return: [cl_01, cl_02, ...cl_11]
    """
    valid_records = [r for r in records if r[4] and r[4] > 0 and r[3] is not None]
    cl_01 = len([d for d in valid_records if d[3] <= -15])
    cl_02 = len([d for d in valid_records if -15 < d[3] <= -10])
    cl_03 = len([d for d in valid_records if -10 < d[3] <= -5])
    cl_04 = len([d for d in valid_records if -5 < d[3] <= 0])
    cl_05 = len([d for d in valid_records if 0 < d[3] <= 5])
    cl_06 = len([d for d in valid_records if 5 < d[3] <= 10])
    cl_07 = len([d for d in valid_records if 10 < d[3] <= 15])
    cl_08 = len([d for d in valid_records if 15 < d[3] <= 20])
    cl_09 = len([d for d in valid_records if d[3] > 20])
    return cl_01, cl_02, cl_03, cl_04, cl_05, cl_06, cl_07, cl_08, cl_09


def compute_tmdgg1(records, num_expected, at_least_perc=0.75):
    """
    Compute 'temperatura media' for different DMA aggregations.
    It assumes r[3] = [tmax, tmin]

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return flag, val_md, val_vr
    """
    valid_records = [r for r in records if len(r[3]) == 2 and r[3][0] is not None
                     and r[3][1] is not None and r[4] and r[4] and r[4] > 0]
    flag = compute_temp_flag(valid_records, at_least_perc, num_expected)
    val_md = val_vr = None
    if not valid_records or not flag[1]:
        return flag, val_md, val_vr
    values_tmediagg1 = [(r[3][0]+r[3][1])/2 for r in valid_records]
    val_md = float(round(statistics.mean(values_tmediagg1), ROUND_PRECISION))
    if len(values_tmediagg1) >= 2:
        val_vr = float(round(statistics.stdev(values_tmediagg1), ROUND_PRECISION))
    return flag, val_md, val_vr


def compute_deltagg(records, num_expected, at_least_perc=0.75):
    """
    Compute 'escursione termica' for different DMA aggregations.
    It assumes r[3] = [tmax, tmin]

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return flag, val_md, val_vr, val_mx, val_mn
    """
    valid_records = [r for r in records if len(r[3]) == 2 and r[3][0] is not None
                     and r[3][1] is not None and r[4] and r[4] > 0]
    flag = compute_temp_flag(valid_records, at_least_perc, num_expected)
    val_md = val_vr = val_mx = val_mn = None
    if not valid_records or not flag[1]:
        return flag, val_md, val_vr, val_mx, val_mn
    values_deltagg = [(r[3][0]-r[3][1]) for r in valid_records]
    val_md = float(round(statistics.mean(values_deltagg), ROUND_PRECISION))
    if len(values_deltagg) >= 2:
        val_vr = float(round(statistics.stdev(values_deltagg), ROUND_PRECISION))
    val_mx = float(max(values_deltagg))
    val_mn = float(min(values_deltagg))
    return flag, val_md, val_vr, val_mx, val_mn


def compute_day_gelo(records, num_expected, at_least_perc=0.75):
    """
    Compute 'numero giorni di gelo' for different DMA aggregations.

    :param records: list of `data` objects
    :param num_expected: number of records expected
    :param at_least_perc: minimum percentage of valid data for the validation flag
    :return flag, num
    """
    valid_records = [r for r in records if r[3] is not None and r[4] and r[4] > 0]
    flag = compute_temp_flag(valid_records, at_least_perc, num_expected)
    val_md = val_vr = None
    if not valid_records or not flag[1]:
        return flag, val_md, val_vr
    num = len([r for r in valid_records if r[3] < 0])
    return flag, num


def compute_dma_records(table_records, field=None, field_funct=None, map_funct=None):
    """
    It return records of kind [{'data_i': ..., 'cod_staz': ...,  'cod_aggr': ...}, ...]
    obtaining aggregating input records in decades, months and years.
    Each output record has also some keys computed running some functions:
    ::

        record[field] = field_funct(aggregated_records, num_expected)

    where field and field_funct are items of `map_funct`, and num_expected=number of total records
    expected in the aggregation.
    If map_funct is not set, map_funct = {`field`: `field_funct`}, otherwise `field` and
    `field_funct` are ignored.
    It assumes input records are of kind (metadata, datetime object, par_code, par_value, flag).

    :param table_records: input records
    :param field: additional key to be set for output records
    :param field_funct: function to be run to compute the value of key `field` for output records
    :param map_funct: dictionary of fields and corresponding field_functions to be run
    :return: list of records of kind {'data_i': ...,'cod_staz': ..., 'cod_aggr': ...,`field`: ...}
    """
    group_by_station = operator.itemgetter(0)
    group_by_year = lambda r: r[1].year
    group_by_month = lambda r: r[1].month

    if map_funct is None:
        map_funct = {field: field_funct}

    def group_by_decade(r):
        if r[1].day <= 10:
            return 1
        elif r[1].day <= 20:
            return 2
        return 3

    year_items = []
    month_items = []
    decade_items = []
    for station, station_records in itertools.groupby(table_records, group_by_station):
        for year, year_records in itertools.groupby(station_records, group_by_year):
            year_records = list(year_records)
            data_i = datetime(year, 12, 31)
            year_item = {
                'data_i': data_i, 'cod_staz': station, 'cod_aggr': 3, 'provenienza': 'daily'}
            days_in_year = calendar.isleap(year) and 366 or 365
            for field, field_funct in map_funct.items():
                year_item[field] = str(field_funct(year_records, days_in_year))
            year_items.append(year_item)
            for month, month_records in itertools.groupby(year_records, group_by_month):
                month_records = list(month_records)
                days_in_month = calendar.monthrange(year, month)[1]
                data_i = datetime(year, month, days_in_month)
                month_item = {
                    'data_i': data_i, 'cod_staz': station,  'cod_aggr': 2, 'provenienza': 'daily'}
                for field, field_funct in map_funct.items():
                    month_item[field] = str(field_funct(month_records, days_in_month))
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
                                   'provenienza': 'daily'}
                    for field, field_funct in map_funct.items():
                        decade_item[field] = str(field_funct(dec_records, days_in_decade))
                    decade_items.append(decade_item)
    data = year_items + month_items + decade_items
    ret_value = []
    for record in data:
        record = upsert.expand_record(record)
        record = {
            k: v for k, v in record.items()
            if v not in (None, 'NULL') and list(filter(lambda r: utils.is_float(r), str(v)))
        }
        ret_value.append(record)
    return ret_value


def compute_year_records(table_records, field=None, field_funct=None, map_funct=None):
    """
    The same of compute_dma_records but only grouping by year.

    :param table_records: input records
    :param field: additional key to be set for output records
    :param field_funct: function to be run to compute the value of key `field` for output records
    :param map_funct: dictionary of fields and corresponding field_functions to be run
    :return: list of records of kind {'data_i': ...,'cod_staz': ..., 'cod_aggr': ...,`field`: ...}
    """
    group_by_station = operator.itemgetter(0)
    group_by_year = lambda r: r[1].year

    if map_funct is None:
        map_funct = {field: field_funct}

    year_items = []
    for station, station_records in itertools.groupby(table_records, group_by_station):
        for year, year_records in itertools.groupby(station_records, group_by_year):
            year_records = list(year_records)
            data_i = datetime(year, 12, 31)
            year_item = {
                'data_i': data_i, 'cod_staz': station, 'cod_aggr': 3, 'provenienza': 'daily'}
            days_in_year = calendar.isleap(year) and 366 or 365
            for field, field_funct in map_funct.items():
                year_item[field] = str(field_funct(year_records, days_in_year))
            year_items.append(year_item)
    data = year_items
    ret_value = []
    for record in data:
        record = upsert.expand_record(record)
        record = {
            k: v for k, v in record.items()
            if v not in (None, 'NULL') and list(filter(lambda r: utils.is_float(r), str(v)))
        }
        ret_value.append(record)
    return ret_value


def process_dma_bagnatura(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of bagnatura fogliare

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA bagnatura fogliare')
    logger.info('select for input records...')
    # records are: (metadata, datetime object, par_code, par_value, flag)
    sql_fields = "cod_staz, data_i, '%s', (%s).%s, ((%s).flag).wht" \
                 % ('bagna', 'bagna', 'val_md', 'bagna')
    table_records = querying.select_records(
        conn, 'ds__bagna', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=startschema)
    logger.info('computing aggregations...')
    data = compute_dma_records(table_records, 'bagna', compute_bagna)
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'
    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'bagna'])
    logger.info('update records....')
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__bagna', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA bagnatura fogliare')


def process_dma_bilancio_idrico(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table containing bilancio idrico

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA bilancio idrico')
    logger.info('select for input records...')
    # records are: (metadata, datetime object, par_code, par_value, flag)
    sql_fields = "cod_staz, data_i, '%s', (%s).%s, ((%s).flag).wht" \
                 % ('deltaidro', 'deltaidro', 'val_md', 'deltaidro')
    table_records = querying.select_records(
        conn, 'ds__delta_idro', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=startschema)
    logger.info('computing aggregations...')
    data = compute_dma_records(table_records, 'deltaidro', compute_deltaidro)
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'
    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'deltaidro'])
    logger.info('update records....')
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__delta_idro', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA bilancio idrico')


def process_dma_eliofania(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of eliofania

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA eliofania')
    logger.info('select for input records...')
    # records are: (metadata, datetime object, par_code, par_value, flag)
    sql_fields = "cod_staz, data_i, '%s', (%s).%s, ((%s).flag).wht" \
                 % ('elio', 'elio', 'val_md', 'elio')
    table_records = querying.select_records(
        conn, 'ds__elio', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=startschema)
    logger.info('computing aggregations...')
    data = compute_dma_records(table_records, 'elio', compute_elio)
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'
    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'elio'])
    logger.info('update records....')
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__elio', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA eliofania')


def process_dma_radiazione_globale(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of radiazione globale

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA radiazione globale')
    logger.info('select for input records...')
    # records are: (metadata, datetime object, par_code, par_value, flag)
    sql_fields = "cod_staz, data_i, '%s', (%s).%s, ((%s).flag).wht" \
                 % ('radglob', 'radglob', 'val_md', 'radglob')
    table_records = querying.select_records(
        conn, 'ds__radglob', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=startschema)
    logger.info('computing aggregations...')
    data = compute_dma_records(table_records, 'radglob', compute_radglob)
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'
    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'radglob'])
    logger.info('update records....')
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__radglob', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA radiazione globale')


def process_dma_evapotraspirazione(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of evapotraspirazione

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA evapotraspirazione')
    logger.info('select for input records...')
    # records are: (metadata, datetime object, par_code, par_value, flag)
    sql_fields = "cod_staz, data_i, '%s', (%s).%s, ((%s).flag).wht" \
                 % ('etp', 'etp', 'val_md', 'etp')
    table_records = querying.select_records(
        conn, 'ds__etp', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=startschema)
    logger.info('computing aggregations...')
    data = compute_dma_records(table_records, 'etp', compute_etp)
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'
    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'etp'])
    logger.info('update records....')
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__etp', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA evapotraspirazione')


def process_dma_gradi_giorno(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of gradi giorno

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA gradi giorno')
    logger.info('selecting for input records...')
    # records are: (metadata, datetime object, par_code, par_value, flag)
    subfields = ['tot00', 'tot05', 'tot10', 'tot15', 'tot21']
    array_field = 'ARRAY[' + ','.join(['(%s).%s' % ('grgg', s) for s in subfields]) + ']'
    sql_fields = "cod_staz, data_i, '%s', %s, ((%s).flag).wht" % ('grgg', array_field, 'grgg')
    table_records = querying.select_records(
        conn, 'ds__grgg', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=startschema)
    logger.info('computing aggregations...')
    data = compute_dma_records(table_records, 'grgg', compute_grgg)
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'
    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'grgg'])
    logger.info('update records....')
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__grgg', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA gradi giorno')


def process_dma_pressione(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of pressione

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA pressione atmosferica')
    logger.info('selecting for input records...')
    # records are: (metadata, datetime object, par_code, par_value, flag)
    subfields = ['val_md', 'val_vr', 'val_mx', 'val_mn']
    array_field = 'ARRAY[' + ','.join(['(%s).%s' % ('press', s) for s in subfields]) + ']'
    sql_fields = "cod_staz, data_i, '%s', %s, ((%s).flag).wht" % ('press', array_field, 'press')
    table_records = querying.select_records(
        conn, 'ds__press', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=startschema)
    logger.info('computing aggregations...')
    data = compute_dma_records(table_records, 'press', compute_press)
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'
    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'press'])
    logger.info('update records....')
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__press', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA pressione atmosferica')


def process_dma_umidita_relativa(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of umidità relativa

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA umidità relativa')
    logger.info('selecting for input records...')
    # records are: (metadata, datetime object, par_code, par_value, flag)
    subfields = ['val_md', 'val_vr', 'val_mx', 'val_mn']
    array_field = 'ARRAY[' + ','.join(['(%s).%s' % ('ur', s) for s in subfields]) + ']'
    sql_fields = "cod_staz, data_i, '%s', %s, ((%s).flag).wht" % ('ur', array_field, 'ur')
    table_records = querying.select_records(
        conn, 'ds__urel', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=startschema)
    logger.info('computing aggregations...')
    data = compute_dma_records(table_records, 'ur', compute_ur)
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'

    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'ur'])
    logger.info('update records....')
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__urel', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA umidità relativa')


def process_dma_bioclimatologia(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of bioclimatologia

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    conn_r = db_utils.get_safe_memory_read_connection(conn)
    logger.info('starting process DMA bioclimatologia')
    logger.info('selecting for input records...')
    station_ids_tuple = '(%s)' % repr(stations_ids)[1:-1]
    # records are: (metadata, datetime object, par_code, par_value, flag)
    sql = """
    SELECT cod_staz, data_i, '', ARRAY[(tmdgg1).val_md, (ur).val_md], 
            ((tmdgg1).flag).wht>0 AND ((ur).flag).wht>0 
    FROM %s.ds__t200 JOIN %s.ds__urel USING (cod_staz, data_i)
    WHERE (tmdgg1).val_md IS NOT NULL AND (ur).val_md IS NOT NULL
    AND cod_staz in %s
    ORDER BY cod_staz, data_i""" % (startschema, startschema, station_ids_tuple)
    table_records = conn_r.execute(sql)
    map_funct = {
        'ifs': compute_ifs,
        'ifu': compute_ifu,
        'ics': compute_ics,
        'icu': compute_icu,
        'sharl': compute_sharl,
        'ifu1': compute_ifu1,
    }
    logger.info('computing aggregations...')
    data = compute_dma_records(table_records, map_funct=map_funct)
    fields = upsert.expand_fields(
        ['data_i', 'cod_staz', 'cod_aggr', 'provenienza'] + list(map_funct.keys()))
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'
    logger.info('update records....')
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__bioclima', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA bioclimatologia')


def process_dma_precipitazione(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of precipitazione

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA precipitazione')
    logger.info('selecting for input records...')

    sql_fields = "cod_staz, data_i, " \
                 "(prec01).val_mx, ((prec01).flag).wht, " \
                 "(prec24).val_tot, ((prec24).flag).wht, " \
                 "(prec12).val_mx, ((prec12).flag).wht, " \
                 "(prec06).val_mx, ((prec06).flag).wht, " \
                 "(cl_prec06).dry, (cl_prec06).wet_01, " \
                 "(cl_prec06).wet_02, (cl_prec06).wet_03, " \
                 "(cl_prec06).wet_04, (cl_prec06).wet_05, " \
                 "(cl_prec12).dry, (cl_prec12).wet_01, " \
                 "(cl_prec12).wet_02, (cl_prec12).wet_03, " \
                 "(cl_prec12).wet_04, (cl_prec12).wet_05"

    logger.info('computing aggregations for prec01...')

    def get_table_records():
        table_records = querying.select_records(
            conn, 'ds__preci', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
            schema=startschema)
        return table_records

    def get_prec01_records():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'prec01', (r[2],), r[3]]
    prec01_records = get_prec01_records()
    data_prec01 = compute_dma_records(prec01_records, 'prec01', compute_prec01_06_12)

    logger.info('selecting for input records (prec24) ...')

    def get_prec24_records():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'prec24', r[4], r[5]]
    prec24_records = get_prec24_records()
    map_funct = {
        'prec24': compute_prec24,
        'cl_prec24': compute_cl_prec24,
    }
    logger.info('computing aggregations for prec24...')
    data_prec24 = compute_dma_records(prec24_records, map_funct=map_funct)

    logger.info('computing aggregations for persistenza precipitazione')
    data_prs_prec = compute_year_records(prec24_records, 'prs_prec', compute_prs_prec)

    logger.info('selecting for input records (prec12) ...')

    def get_prec12_records():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'prec12', ([r[6]]+r[16:]), r[7]]
    prec12_records = get_prec12_records()
    map_funct = {
        'prec12': compute_prec01_06_12,
        'cl_prec12': compute_cl_prec_06_12,
    }
    logger.info('computing aggregations for prec12...')
    data_prec12 = compute_dma_records(prec12_records, map_funct=map_funct)

    logger.info('selecting for input records (prec06) ...')

    def get_data_prec06():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'prec06', ([r[8]]+r[10:16]), r[9]]
    data_prec06 = get_data_prec06()

    map_funct = {
        'prec06': compute_prec01_06_12,
        'cl_prec06': compute_cl_prec_06_12,
    }
    logger.info('computing aggregations for prec06...')
    data_prec06 = compute_dma_records(data_prec06, map_funct=map_funct)

    logger.info('updating table ds__prs_prec')
    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'prs_prec', 'provenienza'])
    for sub_data_prs_prec in utils.chunked_iterable(data_prs_prec, 10000):
        sql = upsert.create_upsert('ds__prs_prec', targetschema, fields, sub_data_prs_prec, policy)
        if sql:
            conn.execute(sql)

    logger.info('merging records before update of table ds__prec...')
    data = functools.reduce(merge_data_items, [data_prec01, data_prec24, data_prec12, data_prec06])
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'

    logger.info('update records for table ds__prec...')
    fields = upsert.expand_fields(
        ['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'prec01', 'prec24', 'cl_prec24',
         'prec12', 'cl_prec12', 'prec06', 'cl_prec06'])
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__preci', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)

    logger.info('end process DMA precipitazione')


def process_dma_vento(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of vento

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA vento')
    logger.info('selecting for input records...')
    data_items = dict()
    wind_subfields = ['frq_s%02.dc%d' % (i, j) for i in range(1, 17) for j in range(1, 5)]
    vnt_array_field = 'ARRAY[(vnt).frq_calme,' + \
                      ','.join(['(vnt).%s' % s for s in wind_subfields]) + ']'
    sql_fields = "cod_staz, data_i, (vntmd).ff, ((vntmd).flag).wht, " \
                 "ARRAY[(vntmxgg).ff,(vntmxgg).dd], ((vntmxgg).flag).wht, " \
                 "%s, ((vnt).flag).wht" % vnt_array_field

    def get_table_records():
        table_records = querying.select_records(
            conn, 'ds__vnt10', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
            schema=startschema)
        return table_records

    def get_vntmx_records():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'vntmxgg', r[4], r[5]]

    logger.info('selecting for input records (vntmxgg)...')
    vntmx_records = get_vntmx_records()
    logger.info('computing aggregations (vntmd)...')
    data_items['vntmxgg'] = compute_dma_records(vntmx_records, 'vntmxgg', compute_vntmxgg)

    logger.info('selecting for input records (vnt)...')

    def get_vnt_records():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'vnt', r[6], r[7]]

    vnt_records = get_vnt_records()
    logger.info('computing aggregations (vnt)...')
    data_items['vnt'] = compute_dma_records(vnt_records, 'vnt', compute_vnt)

    logger.info('computing aggregations (vntmd)...')

    def get_vntmd_records():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'vntmd', r[2], r[3]]

    vntmd_records = get_vntmd_records()
    data_items['vntmd'] = compute_dma_records(vntmd_records, 'vntmd', compute_vntmd)

    logger.info('merging records before update of table ds__prec...')
    data = functools.reduce(merge_data_items, data_items.values())

    logger.info('update records...')
    fields = upsert.expand_fields(
        ['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'vntmxgg', 'vntmd', 'vnt'])
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__vnt10', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA vento')


def process_dma_temperatura(conn, startschema, targetschema, policy, stations_ids, logger):
    """
    process the compute and update of DMA data for table of temperature

    :param conn: db connection object
    :param startschema: db start schema
    :param targetschema: db target schema
    :param policy: onlyinsert or upsert
    :param stations_ids: list of station ids to consider
    :param logger: logging object for function reporting
    """
    logger.info('starting process DMA temperatura')
    logger.info('selecting for input records...')
    data_items = dict()
    sql_fields = "cod_staz, data_i, " \
                 "(tmxgg).val_md, ((tmxgg).flag).wht, " \
                 "(tmngg).val_md, ((tmngg).flag).wht, " \
                 "(tmdgg).val_md, ((tmdgg).flag).wht"

    def get_table_records():
        table_records = querying.select_records(
            conn, 'ds__t200', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
            schema=startschema)
        return table_records

    logger.info('computing aggregations (persistenza temperatura tmax)...')

    def get_tmax_records():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'tmax', r[2], r[3]]

    tmax_records = get_tmax_records()
    prs_t200mx = compute_year_records(tmax_records, 'prs_t200mx', compute_prs_t200mx)

    logger.info('computing aggregations (persistenza temperatura tmin)...')

    def get_tmin_records():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'tmin', r[4], r[5]]

    tmin_records = get_tmin_records()
    prs_t200mn = compute_year_records(tmin_records, 'prs_t200mn', compute_prs_t200mn)

    logger.info('merging records before update of table ds__prs_t200...')
    data_prs = functools.reduce(merge_data_items, [prs_t200mx, prs_t200mn])

    logger.info('update records of table ds__prs_t200...')
    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'prs_t200mx',
                                   'prs_t200mn'])
    for sub_data_prs in utils.chunked_iterable(data_prs, 10000):
        sql = upsert.create_upsert('ds__prs_t200', targetschema, fields, sub_data_prs, policy)
        if sql:
            logger.info('updating DMA table %s.%s' % (targetschema, 'ds__prs_t200'))
            conn.execute(sql)

    logger.info('computing aggregations (tmdgg)...')

    def get_tmd_records():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'tmdgg', r[6], r[7]]

    tmd_records = get_tmd_records()
    data_items['tmdgg'] = compute_dma_records(tmd_records, 'tmdgg', compute_tmdgg)
    logger.info('computing aggregations (tmxgg)...')
    tmax_records = get_tmax_records()
    data_items['tmxgg'] = compute_dma_records(tmax_records, 'tmxgg', compute_tmxgg)
    logger.info('computing aggregations (tmngg)...')
    tmin_records = get_tmin_records()
    data_items['tmngg'] = compute_dma_records(tmin_records, 'tmngg', compute_tmngg)
    logger.info('computing aggregations (cl_tmxgg)...')
    tmax_records = get_tmax_records()
    data_items['c_tmxgg'] = compute_dma_records(tmax_records, 'cl_tmxgg', compute_cl_tmxgg)
    logger.info('computing aggregations (cl_tmngg)...')
    tmin_records = get_tmin_records()
    data_items['c_tmngg'] = compute_dma_records(tmin_records, 'cl_tmngg', compute_cl_tmngg)
    logger.info('computing aggregations (tmdgg1)...')

    def get_tmdgg1_records():
        table_records = get_table_records()
        for r in table_records:
            yield [r[0], r[1], 'tmdgg1', [r[2], r[4]], r[3] and r[5]]

    tmdgg1_records = get_tmdgg1_records()
    data_items['tmdgg1'] = compute_dma_records(tmdgg1_records, 'tmdgg1', compute_tmdgg1)
    logger.info('computing aggregations (deltagg)...')
    tmdgg1_records = get_tmdgg1_records()
    data_items['deltagg'] = compute_dma_records(tmdgg1_records, 'deltagg', compute_deltagg)
    logger.info('computing aggregations (day_gelo)...')
    tmax_records = get_tmax_records()
    data_items['day_gelo'] = compute_dma_records(tmax_records, 'day_gelo', compute_day_gelo)

    logger.info('merging records before update of table ds__t200...')
    data = functools.reduce(merge_data_items, data_items.values())
    logger.info('setting provenienza="DAILY" for the resulting records...')
    for record in data:
        record['provenienza'] = 'DAILY'

    logger.info('update records of table ds__t200...')
    fields = upsert.expand_fields(['data_i', 'cod_staz', 'cod_aggr', 'provenienza', 'tmdgg',
                                   'tmxgg', 'tmngg', 'cl_tmxgg', 'cl_tmngg', 'tmdgg1', 'deltagg',
                                   'day_gelo'])
    for sub_data in utils.chunked_iterable(data, 10000):
        sql = upsert.create_upsert('ds__t200', targetschema, fields, sub_data, policy)
        if sql:
            conn.execute(sql)
    logger.info('end process DMA temperatura')
