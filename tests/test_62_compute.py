
from datetime import datetime, timedelta

from sciafeed import compute

from pprint import pprint
from sciafeed import utils
sample_metadata = {'a metadata': 'a value'}



def create_samples(par_code, hour_step=1):
    metadata = sample_metadata.copy()
    start_time = datetime(2020, 1, 1, 0, 0)
    ret_value = []
    for par_value, thehour in enumerate(range(0, 24, hour_step)):
        record = (metadata, start_time+timedelta(hours=thehour), par_code, par_value, True)
        ret_value.append(record)
    return ret_value


def test_sum_records_by_hour_groups():
    day_records = create_samples('PREC')
    # step h=1
    new_records = compute.sum_records_by_hour_groups(day_records, 1)
    assert new_records == day_records

    # step h=2
    new_records = compute.sum_records_by_hour_groups(day_records, 2)
    assert new_records == [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 1, True),
        (sample_metadata, datetime(2020, 1, 1, 2, 0), 'PREC', 5, True),
        (sample_metadata, datetime(2020, 1, 1, 4, 0), 'PREC', 9, True),
        (sample_metadata, datetime(2020, 1, 1, 6, 0), 'PREC', 13, True),
        (sample_metadata, datetime(2020, 1, 1, 8, 0), 'PREC', 17, True),
        (sample_metadata, datetime(2020, 1, 1, 10, 0), 'PREC', 21, True),
        (sample_metadata, datetime(2020, 1, 1, 12, 0), 'PREC', 25, True),
        (sample_metadata, datetime(2020, 1, 1, 14, 0), 'PREC', 29, True),
        (sample_metadata, datetime(2020, 1, 1, 16, 0), 'PREC', 33, True),
        (sample_metadata, datetime(2020, 1, 1, 18, 0), 'PREC', 37, True),
        (sample_metadata, datetime(2020, 1, 1, 20, 0), 'PREC', 41, True),
        (sample_metadata, datetime(2020, 1, 1, 22, 0), 'PREC', 45, True)
    ]

    # step h=3
    new_records = compute.sum_records_by_hour_groups(day_records, 3)
    assert new_records == [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 3, True),
        (sample_metadata, datetime(2020, 1, 1, 3, 0), 'PREC', 12, True),
        (sample_metadata, datetime(2020, 1, 1, 6, 0), 'PREC', 21, True),
        (sample_metadata, datetime(2020, 1, 1, 9, 0), 'PREC', 30, True),
        (sample_metadata, datetime(2020, 1, 1, 12, 0), 'PREC', 39, True),
        (sample_metadata, datetime(2020, 1, 1, 15, 0), 'PREC', 48, True),
        (sample_metadata, datetime(2020, 1, 1, 18, 0), 'PREC', 57, True),
        (sample_metadata, datetime(2020, 1, 1, 21, 0), 'PREC', 66, True)
    ]

    # step h=4
    new_records = compute.sum_records_by_hour_groups(day_records, 4)
    assert new_records == [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 6, True),
        (sample_metadata, datetime(2020, 1, 1, 4, 0), 'PREC', 22, True),
        (sample_metadata, datetime(2020, 1, 1, 8, 0), 'PREC', 38, True),
        (sample_metadata, datetime(2020, 1, 1, 12, 0), 'PREC', 54, True),
        (sample_metadata, datetime(2020, 1, 1, 16, 0), 'PREC', 70, True),
        (sample_metadata, datetime(2020, 1, 1, 20, 0), 'PREC', 86, True)
    ]

    # step h=6
    new_records = compute.sum_records_by_hour_groups(day_records, 6)
    assert new_records == [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 15, True),
        (sample_metadata, datetime(2020, 1, 1, 6, 0), 'PREC', 51, True),
        (sample_metadata, datetime(2020, 1, 1, 12, 0), 'PREC', 87, True),
        (sample_metadata, datetime(2020, 1, 1, 18, 0), 'PREC', 123, True)
    ]

    # step h=8
    new_records = compute.sum_records_by_hour_groups(day_records, 8)
    assert new_records == [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 28, True),
        (sample_metadata, datetime(2020, 1, 1, 8, 0), 'PREC', 92, True),
        (sample_metadata, datetime(2020, 1, 1, 16, 0), 'PREC', 156, True)
    ]

    # step h=12
    new_records = compute.sum_records_by_hour_groups(day_records, 12)
    assert new_records == [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 66, True),
        (sample_metadata, datetime(2020, 1, 1, 12, 0), 'PREC', 210, True)
    ]

    # step h=24
    new_records = compute.sum_records_by_hour_groups(day_records, 24)
    assert new_records == [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 276, True)
    ]


def test_wet_distribution():
    day_records = create_samples('PREC')
    res = compute.wet_distribution(day_records)
    assert res == (2, 4, 5, 10, 3, 0)


def test_compute_prec24():
    # good set
    day_records = create_samples('PREC')
    flag, val_tot, val_mx, data_mx = compute.compute_prec24(day_records, at_least_perc=0.75)
    assert (flag, val_tot, val_mx, data_mx) == ((24, 1), 276, 23, '2020-01-01T23:00:00')

    # partially good
    day_records = [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 0, False),
        (sample_metadata, datetime(2020, 1, 1, 1, 0), 'PREC', 1, False),
        (sample_metadata, datetime(2020, 1, 1, 2, 0), 'PREC', 2, False),
        (sample_metadata, datetime(2020, 1, 1, 3, 0), 'PREC', 3, False),
        (sample_metadata, datetime(2020, 1, 1, 4, 0), 'PREC', 4, False),
        (sample_metadata, datetime(2020, 1, 1, 5, 0), 'PREC', 5, False),
        (sample_metadata, datetime(2020, 1, 1, 6, 0), 'PREC', 6, True),
        (sample_metadata, datetime(2020, 1, 1, 7, 0), 'PREC', 7, True),
        (sample_metadata, datetime(2020, 1, 1, 8, 0), 'PREC', 8, True),
        (sample_metadata, datetime(2020, 1, 1, 9, 0), 'PREC', 9, True),
        (sample_metadata, datetime(2020, 1, 1, 10, 0), 'PREC', 10, True),
        (sample_metadata, datetime(2020, 1, 1, 11, 0), 'PREC', 11, True),
        (sample_metadata, datetime(2020, 1, 1, 12, 0), 'PREC', 12, True),
        (sample_metadata, datetime(2020, 1, 1, 13, 0), 'PREC', 13, True),
        (sample_metadata, datetime(2020, 1, 1, 14, 0), 'PREC', 14, True),
        (sample_metadata, datetime(2020, 1, 1, 15, 0), 'PREC', 15, True),
        (sample_metadata, datetime(2020, 1, 1, 16, 0), 'PREC', 16, True),
        (sample_metadata, datetime(2020, 1, 1, 17, 0), 'PREC', 17, True),
        (sample_metadata, datetime(2020, 1, 1, 18, 0), 'PREC', 18, True),
        (sample_metadata, datetime(2020, 1, 1, 19, 0), 'PREC', 19, True),
        (sample_metadata, datetime(2020, 1, 1, 20, 0), 'PREC', 20, True),
        (sample_metadata, datetime(2020, 1, 1, 21, 0), 'PREC', 21, True),
        (sample_metadata, datetime(2020, 1, 1, 22, 0), 'PREC', 22, True),
        (sample_metadata, datetime(2020, 1, 1, 23, 0), 'PREC', 23, True),
    ]
    flag, val_tot, val_mx, data_mx = compute.compute_prec24(day_records, at_least_perc=0.75)
    assert (flag, val_tot, val_mx, data_mx) == ((18, 1), 261, 23, '2020-01-01T23:00:00')

    # partially bad (higher percentage)
    flag, val_tot, val_mx, data_mx = compute.compute_prec24(day_records, at_least_perc=0.80)
    assert (flag, val_tot, val_mx, data_mx) == ((18, 0), 261, 23, '2020-01-01T23:00:00')

    # good but missing too many
    day_records = [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 0, True),
        (sample_metadata, datetime(2020, 1, 1, 1, 0), 'PREC', 1, True),
        (sample_metadata, datetime(2020, 1, 1, 2, 0), 'PREC', 2, True),
        (sample_metadata, datetime(2020, 1, 1, 3, 0), 'PREC', 3, True),
        (sample_metadata, datetime(2020, 1, 1, 4, 0), 'PREC', 4, True),
        (sample_metadata, datetime(2020, 1, 1, 5, 0), 'PREC', 5, True),
    ]
    flag, val_tot, val_mx, data_mx = compute.compute_prec24(day_records, at_least_perc=0.80)
    assert (flag, val_tot, val_mx, data_mx) == ((6, 0), 15, 5, '2020-01-01T05:00:00')

    # empty
    day_records = []
    res = compute.compute_prec24(day_records, at_least_perc=0.80)
    assert res is None


def test_compute_cl_prec24():
    day_records = [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 0, False),
        (sample_metadata, datetime(2020, 1, 1, 1, 0), 'PREC', 1, False),
        (sample_metadata, datetime(2020, 1, 1, 2, 0), 'PREC', 2, False),
        (sample_metadata, datetime(2020, 1, 1, 3, 0), 'PREC', 3, False),
        (sample_metadata, datetime(2020, 1, 1, 4, 0), 'PREC', 4, False),
        (sample_metadata, datetime(2020, 1, 1, 5, 0), 'PREC', 5, False),
        (sample_metadata, datetime(2020, 1, 1, 6, 0), 'PREC', 6, True),
        (sample_metadata, datetime(2020, 1, 1, 7, 0), 'PREC', 7, True),
        (sample_metadata, datetime(2020, 1, 1, 8, 0), 'PREC', 8, True),
        (sample_metadata, datetime(2020, 1, 1, 9, 0), 'PREC', 9, True),
        (sample_metadata, datetime(2020, 1, 1, 10, 0), 'PREC', 10, True),
        (sample_metadata, datetime(2020, 1, 1, 11, 0), 'PREC', 11, True),
        (sample_metadata, datetime(2020, 1, 1, 12, 0), 'PREC', 12, True),
        (sample_metadata, datetime(2020, 1, 1, 13, 0), 'PREC', 13, True),
        (sample_metadata, datetime(2020, 1, 1, 14, 0), 'PREC', 14, True),
        (sample_metadata, datetime(2020, 1, 1, 15, 0), 'PREC', 15, True),
        (sample_metadata, datetime(2020, 1, 1, 16, 0), 'PREC', 16, True),
        (sample_metadata, datetime(2020, 1, 1, 17, 0), 'PREC', 17, True),
        (sample_metadata, datetime(2020, 1, 1, 18, 0), 'PREC', 18, True),
        (sample_metadata, datetime(2020, 1, 1, 19, 0), 'PREC', 19, True),
        (sample_metadata, datetime(2020, 1, 1, 20, 0), 'PREC', 20, True),
        (sample_metadata, datetime(2020, 1, 1, 21, 0), 'PREC', 21, True),
        (sample_metadata, datetime(2020, 1, 1, 22, 0), 'PREC', 22, True),
        (sample_metadata, datetime(2020, 1, 1, 23, 0), 'PREC', 23, True),
    ]
    res = compute.compute_cl_prec24(day_records)
    assert res == (0, 0, 5, 10, 3, 0)


def test_compute_prec01():
    day_records = create_samples('PREC')
    flag, val_mx, data_mx = compute.compute_prec01(day_records, at_least_perc=0.75)
    assert (flag, val_mx, data_mx) == ((24, 1), 23, '2020-01-01T23:00:00')

    # good but missing too many
    day_records = [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 0, True),
        (sample_metadata, datetime(2020, 1, 1, 1, 0), 'PREC', 1, True),
        (sample_metadata, datetime(2020, 1, 1, 2, 0), 'PREC', 2, True),
        (sample_metadata, datetime(2020, 1, 1, 3, 0), 'PREC', 3, True),
        (sample_metadata, datetime(2020, 1, 1, 4, 0), 'PREC', 4, True),
        (sample_metadata, datetime(2020, 1, 1, 5, 0), 'PREC', 5, True),
    ]
    flag, val_mx, data_mx = compute.compute_prec01(day_records, at_least_perc=0.80)
    assert (flag, val_mx, data_mx) == ((6, 0), 5, '2020-01-01T05:00:00')
    # with low tolerance
    flag, val_mx, data_mx = compute.compute_prec01(day_records, at_least_perc=0.10)
    assert (flag, val_mx, data_mx) == ((6, 1), 5, '2020-01-01T05:00:00')


def test_compute_prec06():
    day_records = create_samples('PREC')
    flag, val_mx, data_mx = compute.compute_prec06(day_records, at_least_perc=0.75)
    assert (flag, val_mx, data_mx) == ((24, 1), 123, '2020-01-01T18:00:00')


def test_compute_cl_prec06():
    day_records = create_samples('PREC')
    dry, wet_01, wet_02, wet_03, wet_04, wet_05 = compute.compute_cl_prec06(day_records)
    assert (dry, wet_01, wet_02, wet_03, wet_04, wet_05) == (0, 0, 0, 1, 0, 3)


def test_compute_prec12():
    day_records = create_samples('PREC')
    flag, val_mx, data_mx = compute.compute_prec12(day_records, at_least_perc=0.75)
    assert (flag, val_mx, data_mx) == ((24, 1), 210, '2020-01-01T12:00:00')


def test_compute_cl_prec12():
    day_records = create_samples('PREC')
    dry, wet_01, wet_02, wet_03, wet_04, wet_05 = compute.compute_cl_prec12(day_records)
    assert (dry, wet_01, wet_02, wet_03, wet_04, wet_05) == (0, 0, 0, 0, 0, 2)


def test_compute_temperature_flag():
    # right records
    input_records = create_samples('Tmedia')
    assert compute.compute_temperature_flag(input_records) == (24, 1)
    # all at night
    input_records1 = [
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'Tmedia', 0, True),
        (sample_metadata, datetime(2020, 1, 1, 1, 0), 'Tmedia', 1, True),
        (sample_metadata, datetime(2020, 1, 1, 2, 0), 'Tmedia', 2, True),
        (sample_metadata, datetime(2020, 1, 1, 3, 0), 'Tmedia', 3, True),
        (sample_metadata, datetime(2020, 1, 1, 4, 0), 'Tmedia', 4, True),
        (sample_metadata, datetime(2020, 1, 1, 5, 0), 'Tmedia', 5, True),
    ]
    assert compute.compute_temperature_flag(input_records1) == (6, 0)
    # all at daylight
    input_records2 = [
        (sample_metadata, datetime(2020, 1, 1, 9, 0), 'Tmedia', 0, True),
        (sample_metadata, datetime(2020, 1, 1, 10, 0), 'Tmedia', 1, True),
        (sample_metadata, datetime(2020, 1, 1, 11, 0), 'Tmedia', 2, True),
        (sample_metadata, datetime(2020, 1, 1, 12, 0), 'Tmedia', 3, True),
        (sample_metadata, datetime(2020, 1, 1, 13, 0), 'Tmedia', 4, True),
        (sample_metadata, datetime(2020, 1, 1, 14, 0), 'Tmedia', 5, True),
    ]
    assert compute.compute_temperature_flag(input_records2) == (6, 0)
    # not enought for day and night
    input_records3 = input_records1 + input_records2
    assert compute.compute_temperature_flag(input_records3) == (12, 0)


def test_compute_tmdgg():
    input_records = create_samples('Tmedia')
    flag, val_md, val_vr = compute.compute_tmdgg(input_records, at_least_perc=0.75)
    assert (flag, val_md, val_vr) == ((24, 1), 11.5, 7.1)


def test_compute_tmxgg():
    input_records = create_samples('Tmax')
    flag, val_md, val_vr, val_x, data_x = compute.compute_tmxgg(input_records, at_least_perc=0.75)
    assert (flag, val_md, val_vr, val_x, data_x) == ((24, 1), 23, 7.1, 11.5, '2020-01-01T23:00:00')


def test_compute_tmngg():
    input_records = create_samples('Tmin')
    flag, val_md, val_vr, val_x, data_x = compute.compute_tmngg(input_records, at_least_perc=0.75)
    assert (flag, val_md, val_vr, val_x, data_x) == ((24, 1), 0, 7.1, 11.5, '2020-01-01T00:00:00')


def test_compute_press():
    day_records_pmedia = create_samples('P')
    day_records_pmax = create_samples('Pmax')
    day_records_pmin = create_samples('Pmin')

    flag, val_md, val_vr, val_mx, val_mn = compute.compute_press(
        day_records_pmedia, day_records_pmax, day_records_pmin)
    assert (flag, val_md, val_vr, val_mx, val_mn) == ((24, 1), 11.5, 7.1, 23, 0)


def test_compute_bagna():
    metadata = sample_metadata.copy()
    day_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'Bagnatura_f', 46.6998, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'Bagnatura_f', 19.3586, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'Bagnatura_f', 32.8168, False),
        (metadata, datetime(2020, 1, 1, 3, 0), 'Bagnatura_f', 49.6327, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'Bagnatura_f', 5.8928, False),
        (metadata, datetime(2020, 1, 1, 5, 0), 'Bagnatura_f', 1.8212, False),
        (metadata, datetime(2020, 1, 1, 6, 0), 'Bagnatura_f', 22.0986, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'Bagnatura_f', 45.9589, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'Bagnatura_f', 26.5065, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'Bagnatura_f', 20.6426, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'Bagnatura_f', 54.9259, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'Bagnatura_f', 6.737, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'Bagnatura_f', 45.4702, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'Bagnatura_f', 42.287, False),
        (metadata, datetime(2020, 1, 1, 14, 0), 'Bagnatura_f', 4.2066, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'Bagnatura_f', 21.8372, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'Bagnatura_f', 20.4227, True),
        (metadata, datetime(2020, 1, 1, 17, 0), 'Bagnatura_f', 22.0616, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'Bagnatura_f', 48.8321, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'Bagnatura_f', 32.1013, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'Bagnatura_f', 45.4906, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'Bagnatura_f', 4.2239, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'Bagnatura_f', 57.1591, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'Bagnatura_f', 11.9748, False),
    ]
    flag, val_md, val_vr, val_mx, val_mn, val_tot = compute.compute_bagna(day_records)
    assert (flag, val_md, val_vr, val_mx, val_mn, val_tot) == \
           ((11, 0), 4.9, 0.3, 0.8, 0.1, 0.4)


def test_compute_elio():
    metadata = sample_metadata.copy()
    day_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'INSOL', 12.3156, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'INSOL', 53.384, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'INSOL', 8.9947, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'INSOL', 17.9736, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'INSOL', 41.2483, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'INSOL', 12.0817, False),
        (metadata, datetime(2020, 1, 1, 6, 0), 'INSOL', 44.4205, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'INSOL', 49.4548, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'INSOL', 33.5316, False),
        (metadata, datetime(2020, 1, 1, 9, 0), 'INSOL', 4.5963, False),
        (metadata, datetime(2020, 1, 1, 10, 0), 'INSOL', 23.815, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'INSOL', 18.4742, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'INSOL', 25.068, False),
        (metadata, datetime(2020, 1, 1, 13, 0), 'INSOL', 50.023, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'INSOL', 7.0285, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'INSOL', 29.6455, False),
        (metadata, datetime(2020, 1, 1, 16, 0), 'INSOL', 3.0326, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'INSOL', 41.8781, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'INSOL', 54.6086, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'INSOL', 53.9748, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'INSOL', 13.0228, True),
        (metadata, datetime(2020, 1, 1, 21, 0), 'INSOL', 12.48, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'INSOL', 31.0874, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'INSOL', 35.6489, False)
    ]
    flag, val_md, val_vr, val_mx = compute.compute_elio(day_records)
    # NOTE: val_md is the SUM
    assert (flag, val_md, val_vr, val_mx) == ((9, 0), 4.4, 0.3, None)


def test_compute_radglob():
    metadata = sample_metadata.copy()
    day_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'RADSOL', 29.503, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'RADSOL', 80.9462, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'RADSOL', 71.3331, False),
        (metadata, datetime(2020, 1, 1, 3, 0), 'RADSOL', 28.2172, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'RADSOL', 9.2708, False),
        (metadata, datetime(2020, 1, 1, 5, 0), 'RADSOL', 20.3447, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'RADSOL', 3.8309, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'RADSOL', 26.1966, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'RADSOL', 58.7787, False),
        (metadata, datetime(2020, 1, 1, 9, 0), 'RADSOL', 85.7274, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'RADSOL', 66.2083, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'RADSOL', 65.1714, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'RADSOL', 29.8974, False),
        (metadata, datetime(2020, 1, 1, 13, 0), 'RADSOL', 41.012, False),
        (metadata, datetime(2020, 1, 1, 14, 0), 'RADSOL', 34.1688, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'RADSOL', 83.0679, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'RADSOL', 6.9506, True),
        (metadata, datetime(2020, 1, 1, 17, 0), 'RADSOL', 74.8353, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'RADSOL', 9.9903, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'RADSOL', 30.9549, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'RADSOL', 75.6266, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'RADSOL', 47.9269, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'RADSOL', 22.7512, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'RADSOL', 11.4268, False),
    ]
    flag, val_md, val_vr, val_mx, val_mn = compute.compute_radglob(day_records)
    assert (flag, val_md, val_vr, val_mx, val_mn) == ((9, 0), 21.8, 14.4, 41.5, 3.4)


def test_compute_ur():
    metadata = sample_metadata.copy()
    day_records_urmedia = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'UR media', 93.0725, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'UR media', 14.0488, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'UR media', 19.5679, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'UR media', 98.4374, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'UR media', 60.5525, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'UR media', 5.3557, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'UR media', 19.0187, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'UR media', 40.0136, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'UR media', 81.0316, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'UR media', 38.7842, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'UR media', 42.5653, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'UR media', 78.6204, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'UR media', 32.623, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'UR media', 8.1728, False),
        (metadata, datetime(2020, 1, 1, 14, 0), 'UR media', 46.7942, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'UR media', 19.52, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'UR media', 73.0788, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'UR media', 69.9987, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'UR media', 44.1692, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'UR media', 0.7595, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'UR media', 13.4624, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'UR media', 72.7669, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'UR media', 4.1295, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'UR media', 48.7278, False)
    ]
    day_records_urmax = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'UR max', 1.1287, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'UR max', 84.5, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'UR max', 7.051, False),
        (metadata, datetime(2020, 1, 1, 3, 0), 'UR max', 86.2606, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'UR max', 19.5809, False),
        (metadata, datetime(2020, 1, 1, 5, 0), 'UR max', 82.32, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'UR max', 56.0317, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'UR max', 73.2838, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'UR max', 76.6591, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'UR max', 84.9542, False),
        (metadata, datetime(2020, 1, 1, 10, 0), 'UR max', 80.976, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'UR max', 2.5568, False),
        (metadata, datetime(2020, 1, 1, 12, 0), 'UR max', 24.7245, False),
        (metadata, datetime(2020, 1, 1, 13, 0), 'UR max', 2.1739, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'UR max', 19.1577, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'UR max', 11.1011, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'UR max', 90.4718, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'UR max', 67.5488, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'UR max', 97.9166, True),
        (metadata, datetime(2020, 1, 1, 19, 0), 'UR max', 25.1655, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'UR max', 54.2473, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'UR max', 80.8128, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'UR max', 71.5309, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'UR max', 23.1089, True)
    ]
    day_records_urmin = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'UR min', 93.4062, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'UR min', 57.377, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'UR min', 63.7623, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'UR min', 65.5366, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'UR min', 6.0608, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'UR min', 7.9139, False),
        (metadata, datetime(2020, 1, 1, 6, 0), 'UR min', 41.3705, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'UR min', 94.9941, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'UR min', 98.9935, False),
        (metadata, datetime(2020, 1, 1, 9, 0), 'UR min', 6.9763, False),
        (metadata, datetime(2020, 1, 1, 10, 0), 'UR min', 83.4629, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'UR min', 0.0951, False),
        (metadata, datetime(2020, 1, 1, 12, 0), 'UR min', 83.8421, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'UR min', 28.7116, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'UR min', 38.8209, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'UR min', 98.0396, False),
        (metadata, datetime(2020, 1, 1, 16, 0), 'UR min', 74.3587, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'UR min', 36.3907, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'UR min', 91.5099, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'UR min', 26.6897, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'UR min', 74.6414, True),
        (metadata, datetime(2020, 1, 1, 21, 0), 'UR min', 86.2716, True),
        (metadata, datetime(2020, 1, 1, 22, 0), 'UR min', 20.7062, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'UR min', 54.3221, True)
    ]
    flag, val_md, val_vr, flag1, val_mx, val_mn = compute.compute_ur(
        day_records_urmedia, day_records_urmax, day_records_urmin)
    assert (flag, val_md, val_vr, flag1, val_mx, val_mn) == \
           ((11, 0), 39.8, 24.8, (None, None), 97.9, 6.1)


def test_compute_vntmd():
    metadata = sample_metadata.copy()
    day_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'FF', 17.3247, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'FF', 68.9561, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'FF', 26.4429, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'FF', 99.828, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'FF', 7.7739, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'FF', 98.4608, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'FF', 59.3268, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'FF', 86.4781, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'FF', 20.4499, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'FF', 49.0313, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'FF', 23.5007, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'FF', 86.5212, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'FF', 98.4362, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'FF', 55.9767, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'FF', 38.2074, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'FF', 6.5295, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'FF', 88.8914, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'FF', 97.5881, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'FF', 24.5994, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'FF', 27.0289, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'FF', 24.5575, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'FF', 13.6417, True),
        (metadata, datetime(2020, 1, 1, 22, 0), 'FF', 79.6259, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'FF', 2.6536, True)
    ]
    flag, ff = compute.compute_vntmd(day_records, at_least_perc=0.75, force_flag=None)
    assert (flag, ff) == ((18, 1), 52.0)


def test_compute_wind_flag():
    metadata = sample_metadata.copy()
    day_ff_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'FF', 17.3247, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'FF', 68.9561, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'FF', 26.4429, True),  #
        (metadata, datetime(2020, 1, 1, 3, 0), 'FF', 99.828, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'FF', 7.7739, True),  #
        (metadata, datetime(2020, 1, 1, 5, 0), 'FF', 98.4608, True),  #
        (metadata, datetime(2020, 1, 1, 6, 0), 'FF', 59.3268, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'FF', 86.4781, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'FF', 20.4499, True),  #
        (metadata, datetime(2020, 1, 1, 9, 0), 'FF', 49.0313, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'FF', 23.5007, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'FF', 86.5212, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'FF', 98.4362, True),  #
        (metadata, datetime(2020, 1, 1, 13, 0), 'FF', 55.9767, True),  #
        (metadata, datetime(2020, 1, 1, 14, 0), 'FF', 38.2074, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'FF', 6.5295, True),  #
        (metadata, datetime(2020, 1, 1, 16, 0), 'FF', 88.8914, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'FF', 97.5881, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'FF', 24.5994, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'FF', 27.0289, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'FF', 24.5575, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'FF', 13.6417, True),
        (metadata, datetime(2020, 1, 1, 22, 0), 'FF', 79.6259, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'FF', 2.6536, True)  #
    ]
    day_dd_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'DD', 358.3351, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'DD', 175.2109, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'DD', 138.8715, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'DD', 220.809, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'DD', 271.7507, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'DD', 291.5353, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'DD', 288.4084, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'DD', 311.3915, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'DD', 175.8598, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'DD', 209.0426, False),
        (metadata, datetime(2020, 1, 1, 10, 0), 'DD', 110.1688, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'DD', 136.3192, False),
        (metadata, datetime(2020, 1, 1, 12, 0), 'DD', 278.2642, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'DD', 40.0071, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'DD', 210.9397, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'DD', 11.0144, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'DD', 315.8591, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'DD', 159.6154, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'DD', 95.822, True),
        (metadata, datetime(2020, 1, 1, 19, 0), 'DD', 43.7687, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'DD', 11.644, True),
        (metadata, datetime(2020, 1, 1, 21, 0), 'DD', 266.7326, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'DD', 176.5122, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'DD', 175.8149, True)
    ]
    flag = compute.compute_wind_flag(day_ff_records, day_dd_records)
    assert flag == (8, 0)
    flag = compute.compute_wind_flag(day_ff_records, day_dd_records, at_least_perc=0.3)
    assert flag == (8, 1)


def test_compute_vntmxgg():
    metadata = sample_metadata.copy()
    day_ff_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'FF', 17.3247, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'FF', 68.9561, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'FF', 26.4429, True),  #
        (metadata, datetime(2020, 1, 1, 3, 0), 'FF', 99.828, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'FF', 7.7739, True),  #
        (metadata, datetime(2020, 1, 1, 5, 0), 'FF', 98.4608, True),  #
        (metadata, datetime(2020, 1, 1, 6, 0), 'FF', 59.3268, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'FF', 86.4781, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'FF', 20.4499, True),  #
        (metadata, datetime(2020, 1, 1, 9, 0), 'FF', 49.0313, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'FF', 23.5007, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'FF', 86.5212, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'FF', 98.4362, True),  #
        (metadata, datetime(2020, 1, 1, 13, 0), 'FF', 55.9767, True),  #
        (metadata, datetime(2020, 1, 1, 14, 0), 'FF', 38.2074, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'FF', 6.5295, True),  #
        (metadata, datetime(2020, 1, 1, 16, 0), 'FF', 88.8914, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'FF', 97.5881, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'FF', 24.5994, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'FF', 27.0289, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'FF', 24.5575, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'FF', 13.6417, True),
        (metadata, datetime(2020, 1, 1, 22, 0), 'FF', 79.6259, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'FF', 2.6536, True)  #
    ]
    day_dd_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'DD', 358.3351, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'DD', 175.2109, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'DD', 138.8715, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'DD', 220.809, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'DD', 271.7507, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'DD', 291.5353, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'DD', 288.4084, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'DD', 311.3915, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'DD', 175.8598, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'DD', 209.0426, False),
        (metadata, datetime(2020, 1, 1, 10, 0), 'DD', 110.1688, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'DD', 136.3192, False),
        (metadata, datetime(2020, 1, 1, 12, 0), 'DD', 278.2642, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'DD', 40.0071, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'DD', 210.9397, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'DD', 11.0144, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'DD', 315.8591, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'DD', 159.6154, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'DD', 95.822, True),
        (metadata, datetime(2020, 1, 1, 19, 0), 'DD', 43.7687, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'DD', 11.644, True),
        (metadata, datetime(2020, 1, 1, 21, 0), 'DD', 266.7326, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'DD', 176.5122, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'DD', 175.8149, True)
    ]
    flag, ff, dd = compute.compute_vntmxgg(day_ff_records, day_dd_records)
    assert (flag, ff, dd) == ((18, 1), 99.8, None)


def test_wind_ff_distribution():
    metadata = sample_metadata.copy()
    input_records = [
        (metadata, datetime(2020, 1, 1, 1, 0), 'FF', 68.9561, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'FF', 26.4429, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'FF', 99.828, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'FF', 7.7739, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'FF', 98.4608, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'FF', 59.3268, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'FF', 20.4499, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'FF', 49.0313, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'FF', 86.5212, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'FF', 98.4362, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'FF', 55.9767, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'FF', 38.2074, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'FF', 6.5295, True),
        (metadata, datetime(2020, 1, 1, 17, 0), 'FF', 97.5881, True),
        (metadata, datetime(2020, 1, 1, 19, 0), 'FF', 27.0289, True),
        (metadata, datetime(2020, 1, 1, 21, 0), 'FF', 13.6417, True),
        (metadata, datetime(2020, 1, 1, 22, 0), 'FF', 79.6259, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'FF', 2.6536, True),
    ]
    [c1, c2, c3, c4] = compute.wind_ff_distribution(input_records)
    assert [c1, c2, c3, c4] == [1, 0, 2, 15]


def test_wind_dd_partition():
    metadata = sample_metadata.copy()
    input_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'DD', 358.3351, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'DD', 138.8715, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'DD', 271.7507, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'DD', 291.5353, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'DD', 175.8598, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'DD', 110.1688, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'DD', 278.2642, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'DD', 40.0071, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'DD', 11.0144, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'DD', 95.822, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'DD', 11.644, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'DD', 175.8149, True)
    ]
    result = compute.wind_dd_partition(input_records)
    assert result == [
        [
            (metadata, datetime(2020, 1, 1, 15, 0), 'DD', 11.0144, True),
            (metadata, datetime(2020, 1, 1, 20, 0), 'DD', 11.644, True)],
        [
            (metadata, datetime(2020, 1, 1, 13, 0), 'DD', 40.0071, True)
        ],
        [],
        [],
        [
            (metadata, datetime(2020, 1, 1, 10, 0), 'DD', 110.1688, True),
            (metadata, datetime(2020, 1, 1, 18, 0), 'DD', 95.822, True)],
        [],
        [
            (metadata, datetime(2020, 1, 1, 2, 0), 'DD', 138.8715, True)
        ],
        [
            (metadata, datetime(2020, 1, 1, 8, 0), 'DD', 175.8598, True),
            (metadata, datetime(2020, 1, 1, 23, 0), 'DD', 175.8149, True)
        ],
        [],
        [],
        [],
        [],
        [
            (metadata, datetime(2020, 1, 1, 4, 0), 'DD', 271.7507, True),
            (metadata, datetime(2020, 1, 1, 5, 0), 'DD', 291.5353, True),
            (metadata, datetime(2020, 1, 1, 12, 0), 'DD', 278.2642, True)
        ],
        [],
        [],
        [
            (metadata, datetime(2020, 1, 1, 0, 0), 'DD', 358.3351, True)
        ]
    ]


def test_compute_vnt():
    metadata = sample_metadata.copy()
    day_ff_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'FF', 17.3247, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'FF', 0.2, True),  # -> calme
        (metadata, datetime(2020, 1, 1, 2, 0), 'FF', 26.4429, True),  #
        (metadata, datetime(2020, 1, 1, 3, 0), 'FF', 99.828, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'FF', 7.7739, True),  #
        (metadata, datetime(2020, 1, 1, 5, 0), 'FF', 98.4608, True),  #
        (metadata, datetime(2020, 1, 1, 6, 0), 'FF', 59.3268, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'FF', 86.4781, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'FF', 20.4499, True),  #
        (metadata, datetime(2020, 1, 1, 9, 0), 'FF', 49.0313, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'FF', 23.5007, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'FF', 86.5212, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'FF', 98.4362, True),  #
        (metadata, datetime(2020, 1, 1, 13, 0), 'FF', 55.9767, True),  #
        (metadata, datetime(2020, 1, 1, 14, 0), 'FF', 38.2074, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'FF', 6.5295, True),  #
        (metadata, datetime(2020, 1, 1, 16, 0), 'FF', 88.8914, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'FF', 97.5881, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'FF', 24.5994, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'FF', 27.0289, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'FF', 24.5575, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'FF', 13.6417, True),
        (metadata, datetime(2020, 1, 1, 22, 0), 'FF', 79.6259, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'FF', 2.6536, True)  #
    ]
    day_dd_records = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'DD', 358.3351, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'DD', 175.2109, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'DD', 138.8715, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'DD', 220.809, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'DD', 271.7507, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'DD', 291.5353, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'DD', 288.4084, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'DD', 311.3915, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'DD', 175.8598, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'DD', 209.0426, False),
        (metadata, datetime(2020, 1, 1, 10, 0), 'DD', 110.1688, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'DD', 136.3192, False),
        (metadata, datetime(2020, 1, 1, 12, 0), 'DD', 278.2642, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'DD', 40.0071, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'DD', 210.9397, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'DD', 11.0144, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'DD', 315.8591, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'DD', 159.6154, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'DD', 95.822, True),
        (metadata, datetime(2020, 1, 1, 19, 0), 'DD', 43.7687, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'DD', 11.644, True),
        (metadata, datetime(2020, 1, 1, 21, 0), 'DD', 266.7326, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'DD', 176.5122, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'DD', 175.8149, True)
    ]
    # ff = [
    #     (metadata, datetime(2020, 1, 1, 2, 0), 'FF', 26.4429, True),  #
    #     (metadata, datetime(2020, 1, 1, 4, 0), 'FF', 7.7739, True),  #
    #     (metadata, datetime(2020, 1, 1, 5, 0), 'FF', 98.4608, True),  #
    #     (metadata, datetime(2020, 1, 1, 8, 0), 'FF', 20.4499, True),  #
    #     (metadata, datetime(2020, 1, 1, 12, 0), 'FF', 98.4362, True),  #
    #     (metadata, datetime(2020, 1, 1, 13, 0), 'FF', 55.9767, True),  #
    #     (metadata, datetime(2020, 1, 1, 15, 0), 'FF', 6.5295, True),  #
    #     (metadata, datetime(2020, 1, 1, 23, 0), 'FF', 2.6536, True)  #
    # ]
    # dd = [
    #     (metadata, datetime(2020, 1, 1, 2, 0), 'DD', 138.8715, True),
    #     (metadata, datetime(2020, 1, 1, 4, 0), 'DD', 271.7507, True),
    #     (metadata, datetime(2020, 1, 1, 5, 0), 'DD', 291.5353, True),
    #     (metadata, datetime(2020, 1, 1, 8, 0), 'DD', 175.8598, True),
    #     (metadata, datetime(2020, 1, 1, 12, 0), 'DD', 278.2642, True),
    #     (metadata, datetime(2020, 1, 1, 13, 0), 'DD', 40.0071, True),
    #     (metadata, datetime(2020, 1, 1, 15, 0), 'DD', 11.0144, True),
    #     (metadata, datetime(2020, 1, 1, 23, 0), 'DD', 175.8149, True)
    # ]
    result = compute.compute_vnt(day_ff_records, day_dd_records)
    assert len(result) == 66
    flag, frq_calme = result[:2]
    frq_sicj = result[2:]
    assert flag == (18, 1)
    assert frq_calme == 1
    assert frq_sicj == [
        0, 0, 1, 0,
        0, 0, 0, 1,
        0, 0, 0, 0,
        0, 0, 0, 0,
        0, 0, 0, 0,
        0, 0, 0, 0,
        0, 0, 0, 1,
        1, 0, 0, 1,
        0, 0, 0, 0,
        0, 0, 0, 0,
        0, 0, 0, 0,
        0, 0, 0, 0,
        0, 0, 1, 2,
        0, 0, 0, 0,
        0, 0, 0, 0,
        0, 0, 0, 0
    ]


def test_compute_day_indicators():
    metadata = sample_metadata.copy()
    measures = [
        (metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 0, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'PREC', 1, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'PREC', 2, False),
        (metadata, datetime(2020, 1, 1, 3, 0), 'PREC', 3, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'PREC', 4, False),
        (metadata, datetime(2020, 1, 1, 5, 0), 'PREC', 5, False),
        (metadata, datetime(2020, 1, 1, 6, 0), 'PREC', 6, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'PREC', 7, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'PREC', 8, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'PREC', 9, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'PREC', 10, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'PREC', 11, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'PREC', 12, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'PREC', 13, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'PREC', 14, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'PREC', 15, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'PREC', 16, True),
        (metadata, datetime(2020, 1, 1, 17, 0), 'PREC', 17, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'PREC', 18, True),
        (metadata, datetime(2020, 1, 1, 19, 0), 'PREC', 19, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'PREC', 20, True),
        (metadata, datetime(2020, 1, 1, 21, 0), 'PREC', 21, True),
        (metadata, datetime(2020, 1, 1, 22, 0), 'PREC', 22, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'PREC', 23, True),
        (metadata, datetime(2020, 1, 1, 0, 0), 'Bagnatura_f', 46.6998, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'Bagnatura_f', 19.3586, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'Bagnatura_f', 32.8168, False),
        (metadata, datetime(2020, 1, 1, 3, 0), 'Bagnatura_f', 49.6327, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'Bagnatura_f', 5.8928, False),
        (metadata, datetime(2020, 1, 1, 5, 0), 'Bagnatura_f', 1.8212, False),
        (metadata, datetime(2020, 1, 1, 6, 0), 'Bagnatura_f', 22.0986, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'Bagnatura_f', 45.9589, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'Bagnatura_f', 26.5065, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'Bagnatura_f', 20.6426, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'Bagnatura_f', 54.9259, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'Bagnatura_f', 6.737, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'Bagnatura_f', 45.4702, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'Bagnatura_f', 42.287, False),
        (metadata, datetime(2020, 1, 1, 14, 0), 'Bagnatura_f', 4.2066, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'Bagnatura_f', 21.8372, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'Bagnatura_f', 20.4227, True),
        (metadata, datetime(2020, 1, 1, 17, 0), 'Bagnatura_f', 22.0616, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'Bagnatura_f', 48.8321, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'Bagnatura_f', 32.1013, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'Bagnatura_f', 45.4906, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'Bagnatura_f', 4.2239, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'Bagnatura_f', 57.1591, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'Bagnatura_f', 11.9748, False),
        (metadata, datetime(2020, 1, 1, 0, 0), 'FF', 17.3247, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'FF', 0.2, True),  # -> calme
        (metadata, datetime(2020, 1, 1, 2, 0), 'FF', 26.4429, True),  #
        (metadata, datetime(2020, 1, 1, 3, 0), 'FF', 99.828, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'FF', 7.7739, True),  #
        (metadata, datetime(2020, 1, 1, 5, 0), 'FF', 98.4608, True),  #
        (metadata, datetime(2020, 1, 1, 6, 0), 'FF', 59.3268, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'FF', 86.4781, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'FF', 20.4499, True),  #
        (metadata, datetime(2020, 1, 1, 9, 0), 'FF', 49.0313, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'FF', 23.5007, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'FF', 86.5212, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'FF', 98.4362, True),  #
        (metadata, datetime(2020, 1, 1, 13, 0), 'FF', 55.9767, True),  #
        (metadata, datetime(2020, 1, 1, 14, 0), 'FF', 38.2074, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'FF', 6.5295, True),  #
        (metadata, datetime(2020, 1, 1, 16, 0), 'FF', 88.8914, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'FF', 97.5881, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'FF', 24.5994, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'FF', 27.0289, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'FF', 24.5575, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'FF', 13.6417, True),
        (metadata, datetime(2020, 1, 1, 22, 0), 'FF', 79.6259, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'FF', 2.6536, True),  #
        (metadata, datetime(2020, 1, 1, 0, 0), 'DD', 358.3351, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'DD', 175.2109, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'DD', 138.8715, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'DD', 220.809, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'DD', 271.7507, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'DD', 291.5353, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'DD', 288.4084, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'DD', 311.3915, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'DD', 175.8598, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'DD', 209.0426, False),
        (metadata, datetime(2020, 1, 1, 10, 0), 'DD', 110.1688, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'DD', 136.3192, False),
        (metadata, datetime(2020, 1, 1, 12, 0), 'DD', 278.2642, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'DD', 40.0071, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'DD', 210.9397, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'DD', 11.0144, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'DD', 315.8591, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'DD', 159.6154, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'DD', 95.822, True),
        (metadata, datetime(2020, 1, 1, 19, 0), 'DD', 43.7687, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'DD', 11.644, True),
        (metadata, datetime(2020, 1, 1, 21, 0), 'DD', 266.7326, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'DD', 176.5122, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'DD', 175.8149, True),
        (metadata, datetime(2020, 1, 1, 0, 0), 'UR min', 93.4062, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'UR min', 57.377, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'UR min', 63.7623, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'UR min', 65.5366, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'UR min', 6.0608, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'UR min', 7.9139, False),
        (metadata, datetime(2020, 1, 1, 6, 0), 'UR min', 41.3705, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'UR min', 94.9941, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'UR min', 98.9935, False),
        (metadata, datetime(2020, 1, 1, 9, 0), 'UR min', 6.9763, False),
        (metadata, datetime(2020, 1, 1, 10, 0), 'UR min', 83.4629, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'UR min', 0.0951, False),
        (metadata, datetime(2020, 1, 1, 12, 0), 'UR min', 83.8421, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'UR min', 28.7116, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'UR min', 38.8209, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'UR min', 98.0396, False),
        (metadata, datetime(2020, 1, 1, 16, 0), 'UR min', 74.3587, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'UR min', 36.3907, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'UR min', 91.5099, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'UR min', 26.6897, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'UR min', 74.6414, True),
        (metadata, datetime(2020, 1, 1, 21, 0), 'UR min', 86.2716, True),
        (metadata, datetime(2020, 1, 1, 22, 0), 'UR min', 20.7062, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'UR min', 54.3221, True),
        (metadata, datetime(2020, 1, 1, 0, 0), 'UR max', 1.1287, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'UR max', 84.5, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'UR max', 7.051, False),
        (metadata, datetime(2020, 1, 1, 3, 0), 'UR max', 86.2606, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'UR max', 19.5809, False),
        (metadata, datetime(2020, 1, 1, 5, 0), 'UR max', 82.32, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'UR max', 56.0317, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'UR max', 73.2838, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'UR max', 76.6591, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'UR max', 84.9542, False),
        (metadata, datetime(2020, 1, 1, 10, 0), 'UR max', 80.976, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'UR max', 2.5568, False),
        (metadata, datetime(2020, 1, 1, 12, 0), 'UR max', 24.7245, False),
        (metadata, datetime(2020, 1, 1, 13, 0), 'UR max', 2.1739, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'UR max', 19.1577, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'UR max', 11.1011, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'UR max', 90.4718, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'UR max', 67.5488, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'UR max', 97.9166, True),
        (metadata, datetime(2020, 1, 1, 19, 0), 'UR max', 25.1655, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'UR max', 54.2473, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'UR max', 80.8128, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'UR max', 71.5309, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'UR max', 23.1089, True),
        (metadata, datetime(2020, 1, 1, 0, 0), 'UR media', 93.0725, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'UR media', 14.0488, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'UR media', 19.5679, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'UR media', 98.4374, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'UR media', 60.5525, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'UR media', 5.3557, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'UR media', 19.0187, True),
        (metadata, datetime(2020, 1, 1, 7, 0), 'UR media', 40.0136, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'UR media', 81.0316, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'UR media', 38.7842, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'UR media', 42.5653, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'UR media', 78.6204, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'UR media', 32.623, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'UR media', 8.1728, False),
        (metadata, datetime(2020, 1, 1, 14, 0), 'UR media', 46.7942, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'UR media', 19.52, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'UR media', 73.0788, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'UR media', 69.9987, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'UR media', 44.1692, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'UR media', 0.7595, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'UR media', 13.4624, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'UR media', 72.7669, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'UR media', 4.1295, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'UR media', 48.7278, False),
        (metadata, datetime(2020, 1, 1, 0, 0), 'RADSOL', 29.503, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'RADSOL', 80.9462, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'RADSOL', 71.3331, False),
        (metadata, datetime(2020, 1, 1, 3, 0), 'RADSOL', 28.2172, False),
        (metadata, datetime(2020, 1, 1, 4, 0), 'RADSOL', 9.2708, False),
        (metadata, datetime(2020, 1, 1, 5, 0), 'RADSOL', 20.3447, True),
        (metadata, datetime(2020, 1, 1, 6, 0), 'RADSOL', 3.8309, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'RADSOL', 26.1966, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'RADSOL', 58.7787, False),
        (metadata, datetime(2020, 1, 1, 9, 0), 'RADSOL', 85.7274, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'RADSOL', 66.2083, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'RADSOL', 65.1714, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'RADSOL', 29.8974, False),
        (metadata, datetime(2020, 1, 1, 13, 0), 'RADSOL', 41.012, False),
        (metadata, datetime(2020, 1, 1, 14, 0), 'RADSOL', 34.1688, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'RADSOL', 83.0679, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'RADSOL', 6.9506, True),
        (metadata, datetime(2020, 1, 1, 17, 0), 'RADSOL', 74.8353, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'RADSOL', 9.9903, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'RADSOL', 30.9549, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'RADSOL', 75.6266, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'RADSOL', 47.9269, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'RADSOL', 22.7512, True),
        (metadata, datetime(2020, 1, 1, 23, 0), 'RADSOL', 11.4268, False),
        (metadata, datetime(2020, 1, 1, 0, 0), 'INSOL', 12.3156, False),
        (metadata, datetime(2020, 1, 1, 1, 0), 'INSOL', 53.384, False),
        (metadata, datetime(2020, 1, 1, 2, 0), 'INSOL', 8.9947, True),
        (metadata, datetime(2020, 1, 1, 3, 0), 'INSOL', 17.9736, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'INSOL', 41.2483, True),
        (metadata, datetime(2020, 1, 1, 5, 0), 'INSOL', 12.0817, False),
        (metadata, datetime(2020, 1, 1, 6, 0), 'INSOL', 44.4205, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'INSOL', 49.4548, True),
        (metadata, datetime(2020, 1, 1, 8, 0), 'INSOL', 33.5316, False),
        (metadata, datetime(2020, 1, 1, 9, 0), 'INSOL', 4.5963, False),
        (metadata, datetime(2020, 1, 1, 10, 0), 'INSOL', 23.815, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'INSOL', 18.4742, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'INSOL', 25.068, False),
        (metadata, datetime(2020, 1, 1, 13, 0), 'INSOL', 50.023, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'INSOL', 7.0285, False),
        (metadata, datetime(2020, 1, 1, 15, 0), 'INSOL', 29.6455, False),
        (metadata, datetime(2020, 1, 1, 16, 0), 'INSOL', 3.0326, False),
        (metadata, datetime(2020, 1, 1, 17, 0), 'INSOL', 41.8781, True),
        (metadata, datetime(2020, 1, 1, 18, 0), 'INSOL', 54.6086, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'INSOL', 53.9748, False),
        (metadata, datetime(2020, 1, 1, 20, 0), 'INSOL', 13.0228, True),
        (metadata, datetime(2020, 1, 1, 21, 0), 'INSOL', 12.48, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'INSOL', 31.0874, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'INSOL', 35.6489, False),
        (metadata, datetime(2020, 1, 1, 9, 0), 'Tmedia', 0, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'Tmedia', 1, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'Tmedia', 2, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'Tmedia', 3, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'Tmedia', 4, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'Tmedia', 5, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'Tmax', 0, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'Tmax', 2, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'Tmax', 3, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'Tmax', 4, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'Tmax', 5, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'Tmax', 6, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'Tmin', 0, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'Tmin', -1, True),
        (metadata, datetime(2020, 1, 1, 11, 0), 'Tmin', -2, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'Tmin', -3, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'Tmin', -4, True),
        (metadata, datetime(2020, 1, 1, 14, 0), 'Tmin', -5, True),
        (metadata, datetime(2020, 1, 1, 0, 0), 'P', 46.6998, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'P', 19.3586, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'P', 32.8168, False),
        (metadata, datetime(2020, 1, 1, 3, 0), 'P', 49.6327, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'P', 5.8928, False),
        (metadata, datetime(2020, 1, 1, 5, 0), 'P', 1.8212, False),
        (metadata, datetime(2020, 1, 1, 6, 0), 'P', 22.0986, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'P', 45.9589, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'P', 26.5065, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'P', 20.6426, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'P', 54.9259, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'P', 6.737, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'P', 45.4702, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'P', 42.287, False),
        (metadata, datetime(2020, 1, 1, 14, 0), 'P', 4.2066, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'P', 21.8372, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'P', 20.4227, True),
        (metadata, datetime(2020, 1, 1, 17, 0), 'P', 22.0616, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'P', 48.8321, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'P', 32.1013, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'P', 45.4906, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'P', 4.2239, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'P', 57.1591, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'P', 11.9748, False),
        (metadata, datetime(2020, 1, 1, 0, 0), 'Pmax', 46.6998, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'Pmax', 19.3586, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'Pmax', 32.8168, False),
        (metadata, datetime(2020, 1, 1, 3, 0), 'Pmax', 49.6327, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'Pmax', 5.8928, False),
        (metadata, datetime(2020, 1, 1, 5, 0), 'Pmax', 1.8212, False),
        (metadata, datetime(2020, 1, 1, 6, 0), 'Pmax', 22.0986, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'Pmax', 45.9589, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'Pmax', 26.5065, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'Pmax', 20.6426, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'Pmax', 54.9259, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'Pmax', 6.737, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'Pmax', 45.4702, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'Pmax', 42.287, False),
        (metadata, datetime(2020, 1, 1, 14, 0), 'Pmax', 4.2066, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'Pmax', 21.8372, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'Pmax', 20.4227, True),
        (metadata, datetime(2020, 1, 1, 17, 0), 'Pmax', 22.0616, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'Pmax', 48.8321, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'Pmax', 32.1013, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'Pmax', 45.4906, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'Pmax', 4.2239, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'Pmax', 57.1591, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'Pmax', 11.9748, False),
        (metadata, datetime(2020, 1, 1, 0, 0), 'Pmin', 46.6998, True),
        (metadata, datetime(2020, 1, 1, 1, 0), 'Pmin', 19.3586, True),
        (metadata, datetime(2020, 1, 1, 2, 0), 'Pmin', 32.8168, False),
        (metadata, datetime(2020, 1, 1, 3, 0), 'Pmin', 49.6327, True),
        (metadata, datetime(2020, 1, 1, 4, 0), 'Pmin', 5.8928, False),
        (metadata, datetime(2020, 1, 1, 5, 0), 'Pmin', 1.8212, False),
        (metadata, datetime(2020, 1, 1, 6, 0), 'Pmin', 22.0986, False),
        (metadata, datetime(2020, 1, 1, 7, 0), 'Pmin', 45.9589, False),
        (metadata, datetime(2020, 1, 1, 8, 0), 'Pmin', 26.5065, True),
        (metadata, datetime(2020, 1, 1, 9, 0), 'Pmin', 20.6426, True),
        (metadata, datetime(2020, 1, 1, 10, 0), 'Pmin', 54.9259, False),
        (metadata, datetime(2020, 1, 1, 11, 0), 'Pmin', 6.737, True),
        (metadata, datetime(2020, 1, 1, 12, 0), 'Pmin', 45.4702, True),
        (metadata, datetime(2020, 1, 1, 13, 0), 'Pmin', 42.287, False),
        (metadata, datetime(2020, 1, 1, 14, 0), 'Pmin', 4.2066, True),
        (metadata, datetime(2020, 1, 1, 15, 0), 'Pmin', 21.8372, True),
        (metadata, datetime(2020, 1, 1, 16, 0), 'Pmin', 20.4227, True),
        (metadata, datetime(2020, 1, 1, 17, 0), 'Pmin', 22.0616, False),
        (metadata, datetime(2020, 1, 1, 18, 0), 'Pmin', 48.8321, False),
        (metadata, datetime(2020, 1, 1, 19, 0), 'Pmin', 32.1013, True),
        (metadata, datetime(2020, 1, 1, 20, 0), 'Pmin', 45.4906, False),
        (metadata, datetime(2020, 1, 1, 21, 0), 'Pmin', 4.2239, False),
        (metadata, datetime(2020, 1, 1, 22, 0), 'Pmin', 57.1591, False),
        (metadata, datetime(2020, 1, 1, 23, 0), 'Pmin', 11.9748, False),
    ]
    res = compute.compute_day_indicators(measures)
    assert res == {
        'ds__bagna': {'bagna': ((11, 0), 4.9, 0.3, 0.8, 0.1, 0.4)},
        'ds__elio': {'elio': ((9, 0), 4.4, 0.3, None)},
        'ds__preci': {
            'cl_prec06': (0, 0, 0, 0, 0, 3),
            'cl_prec12': (0, 0, 0, 0, 0, 2),
            'cl_prec24': (0, 0, 5, 10, 3, 0),
            'prec01': ((18, 0), 23, '2020-01-01T23:00:00'),
            'prec06': ((18, 0), 123, '2020-01-01T18:00:00'),
            'prec12': ((18, 0), 210, '2020-01-01T12:00:00'),
            'prec24': ((18, 0), 261, 23, '2020-01-01T23:00:00')
        },
        'ds__radglob': {'radglob': ((9, 0), 21.8, 14.4, 41.5, 3.4)},
        'ds__urel': {'ur': ((11, 0), 39.8, 24.8, (None, None), 97.9, 6.1)},
        'ds__vnt10': {
            'vnt': [(18, 1), 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            'vntmd': ((18, 1), 48.2),
            'vntmxgg': ((18, 1), 99.8, None),
        },
        'ds__t200': {
            'tmdgg': ((6, 0), 2.5, 1.9),
            'tmngg': ((6, 0), -5, 1.9, -2.5, '2020-01-01T14:00:00'),
            'tmxgg': ((6, 0), 6, 2.2, 3.3, '2020-01-01T14:00:00')
        },
        'ds__press': {'press': ((11, 0), 26.7, 15.4, 49.6, 4.2)},
    }


def test_compute_etp():
    etp = compute.compute_etp(None, 11.7, 5.3, 40.785333, 14)
    assert not etp
    etp = compute.compute_etp(8.3, 11.7, 5.3, 40.785333, 14)
    assert etp[0] == (None, 1)
    assert etp[1] == 0.9
    etp = compute.compute_etp(7.2, 13.7, 2, 40.785333, 16)
    assert etp[0] == (None, 1)
    assert etp[1] == 1.2


def test_compute_and_store(tmpdir):
    # TODO
    #table_map = compute.INDICATORS_TABLES.copy()
    pass
    #compute.compute_and_store(data, writers, table_map)