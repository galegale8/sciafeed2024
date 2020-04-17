
from datetime import datetime, timedelta

from sciafeed import compute

from pprint import pprint

sample_metadata = {'a metadata': 'a value'}


def create_samples(par_code, hour_step=1):
    metadata = {'a metadata': 'a value'}
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
    assert (flag, val_tot, val_mx, data_mx) == ((24, 1), 276, 23, datetime(2020, 1, 1, 23, 0))

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
    assert (flag, val_tot, val_mx, data_mx) == ((18, 1), 261, 23, datetime(2020, 1, 1, 23, 0))

    # partially bad (higher percentage)
    flag, val_tot, val_mx, data_mx = compute.compute_prec24(day_records, at_least_perc=0.80)
    assert (flag, val_tot, val_mx, data_mx) == ((18, 0), 261, 23, datetime(2020, 1, 1, 23, 0))

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
    assert (flag, val_tot, val_mx, data_mx) == ((6, 0), 15, 5, datetime(2020, 1, 1, 5, 0))

    # empty
    day_records = []
    flag, val_tot, val_mx, data_mx = compute.compute_prec24(day_records, at_least_perc=0.80)
    assert (flag, val_tot, val_mx, data_mx) == ((0, 0), None, None, None)


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
    assert (flag, val_mx, data_mx) == ((24, 1), 23, datetime(2020, 1, 1, 23, 0))

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
    assert (flag, val_mx, data_mx) == ((6, 0), 5, datetime(2020, 1, 1, 5, 0))
    # with low tolerance
    flag, val_mx, data_mx = compute.compute_prec01(day_records, at_least_perc=0.10)
    assert (flag, val_mx, data_mx) == ((6, 1), 5, datetime(2020, 1, 1, 5, 0))


def test_compute_prec06():
    day_records = create_samples('PREC')
    flag, val_mx, data_mx = compute.compute_prec06(day_records, at_least_perc=0.75)
    assert (flag, val_mx, data_mx) == ((24, 1), 123, datetime(2020, 1, 1, 18, 0))


def test_compute_cl_prec06():
    day_records = create_samples('PREC')
    dry, wet_01, wet_02, wet_03, wet_04, wet_05 = compute.compute_cl_prec06(day_records)
    assert (dry, wet_01, wet_02, wet_03, wet_04, wet_05) == (0, 0, 0, 1, 0, 3)


def test_compute_prec12():
    day_records = create_samples('PREC')
    flag, val_mx, data_mx = compute.compute_prec12(day_records, at_least_perc=0.75)
    assert (flag, val_mx, data_mx) == ((24, 1), 210, datetime(2020, 1, 1, 12, 0))


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
        (sample_metadata, datetime(2020, 1, 1, 0, 0), 'PREC', 0, True),
        (sample_metadata, datetime(2020, 1, 1, 1, 0), 'PREC', 1, True),
        (sample_metadata, datetime(2020, 1, 1, 2, 0), 'PREC', 2, True),
        (sample_metadata, datetime(2020, 1, 1, 3, 0), 'PREC', 3, True),
        (sample_metadata, datetime(2020, 1, 1, 4, 0), 'PREC', 4, True),
        (sample_metadata, datetime(2020, 1, 1, 5, 0), 'PREC', 5, True),
    ]
    assert compute.compute_temperature_flag(input_records1) == (6, 0)
    # all at daylight
    input_records2 = [
        (sample_metadata, datetime(2020, 1, 1, 9, 0), 'PREC', 0, True),
        (sample_metadata, datetime(2020, 1, 1, 10, 0), 'PREC', 1, True),
        (sample_metadata, datetime(2020, 1, 1, 11, 0), 'PREC', 2, True),
        (sample_metadata, datetime(2020, 1, 1, 12, 0), 'PREC', 3, True),
        (sample_metadata, datetime(2020, 1, 1, 13, 0), 'PREC', 4, True),
        (sample_metadata, datetime(2020, 1, 1, 14, 0), 'PREC', 5, True),
    ]
    assert compute.compute_temperature_flag(input_records2) == (6, 0)
    # not enought for day and night
    input_records3 = input_records1 + input_records2
    assert compute.compute_temperature_flag(input_records3) == (12, 0)


def test_compute_tmdgg():
    input_records = create_samples('Tmedia')
    flag, val_md, val_vr = compute.compute_tmdgg(input_records, at_least_perc=0.75)
    assert (flag, val_md, val_vr) == ((24, 1), 11.5, 7.0711)


def test_compute_tmxgg():
    input_records = create_samples('Tmax')
    flag, val_md, val_vr, val_x, data_x = compute.compute_tmxgg(input_records, at_least_perc=0.75)
    assert (flag, val_md, val_vr, val_x, data_x) == ((24, 1), 11.5, 7.0711, 23,
                                                     datetime(2020, 1, 1, 23, 0))


def test_compute_tmngg():
    input_records = create_samples('Tmin')
    flag, val_md, val_vr, val_x, data_x = compute.compute_tmngg(input_records, at_least_perc=0.75)
    assert (flag, val_md, val_vr, val_x, data_x) == ((24, 1), 11.5, 7.0711, 0,
                                                     datetime(2020, 1, 1, 0, 0))


def test_compute_press():
    day_records_pmedia = create_samples('Pmedia')
    day_records_pmax = create_samples('Pmax')
    day_records_pmin = create_samples('Pmin')

    flag, val_md, val_vr, val_mx, val_mn = compute.compute_press(
        day_records_pmedia, day_records_pmax, day_records_pmin)
    assert (flag, val_md, val_vr, val_mx, val_mn) == ((24, 1), 11.5, 7.0711, 23, 0)


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
        (metadata, datetime(2020, 1, 1, 23, 0), 'Bagnatura_f', 11.9748, False)
    ]
    flag, val_md, val_vr, val_mx, val_mn, val_tot = compute.compute_bagna(day_records)
    assert (flag, val_md, val_vr, val_mx, val_mn, val_tot) == \
           ((11, 0), 0.4449, 0.2563, 0.8272, 0.0701, 4.8936)


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
    assert (flag, val_md, val_vr, val_mx) == ((9, 0), 4.4147, 0.2693, None)


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
    assert (flag, val_md, val_vr, val_mx, val_mn) == ((9, 0), 21.8431, 14.4367, 41.5178, 3.3662)

