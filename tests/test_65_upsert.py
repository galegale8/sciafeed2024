
from datetime import datetime
from decimal import Decimal

from sciafeed import querying
from sciafeed import upsert


def test_update_prec_flags(conn):
    records = list(querying.select_prec_records(
        conn, sql_fields='cod_staz, data_i, (prec24).val_tot, ((prec24).flag).wht',
        stations_ids=[5800, 5700, 5600], schema='test', flag_threshold=1))
    some_existing_records = [
        [5800, datetime(1992, 12, 1, 0, 0), Decimal('0'), 1],
        [5700, datetime(1992, 12, 1, 0, 0), Decimal('0'), 1],
        [5600, datetime(1992, 12, 1, 0, 0), Decimal('0'), 1]
    ]
    for test_record in some_existing_records:
        assert test_record in records, 'precondition for test on record is not met'

    with_flags_changed = [
        [5800, datetime(1992, 12, 1, 0, 0), Decimal('0'), -1],
        [5700, datetime(1992, 12, 1, 0, 0), Decimal('0'), -2],
        [5600, datetime(1992, 12, 1, 0, 0), Decimal('0'), -3]
    ]
    num_changed = upsert.update_prec_flags(conn, with_flags_changed, schema='test')
    assert num_changed == 3
    records = list(querying.select_prec_records(
        conn, sql_fields='cod_staz, data_i, (prec24).val_tot, ((prec24).flag).wht',
        stations_ids=[5800, 5700, 5600], schema='test', flag_threshold=None))
    for test_record in some_existing_records:
        assert test_record not in records
    for new_record in with_flags_changed:
        assert new_record in records


def test_set_temp_flags(conn):
    records = list(querying.select_temp_records(
        conn, fields=['tmxgg'], sql_fields="cod_staz, data_i, (tmxgg).val_md, ((tmxgg).flag).wht",
        stations_ids=[5800, 5700, 5600], schema='test', flag_threshold=1))
    some_existing_records = [
        [5800, datetime(1992, 12, 1, 0, 0), Decimal('4.5'), 1],
        [5700, datetime(1992, 1, 1, 0, 0), Decimal('6.8'), 1],
        [5600, datetime(1990, 1, 1, 0, 0), Decimal('-3.1'), 1]
    ]
    for test_record in some_existing_records:
        assert test_record in records, 'precondition for test on record is not met'

    with_flags_changed = [
        [5800, datetime(1992, 12, 1, 0, 0), Decimal('4.5'), -1],
        [5700, datetime(1992, 1, 1, 0, 0), Decimal('6.8'), -2],
        [5600, datetime(1990, 1, 1, 0, 0), Decimal('-3.1'), -3]
    ]
    num_changed = upsert.update_temp_flags(conn, with_flags_changed, schema='test',
                                           db_field='tmxgg')
    assert num_changed == 3
    records = list(querying.select_temp_records(
        conn, fields=['tmxgg'], sql_fields="cod_staz, data_i, (tmxgg).val_md, ((tmxgg).flag).wht",
        stations_ids=[5800, 5700, 5600], schema='test', flag_threshold=None))
    for test_record in some_existing_records:
        assert test_record not in records
    for new_record in with_flags_changed:
        assert new_record in records
