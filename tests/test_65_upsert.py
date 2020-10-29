
from datetime import datetime
from decimal import Decimal
from os.path import join

from sciafeed import export
from sciafeed import querying
from sciafeed import upsert

import pytest

from . import TEST_DATA_PATH

# ignore some useless SAWarnings in this module
pytestmark = pytest.mark.filterwarnings("ignore:.*Did not recognize type.*::sqlalchemy[.*]")


def test_update_prec_flags(conn):
    records = list(querying.select_prec_records(
        conn, sql_fields='cod_staz, data_i, (prec24).val_tot, ((prec24).flag).wht',
        stations_ids=[5800, 5700, 5600], schema='test', include_flag_values=(1, 5)))
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
        stations_ids=[5800, 5700, 5600], schema='test'))
    for test_record in some_existing_records:
        assert test_record not in records
    for new_record in with_flags_changed:
        assert new_record in records


def test_update_temp_flags(conn):
    records = list(querying.select_temp_records(
        conn, fields=['tmxgg'], sql_fields="cod_staz, data_i, (tmxgg).val_md, ((tmxgg).flag).wht",
        stations_ids=[5800, 5700, 5600], schema='test', include_flag_values=(1, 5)))
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
        stations_ids=[5800, 5700, 5600], schema='test'))
    for test_record in some_existing_records:
        assert test_record not in records
    for new_record in with_flags_changed:
        assert new_record in records


def test_expand_record():
    # not empty fields
    record = {
        'data_i': '2018-01-01 00:00:00', 'cod_staz': 12502, 'cod_aggr': '4',
        'tmxgg': "((24, 1), 11.5, 1.5, 9.7, '2018-01-01T11:00:00')",
        'tmngg': "((24, 1), 4.7, 1.9, 8.8, '2018-01-01T20:00:00')",
        'tmdgg': '((24, 1), 9.3, 1.6)'
    }
    new_record = upsert.expand_record(record)
    assert new_record == {
        'data_i': '2018-01-01 00:00:00', 'cod_staz': 12502, 'cod_aggr': '4',
        'tmxgg.flag.ndati': '24', 'tmxgg.flag.wht': '1', 'tmxgg.val_md': '11.5',
        'tmxgg.val_vr': '1.5', 'tmxgg.val_x': '9.7', 'tmxgg.data_x': '2018-01-01T11:00:00',
        'tmngg.flag.ndati': '24', 'tmngg.flag.wht': '1', 'tmngg.val_md': '4.7',
        'tmngg.val_vr': '1.9', 'tmngg.val_x': '8.8', 'tmngg.data_x': '2018-01-01T20:00:00',
        'tmdgg.flag.ndati': '24', 'tmdgg.flag.wht': '1', 'tmdgg.val_md': '9.3',
        'tmdgg.val_vr': '1.6'}
    # with some fields empty
    record = {
        'data_i': '2018-01-01 00:00:00', 'cod_staz': 12502, 'cod_aggr': '4',
        'tmxgg': "((24, 1),, 1.5, None, '2018-01-01T11:00:00')",
        'tmngg': "((24, 1), 4.7, 1.9, 8.8, '2018-01-01T20:00:00')",
        'tmdgg': '((24, 1), 9.3,)'
    }
    new_record = upsert.expand_record(record)
    assert new_record == {
        'data_i': '2018-01-01 00:00:00', 'cod_staz': 12502, 'cod_aggr': '4',
        'tmxgg.flag.ndati': '24', 'tmxgg.flag.wht': '1', 'tmxgg.val_md': 'NULL',
        'tmxgg.val_vr': '1.5', 'tmxgg.val_x': 'NULL', 'tmxgg.data_x': '2018-01-01T11:00:00',
        'tmngg.flag.ndati': '24', 'tmngg.flag.wht': '1', 'tmngg.val_md': '4.7',
        'tmngg.val_vr': '1.9', 'tmngg.val_x': '8.8', 'tmngg.data_x': '2018-01-01T20:00:00',
        'tmdgg.flag.ndati': '24', 'tmdgg.flag.wht': '1', 'tmdgg.val_md': '9.3',
        'tmdgg.val_vr': 'NULL'}


def test_expand_fields():
    fields = ['data_i', 'tmxgg', 'bagna', 'cod_staz']
    res = upsert.expand_fields(fields)
    assert res == [
        'data_i', 'tmxgg.flag.ndati', 'tmxgg.flag.wht', 'tmxgg.val_md',
        'tmxgg.val_vr', 'tmxgg.val_x', 'tmxgg.data_x', 'bagna.flag.ndati',
        'bagna.flag.wht', 'bagna.val_md', 'bagna.val_vr', 'bagna.val_mx',
        'bagna.val_tot', 'cod_staz'
    ]


def test_create_insert():
    data = []
    fields = ['col1', 'col2', 'col3']
    sql = upsert.create_insert('atable', 'aschema', fields, data)
    assert not sql

    data = [{'col1': 1, 'col2': '2'}, {'col3': 3}]
    fields = []
    sql = upsert.create_insert('atable', 'aschema', fields, data)
    assert not sql

    fields = ['col1', 'col2', 'col3']
    sql = upsert.create_insert('atable', 'aschema', fields, data)
    assert sql == "INSERT INTO aschema.atable (col1,col2,col3) " \
                  "VALUES ('1','2',NULL),(NULL,NULL,'3')"


def test_create_upsert():
    for policy in ['onlyinsert', 'upsert']:
        data = []
        fields = ['col1', 'col2', 'col3']
        sql = upsert.create_upsert('atable', 'aschema', fields, data, policy)
        assert not sql

        data = [{'col1': 1, 'col2': '2'}, {'col3': 3}]
        fields = []
        sql = upsert.create_insert('atable', 'aschema', fields, data)
        assert not sql

    policy = 'onlyinsert'
    data = [{'col1': 1, 'col2': '2'}, {'col3': 3}]
    fields = ['col1', 'col2', 'col3']
    sql = upsert.create_upsert('atable', 'aschema', fields, data, policy)
    assert sql == "INSERT INTO aschema.atable (col1,col2,col3) " \
                  "VALUES ('1','2',NULL),(NULL,NULL,'3') " \
                  "ON CONFLICT ON CONSTRAINT atable_pkey DO NOTHING"

    policy = 'upsert'
    sql = upsert.create_upsert('atable', 'aschema', fields, data, policy)
    assert sql == "INSERT INTO aschema.atable (col1,col2,col3) " \
                  "VALUES ('1','2',NULL),(NULL,NULL,'3') " \
                  "ON CONFLICT ON CONSTRAINT atable_pkey DO " \
                  "UPDATE SET (col1,col2,col3) = (EXCLUDED.col1,EXCLUDED.col2,EXCLUDED.col3) " \
                  "WHERE atable.cod_staz = EXCLUDED.cod_staz AND atable.data_i = EXCLUDED.data_i"


def test_upsert_items(conn):
    # temperature
    table_name = 'ds__t200'
    sql = 'select * from test.ds__t200 where cod_staz=12502 order by cod_staz, data_i'
    initial_results = conn.execute(sql).fetchall()
    assert initial_results == []

    data_path = join(TEST_DATA_PATH, 'indicators', 'expected', '%s.csv' % table_name)
    items = export.csv2items(data_path, ignore_empty_fields=True)
    policy = 'onlyinsert'
    num_updates = upsert.upsert_items(conn, items, policy, schema='test', table_name=table_name)
    assert num_updates == len(items)  # test schema initially is empty for cod_staz=12502

    sql = 'select * from test.ds__t200 where cod_staz=12502 order by cod_staz, data_i'
    effective_results = conn.execute(sql).fetchall()
    assert effective_results == [
        (datetime(2018, 1, 1, 0, 0), 12502, 4, '("(24,1)",11.5,1.5,9.7,"2018-01-01 11:00:00")',
         '(,,,,,,,,,,)', '("(24,1)",4.7,1.9,8.8,"2018-01-01 20:00:00")', '(,,,,,,,,)',
         '("(24,1)",9.3,1.6)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)'),
        (datetime(2018, 1, 2, 0, 0), 12502, 4, '("(24,1)",11.4,3.0,7.5,"2018-01-02 13:00:00")',
         '(,,,,,,,,,,)', '("(24,1)",1.5,3.2,6.2,"2018-01-02 21:00:00")', '(,,,,,,,,)',
         '("(24,1)",6.8,3.1)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)'),
        (datetime(2018, 1, 3, 0, 0), 12502, 4, '("(24,1)",12.5,3.4,8.1,"2018-01-03 20:00:00")',
         '(,,,,,,,,,,)', '("(24,1)",1.3,3.7,6.6,"2018-01-03 06:00:00")', '(,,,,,,,,)',
         '("(24,1)",7.3,3.5)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)'),
        (datetime(2018, 1, 4, 0, 0), 12502, 4, '("(24,1)",11.9,1.3,9.9,"2018-01-04 13:00:00")',
         '(,,,,,,,,,,)', '("(24,1)",6.0,1.8,8.5,"2018-01-04 01:00:00")', '(,,,,,,,,)',
         '("(24,1)",9.2,1.6)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)'),
        (datetime(2018, 1, 5, 0, 0), 12502, 4, '("(24,1)",15.0,3.2,10.1,"2018-01-05 14:00:00")',
         '(,,,,,,,,,,)', '("(24,1)",4.5,3.3,8.7,"2018-01-05 03:00:00")', '(,,,,,,,,)',
         '("(24,1)",9.4,3.2)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)'),
        (datetime(2018, 1, 6, 0, 0), 12502, 4, '("(24,1)",13.2,1.5,10.7,"2018-01-06 14:00:00")',
         '(,,,,,,,,,,)', '("(24,1)",7.2,1.6,9.9,"2018-01-06 22:00:00")', '(,,,,,,,,)',
         '("(24,1)",10.3,1.6)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)'),
        (datetime(2018, 1, 7, 0, 0), 12502, 4, '("(24,1)",16.1,1.8,13.4,"2018-01-07 12:00:00")',
         '(,,,,,,,,,,)', '("(24,1)",9.5,1.8,12.1,"2018-01-07 06:00:00")', '(,,,,,,,,)',
         '("(24,1)",12.7,1.9)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)'),
        (datetime(2018, 1, 8, 0, 0), 12502, 4, '("(24,1)",18.0,2.1,14.6,"2018-01-08 12:00:00")',
         '(,,,,,,,,,,)', '("(24,1)",9.9,2.2,13.4,"2018-01-08 01:00:00")', '(,,,,,,,,)',
         '("(24,1)",14.0,2.1)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)'),
        (datetime(2018, 1, 9, 0, 0), 12502, 4, '("(24,1)",13.7,2.1,10.5,"2018-01-09 00:00:00")',
         '(,,,,,,,,,,)', '("(24,1)",5.7,2.1,9.9,"2018-01-09 23:00:00")', '(,,,,,,,,)',
         '("(24,1)",10.2,2.1)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)'),
        (datetime(2018, 1, 10, 0, 0), 12502, 4, '("(24,1)",11.8,2.9,7.4,"2018-01-10 14:00:00")',
         '(,,,,,,,,,,)', '("(24,1)",1.5,3.0,6.2,"2018-01-10 05:00:00")', '(,,,,,,,,)',
         '("(24,1)",6.7,3.0)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
         '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)')
    ]
    policy = 'upsert'
    items0 = items[0]
    assert items0['tmxgg'] == "((24, 1), 11.5, 1.5, 9.7, '2018-01-01T11:00:00')"
    items0['tmxgg'] = "((24, 1), 10.5,, None, '2018-01-01T11:00:00')"
    items[0] = items0
    num_updates = upsert.upsert_items(conn, items, policy, schema='test', table_name=table_name)

    assert num_updates == 10
    effective_results2 = conn.execute(sql).fetchall()
    assert effective_results2[1:] == effective_results[1:]
    assert effective_results2[0] == (
        datetime(2018, 1, 1, 0, 0), 12502, 4, '("(24,1)",10.5,,,"2018-01-01 11:00:00")',
        '(,,,,,,,,,,)', '("(24,1)",4.7,1.9,8.8,"2018-01-01 20:00:00")', '(,,,,,,,,)',
        '("(24,1)",9.3,1.6)', '("(,)",,)', '("(,)",,,,)', '("(,)",)', '("(,)",,,,,,,,,,,,,,)',
        '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
        '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
        '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)',
        '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)', '("(,)",,,,)'
    )


def test_choose_main_record():
    records = [
        {'data_i': '2010-01-01 00:00:00', 'cod_staz': 5540, 'cod_aggr': 4,
         'prec24.flag.ndati': '24', 'prec24.flag.wht': '-33', 'prec24.val_tot': '5.5',
         'cl_prec24.dry': '1', 'cl_prec24.wet_02': '3'},
        {'data_i': '2010-01-01 00:00:00', 'cod_staz': 5541, 'cod_aggr': 4,
         'prec24.flag.ndati': '24', 'prec24.flag.wht': '1', 'prec24.val_tot': '5.6',
         'cl_prec24.dry': '4', 'cl_prec24.wet_01': 'NULL', 'cl_prec24.wet_02': '6'},
        {'data_i': '2010-01-01 00:00:00', 'cod_staz': 5542, 'cod_aggr': 4,
         'prec24.flag.ndati': '24', 'prec24.flag.wht': '1', 'prec24.val_tot': '5.7',
         'cl_prec24.dry': '7', 'cl_prec24.wet_01': '8', 'cl_prec24.wet_02': '9'},
    ]
    chosen = upsert.choose_main_record(records, 'prec24.val_tot')
    assert chosen == {
        'data_i': '2010-01-01 00:00:00', 'cod_staz': 5541, 'cod_aggr': 4,
        'prec24.flag.ndati': '24', 'prec24.flag.wht': '1', 'prec24.val_tot': '5.6',
        'cl_prec24.dry': '4', 'cl_prec24.wet_01': 'NULL', 'cl_prec24.wet_02': '6',
        'cod_stazprinc': 5541
    }
    chosen = upsert.choose_main_record(records, 'cl_prec24.dry')
    assert chosen == {
        'data_i': '2010-01-01 00:00:00', 'cod_staz': 5540, 'cod_aggr': 4,
        'prec24.flag.ndati': '24', 'prec24.flag.wht': '-33', 'prec24.val_tot': '5.5',
        'cl_prec24.dry': '1', 'cl_prec24.wet_02': '3',
        'cod_stazprinc': 5540
    }
    chosen = upsert.choose_main_record(records, 'cl_prec24.wet_01')
    assert chosen == {
        'data_i': '2010-01-01 00:00:00', 'cod_staz': 5542, 'cod_aggr': 4,
        'prec24.flag.ndati': '24', 'prec24.flag.wht': '1', 'prec24.val_tot': '5.7',
        'cl_prec24.dry': '7', 'cl_prec24.wet_01': '8', 'cl_prec24.wet_02': '9',
        'cod_stazprinc': 5542
    }
    chosen = upsert.choose_main_record(records, 'cl_prec24.wet_04')
    assert not chosen


def test_load_unique_data(conn):
    startschema = 'test'
    targetschema = 'test2'
    # TODO
    # upsert.load_unique_data(conn, startschema, targetschema, only_tables=['ds__preci'])
