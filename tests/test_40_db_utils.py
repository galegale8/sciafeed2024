
from datetime import datetime
from decimal import Decimal
import pytest

from sciafeed import db_utils

# ignore some useless SAWarnings in this module
pytestmark = pytest.mark.filterwarnings("ignore:.*Did not recognize type.*::sqlalchemy[.*]")


def test_get_table_columns():
    # public table
    table_name = 'geography_columns'
    expected_result = [
        'f_table_catalog', 'f_table_schema', 'f_table_name',
        'f_geography_column', 'coord_dimension', 'srid', 'type']
    result = db_utils.get_table_columns(table_name)
    assert result == expected_result

    # table of a schema
    table_name = 'dailypdbanpacarica.ds__t200'
    expected_result = [
        'data_i', 'cod_staz', 'cod_aggr', 'tmxgg', 'cl_tmxgg', 'tmngg', 'cl_tmngg',
        'tmdgg', 'tmdgg1', 'deltagg', 'day_gelo', 'cl_tist', 't00', 't01', 't02',
        't03', 't04', 't05', 't06', 't07', 't08', 't09', 't10', 't11', 't12', 't13',
        't14', 't15', 't16', 't17', 't18', 't19', 't20', 't21', 't22', 't23']
    result = db_utils.get_table_columns(table_name)
    assert result == expected_result


def test_reset_flags(conn):
    # with filtering on stations
    stations_ids = [1, 2, 3]
    flag_threshold = -5
    set_flag = 2
    expected = ["""
      UPDATE dailypdbanpacarica.ds__preci SET ( 
      ((prec24).flag).wht,
      ((prec01).flag).wht,
      ((prec06).flag).wht,
      ((prec12).flag).wht
      ) = (2, 2, 2, 2)
      WHERE ((prec24).flag).wht < -5 AND cod_staz IN (1, 2, 3)""",
      """
      UPDATE dailypdbanpacarica.ds__t200  SET ((tmngg).flag).wht = 2 
      WHERE ((tmngg).flag).wht < -5 AND cod_staz IN (1, 2, 3)""",
      """
      UPDATE dailypdbanpacarica.ds__t200  SET ((tmxgg).flag).wht = 2 
      WHERE ((tmxgg).flag).wht < -5 AND cod_staz IN (1, 2, 3)"""]

    executed = db_utils.reset_flags(conn, stations_ids, flag_threshold, set_flag, dry_run=True)
    assert executed == expected

    # without filtering on stations
    stations_ids = None
    flag_threshold = -6
    set_flag = 1
    expected = ["""
      UPDATE dailypdbanpacarica.ds__preci SET ( 
      ((prec24).flag).wht,
      ((prec01).flag).wht,
      ((prec06).flag).wht,
      ((prec12).flag).wht
      ) = (1, 1, 1, 1)
      WHERE ((prec24).flag).wht < -6""",
      """
      UPDATE dailypdbanpacarica.ds__t200  SET ((tmngg).flag).wht = 1 
      WHERE ((tmngg).flag).wht < -6""",
      """
      UPDATE dailypdbanpacarica.ds__t200  SET ((tmxgg).flag).wht = 1 
      WHERE ((tmxgg).flag).wht < -6"""]
    executed = db_utils.reset_flags(conn, stations_ids, flag_threshold, set_flag, dry_run=True)
    assert executed == expected


class DummyRecord():
    def __init__(self, cod_staz, data_i, val, field):
        self.cod_staz = cod_staz
        self.data_i = data_i
        setattr(self, field, val)


def test_set_prec_flags(conn):
    records = [
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0')],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('0')],
        [3, datetime(2001, 5, 20, 0, 0), Decimal('0.4')]
    ]
    executed = db_utils.set_prec_flags(conn, records, -15, dry_run=True)
    expected = [
            """
            UPDATE dailypdbanpacarica.ds__preci SET (
            ((prec24).flag).wht,
            ((prec01).flag).wht,
            ((prec06).flag).wht,
            ((prec12).flag).wht
            ) = (-15, -15, -15, -15)
            WHERE data_i = '2001-05-18 00:00:00' AND cod_staz = 1""",
            """
            UPDATE dailypdbanpacarica.ds__preci SET (
            ((prec24).flag).wht,
            ((prec01).flag).wht,
            ((prec06).flag).wht,
            ((prec12).flag).wht
            ) = (-15, -15, -15, -15)
            WHERE data_i = '2001-05-19 00:00:00' AND cod_staz = 2""",
            """
            UPDATE dailypdbanpacarica.ds__preci SET (
            ((prec24).flag).wht,
            ((prec01).flag).wht,
            ((prec06).flag).wht,
            ((prec12).flag).wht
            ) = (-15, -15, -15, -15)
            WHERE data_i = '2001-05-20 00:00:00' AND cod_staz = 3"""
    ]
    assert executed == expected


def test_set_temp_flags(conn):
    records = [
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0')],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('0'), 'val_md'],
        [3, datetime(2001, 5, 20, 0, 0), Decimal('0.4'), 'val_md'],
    ]
    # case var=Tmax
    executed = db_utils.set_temp_flags(conn, records, 'Tmax', -15, dry_run=True)
    expected = [
            """
            UPDATE dailypdbanpacarica.ds__t200 SET ((tmxgg).flag).wht = -15
            WHERE data_i = '2001-05-18 00:00:00' AND cod_staz = 1""",
            """
            UPDATE dailypdbanpacarica.ds__t200 SET ((tmxgg).flag).wht = -15
            WHERE data_i = '2001-05-19 00:00:00' AND cod_staz = 2""",
            """
            UPDATE dailypdbanpacarica.ds__t200 SET ((tmxgg).flag).wht = -15
            WHERE data_i = '2001-05-20 00:00:00' AND cod_staz = 3"""
    ]
    assert executed == expected

    # case var=Tmin
    executed = db_utils.set_temp_flags(conn, records, 'Tmin', -15, dry_run=True)
    expected = [
            """
            UPDATE dailypdbanpacarica.ds__t200 SET ((tmngg).flag).wht = -15
            WHERE data_i = '2001-05-18 00:00:00' AND cod_staz = 1""",
            """
            UPDATE dailypdbanpacarica.ds__t200 SET ((tmngg).flag).wht = -15
            WHERE data_i = '2001-05-19 00:00:00' AND cod_staz = 2""",
            """
            UPDATE dailypdbanpacarica.ds__t200 SET ((tmngg).flag).wht = -15
            WHERE data_i = '2001-05-20 00:00:00' AND cod_staz = 3"""
    ]
    assert executed == expected
