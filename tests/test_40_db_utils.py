
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
    table_name = 'ds__t200'
    expected_result = [
        'data_i', 'cod_staz', 'cod_aggr', 'tmxgg', 'cl_tmxgg', 'tmngg', 'cl_tmngg',
        'tmdgg', 'tmdgg1', 'deltagg', 'day_gelo', 'cl_tist', 't00', 't01', 't02',
        't03', 't04', 't05', 't06', 't07', 't08', 't09', 't10', 't11', 't12', 't13',
        't14', 't15', 't16', 't17', 't18', 't19', 't20', 't21', 't22', 't23']
    result = db_utils.get_table_columns(table_name, schema='dailypdbanpacarica')
    assert result == expected_result

