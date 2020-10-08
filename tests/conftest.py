
import pytest

from sciafeed import db_utils


@pytest.yield_fixture(scope='session')
def conn():
    # configuration = config.TEST_DB_CONFIG
    # model.configure(configuration)
    engine = db_utils.ensure_engine()
    # runner = CliRunner()
    # # NOTE: we are asserting entry points work well :-)
    # runner.invoke(entry_points.create_structure, ['--dburl', engine.url])
    # runner.invoke(entry_points.load_dictionaries, ['--dburl', engine.url])
    # for datafile in []:
    #     file_path = os.path.join(TESTS_DATA_PATH, datafile)
    #     cmd = "psql -d %s -h %s -p %s -U %s < '%s'" % (
    #         engine.url.database, engine.url.host,
    #         engine.url.port, engine.url.username, file_path)
    #     os.system(cmd)
    connection = engine.connect()
    yield connection
    connection.close()