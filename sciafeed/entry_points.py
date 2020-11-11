"""
This modules provides the functions of the SCIA FEED package's entry points
"""
from datetime import datetime
from os import listdir, mkdir
from os.path import exists, isfile, join, splitext
import sys
import warnings

import click
from sqlalchemy import exc

from sciafeed import arpaer
from sciafeed import db_utils
from sciafeed import export
from sciafeed import gui
from sciafeed import hiscentral
from sciafeed import process
from sciafeed import querying
from sciafeed import upsert
from sciafeed import utils


warnings.filterwarnings("ignore", category=exc.SAWarning)


@click.command()
@click.argument('in_filepath', type=click.Path(exists=True, dir_okay=False))
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
@click.option('--outdata_filepath', '-d', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output data file")
@click.option('--parameters_filepath', '-p', type=click.Path(exists=True, dir_okay=False),
              help="customized file path containing information about parameters")
def make_report(in_filepath, report_path, outdata_filepath, parameters_filepath):
    """
    Parse a file containing data located at `in_filepath` and generate a report.
    If outdata_folder is specified, it also export parsed data.
    """
    logger = utils.setup_log(report_path, log_format='%(message)s')
    logger.info('START PROCESS')
    data = process.make_report(in_filepath, outdata_filepath, parameters_filepath, logger)
    if outdata_filepath and data:
        logger.info('data saved on %s' % outdata_filepath)
    logger.info('END PROCESS')

@click.command()
@click.argument('in_folder', type=click.Path(exists=True, file_okay=False))
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
@click.option('--outdata_folder', '-d', type=click.Path(exists=False, file_okay=False),
              help="folder path where to put the output data files")
def make_reports(in_folder, report_path, outdata_folder):
    """
    Parse a folder containing data located at `in_folder` and generate a report.
    If outdata_folder is specified, it also export parsed data.
    """
    logger = utils.setup_log(report_path, log_format='%(message)s')
    logger.info('START PROCESS')
    if outdata_folder and not exists(outdata_folder):
        mkdir(outdata_folder)
    children = sorted(listdir(in_folder))
    for child in children:
        in_filepath = join(in_folder, child)
        if not isfile(in_filepath):
            continue
        logger.info('processing file %r' % child)
        if outdata_folder:
            outdata_filepath = join(outdata_folder, child + '.csv')
        else:
            outdata_filepath = None
        process.make_report(in_filepath, outdata_filepath=outdata_filepath, logger=logger)
    logger.info('END PROCESS')

@click.command()
@click.argument('out_csv_folder', type=click.Path(exists=False, file_okay=False))
@click.option('--region_id', '-R', help="code of the region to download (for example '01')")
@click.option('--variables', '-v', multiple=True,
              help="list of the variables to download. Default is 'Precipitation', 'Tmax', 'Tmin'",
              default=['Precipitation', 'Tmax', 'Tmin'])
@click.option('--locations', '-l', multiple=True,
              help="list of the locations to download. Default is all the locations of the region")
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
def download_hiscentral(out_csv_folder, region_id, variables, locations, report_path):
    """
    Download CSV of the HISCENTRAL for region, locations and variables selected into an
    output folder.
    """
    logger = utils.setup_log(report_path)
    if not region_id:
        logger.error('region_id is required')
        return
    if region_id not in hiscentral.REGION_IDS_MAP:
        print("region_id %r is not recognized as a valid code" % region_id)
        return
    if not locations:
        locations = None
    # print('out_csv_folder: %r' % out_csv_folder)
    if not exists(out_csv_folder):
        mkdir(out_csv_folder)
    ret_value = hiscentral.download_hiscentral(
        region_id, out_csv_folder, variables, locations, logger=logger)
    logger.info('download completed')
    return ret_value


@click.command()
@click.argument('data_folder', type=click.Path(exists=True, file_okay=False))
@click.argument('indicators_folder', type=click.Path(exists=False, file_okay=False))
@click.option('--dburi', '-d', default=db_utils.DEFAULT_DB_URI,
              help="insert something like 'postgresql://user:password@address:port/database', "
                   "default is %s" % db_utils.DEFAULT_DB_URI)
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
def compute_daily_indicators(data_folder, indicators_folder, dburi, report_path):
    """
    Compute daily indicators from data files located at folder `data_folder`,
    and put results as CSV files in the specified `indicators_folder`.
    """
    if not exists(indicators_folder):
        mkdir(indicators_folder)
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()
    logger = utils.setup_log(report_path)
    logger.info('starting process of compute daily indicators')
    process.compute_daily_indicators(conn, data_folder, indicators_folder, logger)
    logger.info('end process of compute daily indicators')


@click.command()
@click.argument('data_folder', type=click.Path(exists=True, file_okay=False))
@click.option('--dburi', '-d', default=db_utils.DEFAULT_DB_URI,
              help="insert something like 'postgresql://user:password@address:port/database', "
                   "default is %s" % db_utils.DEFAULT_DB_URI)
@click.option('--stations_path', '-s', type=click.Path(exists=False, dir_okay=False),
              help="file path of the CSV with the new stations found")
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
def find_new_stations(data_folder, dburi, stations_path, report_path):
    """
    Examine stations on data included in folder `data_folder` and creates a CSV with the new
    stations not found in the database.
    """
    if not stations_path:
        print('"stations_path" is required')
        sys.exit(2)
    msgs1, not_found_stations = querying.find_new_stations(data_folder, dburi)
    export.stations2csv(not_found_stations, stations_path, extra_fields=['source'])
    msgs2 = ['Exported new stations on CSV %r' % stations_path]
    msgs = msgs1 + msgs2
    if not report_path:
        for msg in msgs:
            print(msg)
    else:
        with open(report_path, 'a') as fp:
            for msg in msgs:
                fp.write(msg + '\n')


@click.command()
@click.argument('stations_path', type=click.Path(exists=True, dir_okay=False))
@click.option('--dburi', '-d', default=db_utils.DEFAULT_DB_URI,
              help="insert something like 'postgresql://user:password@address:port/database', "
                   "default is %s" % db_utils.DEFAULT_DB_URI)
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
def upsert_stations(stations_path, dburi, report_path):
    """
    Massive import of stations from a CSV file. If a station already exists, its values are
    updated correspondingly.
    """
    if report_path and exists(report_path):
        print('wrong "report_path": the report must not exist or will be overwritten')
        sys.exit(2)
    msgs, _, _ = upsert.upsert_stations(dburi, stations_path)
    if not report_path:
        for msg in msgs:
            print(msg)
    else:
        with open(report_path, 'a') as fp:
            for msg in msgs:
                fp.write(msg + '\n')


@click.command()
@click.option('--dburi', '-d', default=db_utils.DEFAULT_DB_URI,
              help="insert something like 'postgresql://user:password@address:port/database', "
                   "default is %s" % db_utils.DEFAULT_DB_URI)
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
@click.option('--station_where', '-w',
              help="""SQL where condition on stations (for example: "cod_rete='15'"). 
                      If omitted, checks all stations""")
@click.option('--schema', '-s', default='dailypdbanpacarica',
              help="""database schema to use. Default is 'dailypdbanpacarica'""")
@click.option('--omit_flagsync', default=False, is_flag=True,
              help="""""")
def check_chain(dburi, report_path, station_where, schema, omit_flagsync):
    logger = utils.setup_log(report_path, log_format='%(asctime)s: %(message)s')
    stations_ids = querying.get_stations_by_where(dburi, station_where)
    process.process_checks_chain(dburi, stations_ids, schema, logger, omit_flagsync)


@click.command()
@click.argument('download_folder', type=click.Path(exists=False, file_okay=False))
@click.option('--start', '-s', help='start date: use format "YYYY-MM-DD". Default is the '
                                    'oldest available')
@click.option('--end', '-e', help='end date: use format "YYYY-MM-DD". Default is today')
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
@click.option('--parameters_filepath', '-p', type=click.Path(exists=True, dir_okay=False),
              help="customized file path containing information about parameters"
                   "default is %s" % arpaer.PARAMETERS_FILEPATH,
              default=arpaer.PARAMETERS_FILEPATH)
@click.option('--credentials_folder', '-c', type=click.Path(exists=True, dir_okay=True),
              help="folder containing gdrive credentials."
                   "default is %s" % arpaer.DEFAULT_CREDENTIALS_FOLDER,
              default=arpaer.DEFAULT_CREDENTIALS_FOLDER)
def download_er(start, end, download_folder, report_path, parameters_filepath, credentials_folder):
    """
    Download utility for ARPA Emilia-Romagna.
    """
    logger = utils.setup_log(report_path)
    if not exists(download_folder):
        mkdir(download_folder)
    now = datetime.now()
    if start:
        try:
            start = datetime.strptime(start, '%Y-%m-%d')
        except ValueError:
            logger.error('%r doesn''t seem a date in format "YYYY-MM-DD"' % start)
            sys.exit(2)
    if end:
        try:
            end = datetime.strptime(end, '%Y-%m-%d')
        except ValueError:
            logger.error('%r doesn''t seem a date in format "YYYY-MM-DD"' % end)
            sys.exit(2)
    else:
        end = now

    if start > end or start > now:
        logger.error('required time interval is not valid')
        sys.exit(2)
    arpaer.download_er(
        download_folder, start, end, parameters_filepath=parameters_filepath,
        credentials_folder=credentials_folder, logger=logger)
    logger.info('download completed')


@click.command()
@click.argument('data_folder', type=click.Path(exists=True, file_okay=False))
@click.option('--dburi', '-d', default=db_utils.DEFAULT_DB_URI,
              help="insert something like 'postgresql://user:password@address:port/database', "
                   "default is %s" % db_utils.DEFAULT_DB_URI)
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
@click.option('--policy', '-p', type=click.Choice(['onlyinsert', 'upsert']), default='upsert',
              help="policy to apply in the insert")
@click.option('--schema', '-s', default='dailypdbanpacarica',
              help="""database schema to use. Default is 'dailypdbanpacarica'""")
def insert_daily_indicators(data_folder, dburi, report_path, policy, schema):
    """
    Insert indicators from a folder `data_folder` inside the database in the selected schema.
    """
    logger = utils.setup_log(report_path)
    logger.info('starting process of inserting data')
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()
    children = sorted(listdir(data_folder))
    for child in children:
        csv_table_path = join(data_folder, child)
        if not isfile(csv_table_path):
            continue
        table_name = splitext(child)[0]
        table_cols = db_utils.get_table_columns(table_name, schema=schema)
        if not table_cols:
            logger.warning('ignoring file %s: not found the table %s.%s'
                           % (child, schema, table_name))
            continue
        logger.info('- reading data from file %s' % child)
        try:
            items = export.csv2items(csv_table_path)
            logger.info('- start insert of %s records from file %s' % (len(items), child))
            upsert.upsert_items(conn, items, policy, schema, table_name, logger)
        except:
            logger.exception('something went wrong')
            raise

@click.command()
@click.option('--dburi', '-d', default=db_utils.DEFAULT_DB_URI,
              help="insert something like 'postgresql://user:password@address:port/database', "
                   "default is %s" % db_utils.DEFAULT_DB_URI)
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
@click.option('--startschema', '-s', default='dailypdbanpacarica',
              help="""database schema to use for data input. Default is 'dailypdbanpacarica'""")
@click.option('--targetschema', '-t', default='dailypdbanpaclima',
              help="""database schema to use. Default is 'dailypdbanpaclima'""")
def load_unique_data(dburi, report_path, startschema, targetschema):
    """Utility for 'eliminazione serie duplicate'"""
    logger = utils.setup_log(report_path)
    logger.info('starting process of loading unique data (from %s to %s)'
                % (startschema, targetschema))

    upsert.load_unique_data(dburi, startschema, targetschema, logger)
    logger.info('process concluded')


@click.command()
@click.option('--dburi', '-d', default=db_utils.DEFAULT_DB_URI,
              help="insert something like 'postgresql://user:password@address:port/database', "
                   "default is %s" % db_utils.DEFAULT_DB_URI)
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
@click.option('--schema', '-s', default='dailypdbanpacarica',
              help="""database schema to use for data input. Default is 'dailypdbanpacarica'""")
def compute_daily_indicators2(dburi, report_path, schema):
    """Utility for update secondary indicators"""
    logger = utils.setup_log(report_path)
    logger.info('starting process of loading secondary indicators in schema %s' % schema)
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()
    process.compute_daily_indicators2(conn, schema, logger)
    logger.info('process concluded')


@click.command()
@click.option('--dburi', '-d', default=db_utils.DEFAULT_DB_URI,
              help="insert something like 'postgresql://user:password@address:port/database', "
                   "default is %s" % db_utils.DEFAULT_DB_URI)
@click.option('--report_path', '-r', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
@click.option('--startschema', '-s', default='dailypdbanpaclima',
              help="""database schema to use for data input. Default is 'dailypdbanpaclima'""")
@click.option('--targetschema', '-t', default='dailypdbanpaclima',
              help="""database schema to use. Default is 'dailypdbanpaclima'""")
@click.option('--policy', '-p', type=click.Choice(['onlyinsert', 'upsert']), default='upsert',
              help="policy to apply in the insert")
def process_dma(dburi, report_path, startschema, targetschema, policy):
    """Utility for update DMA indicators"""
    logger = utils.setup_log(report_path)
    logger.info('starting process of update DMA indicators from schema %s to schema %s'
                % (startschema, targetschema))
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()
    process.process_dma(conn, startschema, targetschema, policy, logger)
    logger.info('process concluded')


def sciafeed_gui(policy):
    """Graphical interface for SCIA-FEED"""
    return gui.run_sciafeed_gui()
