"""
This modules provides the functions of the SCIA FEED package's entry points
"""
import click

from sciafeed import arpa19
from sciafeed import arpa21


@click.command()
@click.argument('in_filepath', type=click.Path(exists=True, dir_okay=False))
@click.option('--out_filepath', '-o', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
@click.option('--outdata_filepath', '-d', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output data file")
@click.option('--parameters_filepath', '-p', type=click.Path(exists=True, dir_okay=False),
              help="customized file path containing information about parameters",
              default=arpa19.PARAMETERS_FILEPATH)
def make_arpa19_report(**kwargs):
    """
    Parse an ARPA19 file located at `in_filepath` and generate a report.
    Optionally, it can also export parsed data.
    """
    msgs, _ = arpa19.make_report(**kwargs)
    if not kwargs['out_filepath']:
        for msg in msgs:
            print(msg)
    if kwargs['outdata_filepath']:
        print('data saved on %s' % kwargs['outdata_filepath'])


@click.command()
@click.argument('in_filepath', type=click.Path(exists=True, dir_okay=False))
@click.option('--out_filepath', '-o', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output report. If not provided, prints on screen")
@click.option('--outdata_filepath', '-d', type=click.Path(exists=False, dir_okay=False),
              help="file path of the output data file")
@click.option('--parameters_filepath', '-p', type=click.Path(exists=True, dir_okay=False),
              help="customized file path containing information about parameters",
              default=arpa21.PARAMETERS_FILEPATH)
def make_arpa21_report(**kwargs):
    """
    Parse an ARPA21 file located at `in_filepath` and generate a report.
    Optionally, it can also export parsed data.
    """
    msgs, _ = arpa21.make_report(**kwargs)
    if not kwargs['out_filepath']:
        for msg in msgs:
            print(msg)
    if kwargs['outdata_filepath']:
        print('data saved on %s' % kwargs['outdata_filepath'])
