
import click

from sciafeed import utils


@click.command()
@utils.setup_log
def hello_world():
    """
    Just for testing entry points
    """
    ret_value = utils.hello_world()
    print(ret_value)
