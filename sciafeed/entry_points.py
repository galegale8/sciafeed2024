"""
This modules provides the functions of the SCIA FEED package's entry points
"""
import click

from sciafeed import utils


@click.group()
def sciacmd():
    """Command group of SCIA FEED"""
    pass


@click.command()
def hello_world():
    """
    Just for testing entry points
    """
    ret_value = utils.hello_world()
    print(ret_value)
