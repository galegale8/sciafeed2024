"""
This modules provides the functions of the SCIA FEED package's entry points
"""
import click

from sciafeed import a_module


@click.command()
def hello_world():
    """
    Just for testing entry points
    """
    ret_value = a_module.hello_world()
    print(ret_value)
