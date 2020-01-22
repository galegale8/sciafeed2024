
from sciafeed import utils


def test_hello_world():
    expected = 'Hello world!'
    effective = utils.hello_world()
    assert effective == expected
