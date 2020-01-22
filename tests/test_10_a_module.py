
from sciafeed import a_module


def test_hello_world():
    expected = 'Hello world!'
    effective = a_module.hello_world()
    assert effective == expected
