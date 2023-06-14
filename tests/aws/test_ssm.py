import pytest
from cessoc.aws import ssm


def test_put_value_type_error():
    """Ensures put_value raising exceptions"""
    bad_entry = dict()
    with pytest.raises(TypeError):
        ssm.put_value("garbage-entry-test", bad_entry)
    bad_entry = list()
    with pytest.raises(TypeError):
        ssm.put_value("garbage-entry-test", bad_entry)
