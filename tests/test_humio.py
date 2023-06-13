import pytest
from cessoc import humio

def test_write_humio_type_error():
        """Ensures write_humio is raising exceptions on type values"""
        bad_entry = dict()
        with pytest.raises(TypeError):
            humio.write(bad_entry)
        bad_entry = set()
        with pytest.raises(TypeError):
            humio.write(bad_entry)
        bad_entry = str()
        with pytest.raises(TypeError):
            humio.write(bad_entry)

