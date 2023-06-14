from cessoc.aws import s3
import pytest
from botocore.exceptions import ClientError, ParamValidationError


def test_write_type_error():
    """Ensures write is raising exceptions on type values"""
    bad_entry = set()
    with pytest.raises(TypeError):
        s3.write(bad_entry)
    bad_entry = tuple()
    with pytest.raises(TypeError):
        s3.write(bad_entry)


def test_s3_access_denied():
    """Test s3 connections"""
    with pytest.raises(ClientError):
        s3.read(
            key="AccessDenied",  # nosec
            bucket="my-test-bucket",
            access_key="AKIATESTSTRING",
            secret_key="asdiv8e=34fdfa9dfad",
            region_name="us-west-2",
        )

    with pytest.raises(ClientError):
        s3.write(
            key="AccessDenied",  # nosec
            bucket="my-test-bucket",
            body="test body",
            access_key="AKIATESTSTRING",
            secret_key="asdiv8e=34fdfa9dfad",
            region_name="us-west-2",
        )


def test_s3_put_types():
    """Data input to s3 can only be str, bytes, or file-like object"""

    with pytest.raises(ParamValidationError):
        s3.write(
            key="TypeTest",
            bucket="my-test-bucket",
            body={},
            region_name="us-west-2",
        )


def test_s3_get_failure():
    """Test s3 function isn't hiding exceptions"""
    with pytest.raises(ParamValidationError):
        s3.write(key="", bucket="my-test-bucket", body="test body", region_name="us-west-2")
