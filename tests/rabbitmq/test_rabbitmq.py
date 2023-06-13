import datetime
from io import StringIO
from dateutil.tz import tzutc
import pytest
import boto3
from botocore.exceptions import ClientError, ParamValidationError

from cessoc.rabbitmq import rabbitmq as rabbit

class MockBoto3Client:
    """Defines mock functions for boto3 s3 get_object()"""

    class MockStream:
        """Defines mock response from S3 actions"""

        def __init__(self, content):
            """Mimic s3 file stream"""
            self.content = content

        def read(self):
            """Mimics s3 file stream reader, data returned as encoded"""
            return str(self.content).encode()

    def __init__(self, *args, **kwargs):
        """Instantiates like boto3.client("s3")"""
        self.response_get_success = {
            "ResponseMetadata": {
                "RequestId": "A7AC64ABC4A4C3CC",
                "HostId": "Jy9mil2Zk3JG8gdIwDaQ3D3of7Z49Vx9tibwHMqMk4akd/y+CEEjgxV9Y/whJ3jQUu5dS8Al9M4=",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "x-amz-id-2": "Jy9mil2Zk3JG8gdIwDaQ3D3of7Z49Vx9tibwHMqMk4akd/y+CEEjgxV9Y/whJ3jQUu5dS8Al9M4=",
                    "x-amz-request-id": "A7AC64ABC4A4C3CC",
                    "date": "Thu, 27 Aug 2020 17:06:40 GMT",
                    "last-modified": "Fri, 20 Mar 2020 16:11:03 GMT",
                    "etag": '"f14e198a48b60393b99e3b3bf8bac6f7"',
                    "x-amz-server-side-encryption": "AES256",
                    "accept-ranges": "bytes",
                    "content-type": "text/markdown",
                    "content-length": "147",
                    "server": "AmazonS3",
                },
                "RetryAttempts": 0,
            },
            "AcceptRanges": "bytes",
            "LastModified": datetime.datetime(2020, 3, 20, 16, 11, 3, tzinfo=tzutc()),
            "ContentLength": 147,
            "ETag": '"f14e198a48b60393b99e3b3bf8bac6f7"',
            "ContentType": "text/markdown",
            "ServerSideEncryption": "AES256",
            "Metadata": {},
            "Body": self.MockStream({"test": True}),
        }
        self.response_denied = {
            "Error": {"Code": "AccessDenied", "Message": "Access Denied"},
            "ResponseMetadata": {
                "RequestId": "504EAABD8D76D187",
                "HostId": "SCM8c6nLkdBou7jv96F5GvkA/uXyxnkYnIjJoMpXrzcpRsM0+hz7UeAigl09LozxlxsszwsjTt8=",
                "HTTPStatusCode": 403,
                "HTTPHeaders": {
                    "x-amz-request-id": "504EAABD8D76D187",
                    "x-amz-id-2": "SCM8c6nLkdBou7jv96F5GvkA/uXyxnkYnIjJoMpXrzcpRsM0+hz7UeAigl09LozxlxsszwsjTt8=",
                    "content-type": "application/xml",
                    "transfer-encoding": "chunked",
                    "date": "Thu, 27 Aug 2020 18:02:21 GMT",
                    "server": "AmazonS3",
                },
                "RetryAttempts": 0,
            },
        }
        self.response_put_success = {
            "ResponseMetadata": {
                "RequestId": "BQAN4K5QFM4QAR9W",
                "HostId": "moH/k4jmNiLMf3auepUHkhr5eOwXocoKEn5shdZmfwILdB2efyVZo7146EB0PTfn0IaIO8Moj3Q=",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "x-amz-id-2": "moH/k4jmNiLMf3auepUHkhr5eOwXocoKEn5shdZmfwILdB2efyVZo7146EB0PTfn0IaIO8Moj3Q=",
                    "x-amz-request-id": "BQAN4K5QFM4QAR9W",
                    "date": "Thu, 27 Aug 2020 17:30:26 GMT",
                    "x-amz-server-side-encryption": "AES256",
                    "etag": '"098f6bcd4621d373cade4e832627b4f6"',
                    "content-length": "0",
                    "server": "AmazonS3",
                },
                "RetryAttempts": 0,
            },
            "ETag": '"098f6bcd4621d373cade4e832627b4f6"',
            "ServerSideEncryption": "AES256",
        }

    def get_object(self, Bucket, Key):
        """Fake s3 get_object"""
        # For servicing different test cases
        if "AccessDenied" in Key:
            raise ClientError(self.response_denied, "GetObject")

        if Bucket and Key:
            return self.response_get_success

        raise ParamValidationError(
            report="Invalid length for parameter Key, value: 0, valid range: 1-inf"
        )

    def put_object(self, Bucket, Key, Body):
        """Fake s3 put_object"""
        if "AccessDenied" in Key:
            raise ClientError(self.response_denied, "PutObject")

        if Bucket and Key and Body:
            return self.response_put_success

        # For different test cases
        raise ParamValidationError(
            report="Invalid length for parameter Key, value: 0, valid range: 1-inf"
        )


@pytest.fixture(scope="class")
def instance():
    """Run once per instantiation of class"""
    boto3.client = MockBoto3Client
    return rabbit.EDM()

class TestEDM:
    """Test EDM Base Class"""

    def test_edm(self):
        rabbit.EDM()