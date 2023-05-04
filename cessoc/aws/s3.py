import logging
import json
from abc import ABC, abstractmethod
from typing import Dict, List, Union, Optional
import boto3
from botocore.exceptions import ClientError

print("s3 imported")


def write_s3(
    data: Union[List, Dict, str, bytes],
    etl_destination,
    intel_file,
) -> None:
    """
    Writes data to an object to etl_destination (config.json) S3 bucket in the path specified by intel_file (config.json)
    Data must be a dictionary

    :param data: A collection of data objects to write to S3
    :param etl_destination: The S3 bucket which will receive the data
    :param intel_file: The S3 bucket key or file path with file name

    :raises TypeError: if the data is not of type list, dict, or str
    :raises KeyError: if the config.json does not include the required fields
    :raises ClientError: in the case where writing to the bucket failed (e.g. Access Denied, No Such Bucket)
    :raises Exception: if for any other reason the write to S3 fails
    """
    if not isinstance(data, (list, dict, str, bytes)):
        raise TypeError(
            f"Data to write to S3 must be of type 'list', 'dict', 'str', or 'bytes' not '{type(data)}'"
        )
    
    try:
        # Check for function parameter overrides for bucket name
        bucket_name = (
            etl_destination
        )
        # Check for function parameter overrides for file path + filename
        file_path = intel_file
    except KeyError as ex:
        raise KeyError("Missing config.json parameter") from ex

    s3_resource = boto3.resource("s3")

    try:
        if isinstance(data, (list, dict)):
            encoded_data = json.dumps(data).encode()
        elif isinstance(data, str):
            encoded_data = data.encode()
        else:
            encoded_data = data
        res = s3_resource.Object(bucket_name, file_path)
        res = res.put(Body=encoded_data)
    except ClientError as ex:
        raise Exception("Unable to write to S3 ETL Bucket") from ex
    except Exception as ex:
        raise Exception(
            "An Exception occurred while attempting to write to S3"
        ) from ex
        
def get_s3(
    key: str,
    bucket: str,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region_name: Optional[str] = "us-west-2",
) -> bytes:
    """
    S3 GET operator for base class

    :param key: the path + name of file to be accessed
    :param bucket: the bucket containing the file
    :param access_key: AWS access key id for bucket call
    :param secret_key: AWS secret key matching access key
    :param region_name: Region name of bucket (if needed)

    :raises ClientError: on boto3 python sdk error

    :returns: Bucket data as string or as bytes.
    """
    logging.debug("Accessing %s in s3 bucket %s", key, bucket)
    if access_key and secret_key:
        client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
        )
    else:
        client = boto3.client("s3", region_name=region_name)
    try:
        response = client.get_object(Key=key, Bucket=bucket)
        logging.debug(response)
    except ClientError as ex:
        logging.error("Could not get from S3: %s", ex)
        raise ex
    else:
        return response["Body"].read()
