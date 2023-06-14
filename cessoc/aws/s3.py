import logging
from typing import Optional
import boto3
from botocore.exceptions import ClientError


def write(
    key: str,
    bucket: str,
    body: str,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region_name: Optional[str] = "us-west-2",
) -> None:
    """
    S3 PUT operator for base class

    :param key: the path + name of file to be accessed
    :param bucket: the bucket containing the file
    :param body: body of file to be written
    :param access_key: AWS access key id for bucket call
    :param secret_key: AWS secret key matching access key
    :param region_name: Region name of bucket (if needed)

    :raises ClientError: on boto3 python sdk error
    """
    logging.debug("Putting data to %s in s3 bucket %s", key, bucket)
    if access_key and secret_key:
        client = boto3.client(
            "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name
        )
    else:
        client = boto3.client("s3", region_name=region_name)
    try:
        response = client.put_object(Key=key, Bucket=bucket, Body=body)
        logging.debug(response)
    except ClientError as ex:
        logging.error("Could not put to s3: %s", ex)
        raise ex
        
        
def read(
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
        return response["Body"].read().decode("utf-8")
