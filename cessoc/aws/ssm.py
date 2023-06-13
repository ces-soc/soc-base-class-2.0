import logging
from typing import Dict, Optional
import boto3
from botocore.exceptions import ClientError


def get_value(
    path: str,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region: str = "us-west-2",
) -> str:
    """
    Gets a single str value from the Parameter Store

    :param path: Name of the path to the SSM parameter
    :param access_key: AWS Access Key (Override in case default credentials don't have permissions to resource)
    :param secret_key: AWS Secret Key (Override in case default credentials don't have permissions to resource)
    :param region: Default region in which to instantiate client

    :returns: The specified str
    """

    if access_key and secret_key:
        # For cases in which SSM parameters are in a different account
        logging.debug("AWS Auth keys present in get_value signature")
        ssm = boto3.client(
            "ssm",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
        return ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"]["Value"]
    else:
        # Otherwise use the default available keys
        logging.debug("Auth keys not preset, using default AWS keys")
        ssm = boto3.client("ssm", region_name=region)
        return ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"]["Value"]


def get_parameters_of_path(
    path: str,
    recursive: bool = True,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region: str = "us-west-2",
) -> Dict:
    """
    Gets dictionary of all the parameters under the given path from the Parameter Store.
    Example of a valid path: /etl-template/endpoints (campus is prepended)

    :param path: Name of the path to the SSM parameter
    :param recursive: Retrieve parameters from any sub paths

    :returns: A dict of all parameters and their value under the path
    """

    def _get(ssm: boto3.client, path: str) -> list:
        """
        Get Value using parameter store path

        :param ssm: instantiation of boto3 client for accessing Parameter store
        :param path: The path to the SSM parameters

        :raises ClientError: When we encounter a problem accessing the specified parameter

        :returns: The list of parameters returned from the ssm lookup
        """
        parameters = []
        next_token = ""
        while True:
            if next_token == "":
                res = ssm.get_parameters_by_path(
                    Path=path,
                    Recursive=recursive,
                    WithDecryption=True,
                    MaxResults=10,
                )
            else:
                res = ssm.get_parameters_by_path(
                    Path=path,
                    Recursive=recursive,
                    WithDecryption=True,
                    MaxResults=10,
                    NextToken=next_token,
                )

            parameters.extend(res["Parameters"])

            if "NextToken" not in res:
                return parameters
            next_token = res["NextToken"]

    if access_key and secret_key:
        # For cases in which SSM parameters are in a different account
        logging.debug("AWS Auth keys present in get_value signature")
        ssm = boto3.client(
            "ssm",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
    else:
        # Otherwise use the default available keys
        logging.debug("Auth keys not preset, using default AWS keys")
        ssm = boto3.client("ssm", region_name=region)
    param_dict: Dict[str, str] = {}
    for parameter in _get(ssm, path):
        param_dict[parameter["Name"]] = parameter["Value"]
    return param_dict


def put_value(
    path: str,
    value: str,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    overwrite: bool = True,
    region: str = "us-west-2",
) -> None:
    """
    Puts a single str value to the Parameter Store

    :param path: Parameter Store path (or ssm parameter name)
    :param value: Value to store under the indicated path in the Parameter Store
    :param access_key: Access Key provided in the case that the ETL needs special permissions
    :param secret_key: Secret Key provided in the case that the ETL needs special permissions
    :param overwrite: Overwrite existing parameter values, or if false, ignore call
    :param region: Boto3 credential instantiation requires a default region, defaults to Oregon if not specified

    :raises Exception:
    """

    def _put(ssm: boto3.client, path: str, value: str) -> None:
        """
        Puts a single string value to Parameter store

        :param ssm: Instantiated SSM client
        :param path: Path where value will be stored
        :param value: String value that will be put to Parameter

        :raises TypeError: if value is not of type str
        :raises ClientError: if a problem occurs while writing the data
        """
        if not isinstance(value, str):
            raise TypeError(
                f"Data to write to Parameter Store must be of type 'str' not '{type(value)}'"
            )
        try:
            ssm.put_parameter(
                Name=path, Value=value, Type="String", Overwrite=overwrite
            )
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "ParameterAlreadyExists":
                logging.warning(
                    "put_value item not written. Remove overwrite=False to overwrite existing value"
                )
            else:
                raise ex

    if access_key and secret_key:
        # For cases in which SSM parameters are in a different account
        logging.debug("AWS Auth keys present in put_value signature")
        ssm = boto3.client(
            "ssm",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
        _put(ssm, path, value)
    else:
        # Otherwise use the default available keys
        logging.debug("Auth keys not preset, using default AWS keys")
        ssm = boto3.client("ssm", region_name=region)
        _put(ssm, path, value)

