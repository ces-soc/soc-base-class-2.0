"""
Provides methods to get information from IAM in AWS
"""
import boto3
from typing import Optional
from cessoc.aws import ssm


def getRDSToken(DBUsername: str, DBHostname: Optional[str] = None, Port: Optional[int] = 5432, Region: Optional[str] = "us-west-2") -> str:
    """Gets an IAM token"""
    session = boto3.Session()
    client = session.client('rds')
    if DBHostname is None:
        DBHostname = ssm.get_value("/ces/data_store/rds_host")
    return client.generate_db_auth_token(DBHostname=DBHostname, Port=Port, DBUsername=DBUsername, Region=Region)
