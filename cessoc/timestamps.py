import os
import boto3


class ItemNotFoundException(Exception):
    """Raised when the "Item" attribute is not present on a get response from DynamoDB"""
    pass


def get(key: str) -> dict:
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table("cessoc-timestamps-" + os.environ["STAGE"])
    response = table.get_item(
        Key={"key": key}
    )
    if "Item" in response:
        return response["Item"]
    raise ItemNotFoundException("Could not find \"Item\" within dynamodb response.")


def put(key: str, values: dict) -> None:
    """Inserts a key. Will overwrite everything at the key if the key exists."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table("cessoc-timestamps-" + os.environ["STAGE"])
    values["key"] = key # Adding the key to the request
    table.put_item(
        Item=values
    )


def update(key: str, values: dict) -> None:
    """Updates individual columns at the key value. Will insert if the key does not exist."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table("cessoc-timestamps-" + os.environ["STAGE"])
    table.update_item(
        Key={"key": key},
        UpdateExpression=_createUpdateExpression(values),
        ExpressionAttributeValues=_createExpressionAttributeValues(values)
    )


def _createUpdateExpression(values: dict) -> str:
    return_val = "SET "
    for item in values:
        return_val += " " + item + "=:" + item + ","
    return_val = return_val[:-1] # Trim the last comma off
    return return_val


def _createExpressionAttributeValues(values: dict) -> dict:
    return_val = {}
    for item in values:
        return_val[":" + item] = values[item]
    return return_val
