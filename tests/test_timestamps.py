from cessoc import timestamps


def test_createUpdateExpression():
    values = {"key1": True, "key2": "key1", "key3": False}
    answer = "SET  key1=:key1, key2=:key2, key3=:key3"
    assert answer == timestamps._createUpdateExpression(values)


def test_createExpressionAttributeValues():
    values = {"key1": True, "key2": "key1", "key3": False}
    answer = {":key1": True, ":key2": "key1", ":key3": False}
    assert answer == timestamps._createExpressionAttributeValues(values)
