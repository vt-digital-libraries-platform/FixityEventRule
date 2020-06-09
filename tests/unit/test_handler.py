import json

import pytest

# from hello_world import app
from fixitycheck import lambda_function

@pytest.fixture()
def test_event():

    """ Generates Example Lambda Event"""
    return {
        "body": '{ "test": "body"}'
    }


def test_lambda_handler(test_event):

    ret = lambda_function.lambda_handler(test_event, "")
    data = json.loads(ret["body"])

    assert ret["statusCode"] == 200
    assert "message" in ret["body"]
    assert data["message"] == "hello world"
