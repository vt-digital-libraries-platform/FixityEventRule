import json
import pytest

from sharedutils import sharedutils
from datetime import datetime, timedelta


@pytest.fixture()
def test_event():
    pass


def test_getDateFromDay():

    days = 10
    output = sharedutils.getDateFromDay(days)
    expected = datetime.now() - timedelta(days=days)

    assert output == expected.strftime('%Y-%m-%d')


def test_create_steps_task_json():

    inputData = [{'VarCharValue': 'fixity-test'},
                 {'VarCharValue': '00001.tif'}]
    outputBucket = "fixityoutput"
    expected = '{"Bucket": "fixity-test", "Key": "00001.tif", "OutputBucket": "fixityoutput"}'

    output = sharedutils.create_steps_task_json(inputData, outputBucket)
    assert output == expected


def test_list_query_results():

    inputData = [{'VarCharValue': 'fixity-test'},
                 {'VarCharValue': '00001.tif'}]

    expected = '"S3 Bucket": "fixity-test", "Filename": "00001.tif"'

    output = sharedutils.list_query_results(inputData)
    assert output == expected


def test_get_value_from_list():

    inputData = {'VarCharValue': 'test'}

    output = sharedutils.get_value_from_list(inputData)
    assert output == "test"
