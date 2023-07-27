from pg8000 import VARCHAR
from requests import patch
from src.ingestion_lambda.utils.get_staff_add import get_staff_add
from unittest.mock import patch, Mock
from unittest import mock
import pg8000.native
import datetime
import pytest
from decimal import Decimal


def test_get_staff_add_returns_list():
    with patch('src.ingestion_lambda.utils.get_staff_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        # test that we are returning a list,
        result = get_staff_add()
        # print(result)
        assert isinstance(result, list)


def test_gget_staff_add_returns_correct_keys():
    with patch('src.ingestion_lambda.utils.get_staff_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        # test that we have the correct keys
        expected_keys = {
            "staff_id", "first_name", "last_name", "department_id", "email_address", "created_at", "last_updated"}
        result = get_staff_add()
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_staff_add_has_correct_value_types():
    with patch('src.ingestion_lambda.utils.get_staff_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
    # test that each key has the correct type value
        result = get_staff_add()
        for item in result:
            assert isinstance(item['staff_id'], int)
            assert isinstance(item['first_name'], str)
            assert isinstance(item['last_name'], str)
            assert isinstance(item['department_id'], int)
            assert isinstance(item['email_address'], str)
            assert isinstance(item['created_at'], datetime.date)
            assert isinstance(item['last_updated'], datetime.date)



def test_get_staff_add_calls_get_last_time():
    # test that the SQL query calls get_last_time()
    with patch('src.ingestion_lambda.utils.get_staff_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        get_staff_add()
        assert mock_get_last_time.call_count == 1
