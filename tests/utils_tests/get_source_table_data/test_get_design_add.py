from decimal import Decimal
from src.ingestion_lambda.get_design_add import get_design_add
from unittest.mock import patch, Mock
from unittest import mock
import pg8000.native
import datetime
import pytest


def test_get_design_add_returns_list():
    with patch('src.ingestion_lambda.get_design_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        # test that we are returning a list,
        result = get_design_add()

        assert isinstance(result, list)


def test_get_design_add_returns_correct_keys():
    with patch('src.ingestion_lambda.get_design_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        # test that we have the correct keys
        expected_keys = {
            "design_id",
            "created_at",
            "last_updated",
            "design_name",
            "file_location",
            "file_name"
        }
        result = get_design_add()
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_design_add_has_correct_value_types():
    with patch('src.ingestion_lambda.get_design_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
    # test that each key has the correct type value
        result = get_design_add()
        for item in result:
            assert isinstance(item['design_id'], int)
            assert isinstance(item['created_at'], datetime.date)
            assert isinstance(item['last_updated'], datetime.date)
            assert isinstance(item['design_name'], str)
            assert isinstance(item['file_location'], str)
            assert isinstance(item['file_name'], str)


def test_get_design_add_calls_get_last_time():
    # test that the SQL query calls get_last_time()
    with patch('src.ingestion_lambda.get_design_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        get_design_add()
        assert mock_get_last_time.call_count == 1
