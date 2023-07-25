from pg8000 import VARCHAR
from requests import patch
from src.ingestion_lambda.utils.get_address_add import get_address_add
from unittest import mock, result
import pg8000.native
import datetime
import pytest
from unittest.mock import patch

mock_return = [
    [1, '6826 Herzog Via', None, 'Avon', 'New Patienceburgh', '28441',
     'Turkey', '1803 637401', datetime.datetime(
         2023, 7, 25, 15, 20, 49, 962000),
     datetime.datetime(2023, 7, 25, 15, 20, 49, 962000)],
    [2, '1234 Calle Norte', None, 'North District', 'Madrid', '112250',
     'Spain', '0123 14567', datetime.datetime(
         2019, 11, 3, 14, 20, 49, 962000),
     datetime.datetime(
         2019, 11, 3, 14, 20, 49, 962000)]]

mock_search_interval = datetime.datetime(2020, 7, 25, 15, 20, 49, 962000)


def test_get_address_add_returns_list():
    # test that we are returning a list,
    result = get_address_add()
    # print(result)
    assert isinstance(result, list)


def test_get_address_add_returns_correct_keys():
    # test that we have the correct keys
    expected_keys = {
        "location_id", "address_line_1", "address_line_2", "district", "city",
        "postal_code", "country", "phone"}
    result = get_address_add()
    assert all(set(item.keys()) == expected_keys for item in result)


def test_get_address_add_has_correct_value_types():
    # test that each key has the correct type value
    result = get_address_add()
    for item in result:
        assert isinstance(item['location_id'], int)
        assert isinstance(item['address_line_1'], str)
        assert isinstance(item['address_line_2'],
                          str) or item['address_line_2'] is None
        assert isinstance(item['district'], str) or item['district'] is None
        assert isinstance(item['city'], str)
        assert isinstance(item['postal_code'], str)
        assert isinstance(item['country'], str)
        assert isinstance(item['phone'], str)


def test_get_address_add_returns_only_results_within_created_by_interval():
    with patch('pg8000.native.Connection.run', return_value=mock_return):
        result = get_address_add(search_interval=mock_search_interval)
        print(result)
        assert result == [
            {'location_id': 1, 'address_line_1': '6826 Herzog Via',
             'address_line_2': None, 'district': 'Avon',
             'city': 'New Patienceburgh', 'postal_code': '28441',
             'country': 'Turkey', 'phone': '1803 637401'}]


# def test_get_address_add_
#  test that add to csv is called

# test that add to update csv is called

# test that error handling when conn to db is not made

# test that it logs to cloudwatch
