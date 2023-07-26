from pg8000 import VARCHAR
from requests import patch
from src.ingestion_lambda.utils.get_address_add import get_address_add
from unittest.mock import patch, Mock
from unittest import mock
import pg8000.native
import datetime
import pytest


# mock_return = [
#     [1, '6826 Herzog Via', None, 'Avon', 'New Patienceburgh', '28441',
#      'Turkey', '1803 637401', datetime.datetime(
#          2023, 7, 25, 15, 20, 49, 962000),
#      datetime.datetime(2023, 7, 25, 15, 20, 49, 962000)],
#     [2, '1234 Calle Norte', None, 'North District', 'Madrid', '112250',
#      'Spain', '0123 14567', datetime.datetime(
#          2019, 11, 3, 14, 20, 49, 962000),
#      datetime.datetime(
#          2019, 11, 3, 14, 20, 49, 962000)]]

# # mock_search_interval = datetime.datetime('2020-07-25 15:20:49.962000')
# # dt_formatted = five_mins_ago.strftime('%Y, %m, %d, %H, %M, %S, %f')
# mock_search_interval = datetime.datetime.strptime(
#     '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')

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


def test_get_address_calls_get_last_time():
    #test that the SQL query calls get_last_time()
    with patch('src.ingestion_lambda.utils.get_address_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
        '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        get_address_add()
        assert mock_get_last_time.call_count == 1