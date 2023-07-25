from src.ingestion_lambda.utils.get_address import get_address
from unittest import mock
import pg8000.native
import datetime
import pytest



@mock.patch('pg8000.native.Connection')
def test_get_address(mock_conn):
    mock_cursor = mock_conn.return_value.cursor.return_value
    mock_cursor.fetchall.return_value = [
        (1, '6826 Herzog Via', None, 'Avon', 'New Patienceburgh', '28441', 'Turkey', '1803 637401')]
    result = get_address()
    assert result == [{'location_id': 1, 'address_line_1': '6826 Herzog Via', 'address_line_2': None, 'district': 'Avon', 'city': 'New Patienceburgh', 'postal_code': '28441', 'country': 'Turkey', 'phone': '1803 637401'}
]




# test that we are returning a list, 

# test that we have the correct keys

# test that each key has the correct type value

# test that add to csv is called 

# test that add to update csv is called 

# test that error hadnling when conn to db is not made

# test that it logs to cloudwatch