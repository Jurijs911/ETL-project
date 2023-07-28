
from src.ingestion_lambda.get_address_add import get_address_add, MissingRequiredEnvironmentVariables
import pytest
from unittest.mock import patch
import datetime
import pg8000.exceptions

ingestion_utils_path = "src.ingestion_lambda."
address_get_last_time_path = "get_address_add.get_last_time"
get_last_time_patch_path = ingestion_utils_path + address_get_last_time_path


def test_get_address_add_returns_list():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        result = get_address_add()
        assert isinstance(result, list)


def test_get_address_add_returns_correct_keys():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        expected_keys = {
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
            "created_at",
            "last_updated",
        }
        result = get_address_add()
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_address_add_has_correct_value_types():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        result = get_address_add()
        for item in result:
            assert isinstance(item["location_id"], int)
            assert isinstance(item["address_line_1"], str)
            assert (
                isinstance(item["address_line_2"], str)
                or item["address_line_2"] is None
            )
            assert (
                isinstance(item["district"], str) or item["district"] is None
            )
            assert isinstance(item["city"], str)
            assert isinstance(item["postal_code"], str)
            assert isinstance(item["country"], str)
            assert isinstance(item["phone"], str)
            assert isinstance(item["created_at"], datetime.date)
            assert isinstance(item["last_updated"], datetime.date)


def test_get_address_calls_get_last_time():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        get_address_add()
        assert mock_get_last_time.call_count == 1


def test_database_error():
    with patch('pg8000.native.Connection') as mock_connection:
        mock_connection.side_effect = pg8000.exceptions.DatabaseError(
            "Database error")
        with pytest.raises(Exception, match="Database error"):
            get_address_add()


def test_missing_environment_variables():
    with patch('os.environ', {}):
        with pytest.raises(MissingRequiredEnvironmentVariables):
            get_address_add()


# def test_no_data_returned_by_query():
#     with patch('pg8000.native.Connection') as mock_connection:
#         mock_connection.return_value.run.return_value = []
#         with pytest.raises(Exception, match="No data returned by query"):
#             get_address_add()


def test_correct_data_returned_by_query():
    with patch('pg8000.native.Connection') as mock_connection:
        mock_connection.return_value.run.return_value = [
            [1, '6826 Herzog Via', None, 'Avon', 'New Patienceburgh',
             '28441', 'Turkey', '1803 637401',
             datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
             datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]
        ]
        result = get_address_add()
        assert result == [
            {'location_id': 1, 'address_line_1': '6826 Herzog Via',
             'address_line_2': None, 'district': 'Avon', 'city': 'New Patienceburgh',
             'postal_code': '28441', 'country': 'Turkey', 'phone': '1803 637401',
             'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
             'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)}
        ]


def test_calls_query_once():
    with patch('pg8000.native.Connection') as mock_connection:
        with patch(get_last_time_patch_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            get_address_add()
            mock_connection.return_value.run.assert_called_once_with(
                'SELECT * FROM address WHERE created_at > :search_interval;',
                search_interval=datetime.datetime.strptime(
                    '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f'
                )
            )
