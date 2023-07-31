from src.ingestion_lambda.get_department_add \
    import get_department_add
from unittest.mock import patch
import datetime
import pytest
import os
import pg8000.exceptions
from dotenv import load_dotenv

load_dotenv()


ingestion_utils_path = 'src.ingestion_lambda.'
department_get_last_time_path = 'get_department_add.get_last_time'
get_last_time_patch_path = ingestion_utils_path + \
    department_get_last_time_path


def test_get__department_add_returns_list_with_correct_keys():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )

        result = get_department_add(
            db_user=os.environ.get("test_user"),
            db_database=os.environ.get(
                "test_database"),
            db_host=os.environ.get('test_host'),
            db_port=os.environ.get("test_port"),
            db_password=os.environ.get("test_password"))

        assert isinstance(result, list)
        expected_keys = {
            "department_id",
            "department_name",
            "location",
            "manager",
            "created_at",
            "last_updated",
        }
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_department_add_has_correct_value_types():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        result = get_department_add(
            db_user=os.environ.get("test_user"),
            db_database=os.environ.get(
                "test_database"),
            db_host=os.environ.get('test_host'),
            db_port=os.environ.get("test_port"),
            db_password=os.environ.get("test_password"))
        for item in result:
            assert isinstance(item["department_id"], int)
            assert isinstance(item["department_name"], str)
            assert (
                isinstance(item["location"], str) or item["location"] is None
            )
            assert isinstance(item["manager"], str) or item["manager"] is None
            assert isinstance(item["created_at"], datetime.date)
            assert isinstance(item["last_updated"], datetime.date)


def test_get_department_add_calls_get_last_time():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        get_department_add(
            db_user=os.environ.get("test_user"),
            db_database=os.environ.get(
                "test_database"),
            db_host=os.environ.get('test_host'),
            db_port=os.environ.get("test_port"),
            db_password=os.environ.get("test_password"))
        assert mock_get_last_time.call_count == 1


def test_database_error():
    with patch('pg8000.native.Connection') as mock_connection:
        mock_connection.side_effect = pg8000.exceptions.DatabaseError(
            "Database error")
        with pytest.raises(Exception, match="Database error"):
            get_department_add(
                db_user=os.environ.get("test_user"),
                db_database=os.environ.get("test_database"),
                db_host=os.environ.get('test_host'),
                db_port=os.environ.get("test_port"),
                db_password=os.environ.get("test_password"))


# def test_missing_environment_variables():
#     with patch('os.environ', {}):
#         with pytest.raises(MissingRequiredEnvironmentVariables):
#             get_currency_add(
#                 db_user=os.environ.get("test_user"),
#                 db_database=os.environ.get("test_database"),
#                 db_host=os.environ.get('test_host'),
#                 db_port=os.environ.get("test_port"),
#                 db_password=os.environ.get("test_password"))


def test_correct_data_returned_by_query():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2023-07-29 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        result = get_department_add(
            db_user=os.environ.get("test_user"),
            db_database=os.environ.get("test_database"),
            db_host=os.environ.get('test_host'),
            db_port=os.environ.get("test_port"),
            db_password=os.environ.get("test_password"))

        assert result == [
            {'department_id': 4, 'department_name': 'Coats',
             'location': '3rd Floor', 'manager': 'Nigel',
             'created_at': datetime.datetime(
                 2023, 7, 31, 16, 11, 1, 427541),
             'last_updated': datetime.datetime(
                 2023, 7, 31, 16, 11, 1, 427541)}]
