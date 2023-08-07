from get_design_add \
    import get_design_add, MissingRequiredEnvironmentVariables
from unittest.mock import patch
import datetime
import pytest
import os
import pg8000.exceptions
from dotenv import load_dotenv

load_dotenv()


design_get_last_time_path = 'get_design_add.get_last_time'


class Test_Ingestion_Design:
    def test_get_design_add_returns_list_with_correct_keys():
        with patch(design_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )

            result = get_design_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

            assert isinstance(result, list)
            expected_keys = {
                "design_id",
                "created_at",
                "last_updated",
                "design_name",
                "file_location",
                "file_name"
            }
            assert all(set(item.keys()) == expected_keys for item in result)

    def test_get_design_add_has_correct_value_types():
        with patch(design_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-30 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_design_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

            for item in result:
                assert isinstance(item['design_id'], int)
                assert isinstance(item['created_at'], datetime.date)
                assert isinstance(item['last_updated'], datetime.date)
                assert isinstance(item['design_name'], str)
                assert isinstance(item['file_location'], str)
                assert isinstance(item['file_name'], str)

    def test_get_design_add_calls_get_last_time():
        with patch(design_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            get_design_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

            assert mock_get_last_time.call_count == 1

    def test_database_error():
        with patch('pg8000.native.Connection') as mock_connection:
            mock_connection.side_effect = pg8000.exceptions.DatabaseError(
                "Database error")
            with pytest.raises(Exception, match="Database error"):
                get_design_add(
                    db_user=os.environ.get("TEST_SOURCE_USER"),
                    db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                    db_host=os.environ.get("TEST_SOURCE_HOST"),
                    db_port=os.environ.get("TEST_SOURCE_PORT"),
                    db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

    def test_missing_environment_variables():
        with patch('os.environ', {}):
            with pytest.raises(MissingRequiredEnvironmentVariables):
                get_design_add(
                    db_user=os.environ.get("test_user"),
                    db_database=os.environ.get("test_database"),
                    db_host=os.environ.get('test_host'),
                    db_port=os.environ.get("test_port"),
                    db_password=os.environ.get("test_password"))

    def test_correct_data_returned_by_query():
        with patch(design_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2023-07-29 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_design_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

            assert result == [
                {'design_id': 2, 'created_at': datetime.datetime(
                    2023, 8, 1, 12, 23, 35, 315112),
                 'last_updated': datetime.datetime(
                    2023, 8, 1, 12, 23, 35, 315112),
                 'design_name': 'Invisible', 'file_location': 'concept',
                 'file_name': 'invisible_tote'}]
