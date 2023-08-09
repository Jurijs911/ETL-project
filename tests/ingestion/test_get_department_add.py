from src.ingestion_lambda.get_department_add import (
    get_department_add,
    MissingRequiredEnvironmentVariables,
)
from unittest.mock import patch
import datetime
import pytest
import os
import pg8000.exceptions
from dotenv import load_dotenv

load_dotenv()


department_get_last_time_path = (
    "src.ingestion_lambda.get_department_add.get_last_time"
)


class Test_Ingestion_Departments:
    def test_get_department_add_returns_list_with_correct_keys(self):
        """
        Test if get_department_add() returns a list of dictionaries with
        correct keys.

        It mocks the get_last_time function to return a datetime value. Then,
        it calls get_department_add() with test environment variables and
        asserts that the result is a list of dictionaries with expected keys.
        """
        with patch(department_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )

            result = get_department_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )

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

    def test_get_department_add_has_correct_value_types(self):
        """
        Test if get_department_add() returns data with correct value types.

        It mocks the get_last_time function to return a datetime value. Then,
        it calls get_department_add() with test environment variables and
        asserts that each item in the result has the correct value types.
        """
        with patch(department_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_department_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )

            for item in result:
                assert isinstance(item["department_id"], int)
                assert isinstance(item["department_name"], str)
                assert (
                    isinstance(item["location"], str)
                    or item["location"] is None
                )
                assert (
                    isinstance(item["manager"], str) or item["manager"] is None
                )
                assert isinstance(item["created_at"], datetime.date)
                assert isinstance(item["last_updated"], datetime.date)

    def test_get_department_add_calls_get_last_time(self):
        """
        Test if get_department_add() calls get_last_time function.

        It mocks the get_last_time function to return a datetime value. Then,
        it calls get_department_add() with test environment variables and
        asserts that get_last_time function is called once.
        """
        with patch(department_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            get_department_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )

            assert mock_get_last_time.call_count == 1

    def test_database_error(self):
        """
        Test if get_department_add() raises an Exception when there's a
        DatabaseError.

        It mocks the pg8000.native.Connection to raise a DatabaseError.
        Then, it calls get_department_add() with test environment variables and
        asserts that it raises an Exception with "Database error" message.
        """
        with patch("pg8000.native.Connection") as mock_connection:
            mock_connection.side_effect = pg8000.exceptions.DatabaseError(
                "Database error"
            )
            with pytest.raises(Exception, match="Database error"):
                get_department_add(
                    db_user=os.environ.get("TEST_SOURCE_USER"),
                    db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                    db_host=os.environ.get("TEST_SOURCE_HOST"),
                    db_port=os.environ.get("TEST_SOURCE_PORT"),
                    db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
                )

    def test_missing_environment_variables(self):
        """
        Test if get_department_add() raises 'Missing Required Environment
        Variables' when missing environment variables.

        It mocks the os.environ to return an empty dictionary. Then, it calls
        get_department_add() with test environment variables and asserts that
        it raises MissingRequiredEnvironmentVariables.
        """
        with patch("os.environ", {}):
            with pytest.raises(MissingRequiredEnvironmentVariables):
                get_department_add(
                    db_user=os.environ.get("test_user"),
                    db_database=os.environ.get("test_database"),
                    db_host=os.environ.get("test_host"),
                    db_port=os.environ.get("test_port"),
                    db_password=os.environ.get("test_password"),
                )

    def test_correct_data_returned_by_query(self):
        """
        Tests if get_department_add() returns the correct data based on the
        query.

        It mocks the get_last_time function to return a fixed datetime value.
        Then, it calls get_department_add() with test environment variables and
        asserts that the result is as expected.
        """
        with patch(department_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2023-07-29 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_department_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )

            assert result == [
                {
                    "department_id": 4,
                    "department_name": "Coats",
                    "location": "3rd Floor",
                    "manager": "Nigel",
                    "created_at": datetime.datetime(
                        2023, 7, 31, 16, 11, 1, 427541
                    ),
                    "last_updated": datetime.datetime(
                        2023, 7, 31, 16, 11, 1, 427541
                    ),
                }
            ]
