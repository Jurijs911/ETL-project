from src.ingestion_lambda.get_staff_add \
    import get_staff_add, MissingRequiredEnvironmentVariables
from unittest.mock import patch
import os
import datetime
import pytest
import pg8000.exceptions
from dotenv import load_dotenv

load_dotenv()


staff_get_last_time_path = "get_staff_add.get_last_time"


class Test_Ingestion_Staff:
    def test_get_staff_add_returns_list_with_correct_keys():
        """
        Test whether the 'get_staff_add' function returns a list of
        dictionaries with the correct keys for the 'staff' table.

        The test patches the 'get_last_time' function to provide a date as the
        search interval. The function is then called with test environment
        variables to connect to the test database. The retrieved data is
        verified to contain dictionaries with the expected keys.
        """
        with patch(staff_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_staff_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

            assert isinstance(result, list)
            expected_keys = {
                "staff_id",
                "first_name",
                "last_name",
                "department_id",
                "email_address",
                "created_at",
                "last_updated",
            }
            assert all(set(item.keys()) == expected_keys for item in result)

    def test_get_staff_add_has_correct_value_types():
        """
        Test whether the 'get_staff_add' function returns data with
        correct value types for the 'staff' table.

        The test patches the 'get_last_time' function to provide a date as the
        search interval. The function is called with test environment variables
        to connect to the test database. Test then checks the retrieved data
        contains dictionaries with the expected value types for each key.
        """
        with patch(staff_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_staff_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))
            for item in result:
                assert isinstance(item["staff_id"], int)
                assert isinstance(item["first_name"], str)
                assert isinstance(item["last_name"], str)
                assert isinstance(item["department_id"], int)
                assert isinstance(item["email_address"], str)
                assert isinstance(item["created_at"], datetime.date)
                assert isinstance(item["last_updated"], datetime.date)

    def test_get_staff_add_calls_get_last_time():
        """
        Test whether the 'get_staff_add' function calls the
        'get_last_time' function.

        The test patches the 'get_last_time' function to provide a fixed date
        as the search interval. The function is called with test environment
        variables to connect to the test database.
        """
        with patch(staff_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            get_staff_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))
            assert mock_get_last_time.call_count == 1

    def test_database_error():
        """
        Test whether the 'get_staff_add' function raises an exception
        for a database error.

        The test patches the database connection to raise a 'DatabaseError'
        exception. The function is then called with test environment variables
        to connect to the test database.
        """
        with patch('pg8000.native.Connection') as mock_connection:
            mock_connection.side_effect = pg8000.exceptions.DatabaseError(
                "Database error")
            with pytest.raises(Exception, match="Database error"):
                get_staff_add(
                    db_user=os.environ.get("TEST_SOURCE_USER"),
                    db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                    db_host=os.environ.get("TEST_SOURCE_HOST"),
                    db_port=os.environ.get("TEST_SOURCE_PORT"),
                    db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

    def test_missing_environment_variables():
        """
        Test whether the 'get_staff_add' function raises an exception
        for missing environment variables.

        The test patches the 'os.environ' dictionary to simulate missing
        environment variables. The function is called with test environment
        variables to connect to the test database.
        """
        with patch('os.environ', {}):
            with pytest.raises(MissingRequiredEnvironmentVariables):
                get_staff_add(db_user=os.environ.get("test_user"),
                              db_database=os.environ.get("test_database"),
                              db_host=os.environ.get('test_host'),
                              db_port=os.environ.get("test_port"),
                              db_password=os.environ.get("test_password"))

    def test_correct_data_returned_by_query():
        """
        Test whether the 'get_staff_add' function returns the correct
        data for the 'staff' table.

        The test patches the 'get_last_time' function to provide a date as the
        search interval. The function is then called with test environment
        variables to connect to the test database.
        """
        with patch(staff_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2023-07-27 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_staff_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))
            assert result == [
                {'staff_id': 1,
                 'first_name': 'Paul',
                 'last_name': 'McCartney',
                 'department_id': 1,
                 'email_address': 'paul@northcoders.com',
                 'created_at': datetime.datetime(
                    2023, 7, 28, 15, 1, 52, 760464),
                 'last_updated': datetime.datetime(
                    2023, 7, 28, 15, 1, 52, 760464)},
                {'staff_id': 2,
                 'first_name': 'Ringo',
                 'last_name': 'Starr',
                 'department_id': 2,
                 'email_address': 'ringo@northcoders.com',
                 'created_at': datetime.datetime(
                    2023, 7, 28, 15, 2, 21, 393482),
                 'last_updated': datetime.datetime(
                    2023, 7, 28, 15, 2, 21, 393482)}]
