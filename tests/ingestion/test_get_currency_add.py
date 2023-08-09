from src.ingestion_lambda.get_currency_add \
    import get_currency_add, MissingRequiredEnvironmentVariables
from unittest.mock import patch
import datetime
import pytest
import os
import pg8000.exceptions
from dotenv import load_dotenv

load_dotenv()

currency_get_last_time_path = 'get_currency_add.get_last_time'


class Test_Ingestion_Currency:
    def test_get_currency_add_returns_list_with_correct_keys(self):
        """
        Test that the 'get_currency_add' function returns a list of
        dictionaries with the correct keys for currency data.

        It uses mocking to simulate the behavior of the 'get_last_time'
        function.
        """
        with patch(currency_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )

            result = get_currency_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

            assert isinstance(result, list)
            expected_keys = {
                "currency_id", "currency_code", "created_at", "last_updated"}
            assert all(set(item.keys()) == expected_keys for item in result)

    def test_get_currency_add_has_correct_value_types(self):
        """
        Test that the 'get_currency_add' function returns currency data with
        the correct value types.

        It uses mocking to simulate the behavior of the 'get_last_time'
        function.
        """
        with patch(currency_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
            result = get_currency_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

            for item in result:
                assert isinstance(item['currency_id'], int)
                assert isinstance(item['currency_code'], str)
                assert isinstance(item['created_at'], datetime.date)
                assert isinstance(item['last_updated'], datetime.date)

    def test_get_currency_add_calls_get_last_time(self):
        """
        Test that the 'get_currency_add' function calls 'get_last_time'
        exactly once.

        It uses mocking to simulate the behavior of the 'get_last_time'
        function.
        """
        with patch(currency_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
            get_currency_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))
            assert mock_get_last_time.call_count == 1

    def test_database_error(self):
        """
        Test that the 'get_currency_add' function raises an exception with
        "Database error" message when a DatabaseError occurs during the
        connection attempt.

        It uses mocking to simulate the behavior of the
        'pg8000.native.Connection' class and raises a
        pg8000.exceptions.DatabaseError.
        """
        with patch('pg8000.native.Connection') as mock_connection:
            mock_connection.side_effect = pg8000.exceptions.DatabaseError(
                "Database error")
            with pytest.raises(Exception, match="Database error"):
                get_currency_add(
                    db_user=os.environ.get("TEST_SOURCE_USER"),
                    db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                    db_host=os.environ.get("TEST_SOURCE_HOST"),
                    db_port=os.environ.get("TEST_SOURCE_PORT"),
                    db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

    def test_missing_environment_variables(self):
        """
        Test that the 'get_currency_add' function raises 'Missing Required
        Environment Variables' exception when any of the required environment
        variables (db_user, db_database, db_host, db_port, db_password) is
        missing or empty.

        It uses mocking to simulate the behavior of the 'os.environ' dictionary
        and sets it to an empty dictionary.
        """
        with patch('os.environ', {}):
            with pytest.raises(MissingRequiredEnvironmentVariables):
                get_currency_add(
                    db_user=os.environ.get("test_user"),
                    db_database=os.environ.get("test_database"),
                    db_host=os.environ.get('test_host'),
                    db_port=os.environ.get("test_port"),
                    db_password=os.environ.get("test_password"))

    def test_correct_data_returned_by_query(self):
        """
        Test that the 'get_currency_add' function returns the correct currency
        data when querying data created in the last search interval.

        It uses mocking to simulate the behavior of the 'get_last_time'
        function.
        """
        with patch(currency_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2023-07-29 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_currency_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

            assert result == [
                {'currency_id': 3, 'currency_code': 'THB',
                 'created_at': datetime.datetime(
                    2023, 7, 31, 12, 1, 49, 175474),
                 'last_updated': datetime.datetime(
                    2023, 7, 31, 12, 1, 49, 175474)}]
