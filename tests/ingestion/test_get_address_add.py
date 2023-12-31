import os
from src.ingestion_lambda.get_address_add import (
    get_address_add,
    MissingRequiredEnvironmentVariables,
)
import pytest
from unittest.mock import patch
import datetime
import pg8000.exceptions
from dotenv import load_dotenv

load_dotenv()

address_get_last_time_path = (
    "src.ingestion_lambda.get_address_add.get_last_time"
)


class Test_Ingestion_Address:
    def test_get_address_add_returns_list_with_correct_keys(self):
        """
        Test to check the get_address_add function returns a list of
        dictionaries with the expected keys for each item.

        It mocks the get_last_time function to simulate a specific time and
        then calls get_address_add with environment variables to connect to
        the database.
        """
        with patch(address_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )

            result = get_address_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )

            assert isinstance(result, list)
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
            assert all(set(item.keys()) == expected_keys for item in result)

    def test_get_address_add_has_correct_value_types(self):
        """
        Test whether the get_address_add function returns data with correct
        value types.

        It mocks the get_last_time function to simulate a specific time and
        then calls get_address_add with environment variables to connect to
        the database.
        """
        with patch(address_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_address_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )
            for item in result:
                assert isinstance(item["location_id"], int)
                assert isinstance(item["address_line_1"], str)
                assert (
                    isinstance(item["address_line_2"], str)
                    or item["address_line_2"] is None
                )
                assert (
                    isinstance(item["district"], str)
                    or item["district"] is None
                )
                assert isinstance(item["city"], str)
                assert isinstance(item["postal_code"], str)
                assert isinstance(item["country"], str)
                assert isinstance(item["phone"], str)
                assert isinstance(item["created_at"], datetime.date)
                assert isinstance(item["last_updated"], datetime.date)

    def test_get_address_calls_get_last_time(self):
        """
        Test whether the get_address_add function calls the get_last_time
        function.

        It mocks the get_last_time function to simulate a specific time and
        then calls get_address_add with environment variables to connect to
        the database.
        """
        with patch(address_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            get_address_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )
            assert mock_get_last_time.call_count == 1

    def test_database_error(self):
        """
        Test whether the get_address_add function raises an exception on
        database error.

        It mocks the pg8000.native.Connection to raise a DatabaseError,and
        then calls get_address_add with environment variables to connect
        to the database.
        """
        with patch("pg8000.native.Connection") as mock_connection:
            mock_connection.side_effect = pg8000.exceptions.DatabaseError(
                "Database error"
            )
            with pytest.raises(Exception, match="Database error"):
                get_address_add(
                    db_user=os.environ.get("TEST_SOURCE_USER"),
                    db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                    db_host=os.environ.get("TEST_SOURCE_HOST"),
                    db_port=os.environ.get("TEST_SOURCE_PORT"),
                    db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
                )

    def test_missing_environment_variables(self):
        """
        Test whether the get_address_add function raises the exception
        MissingRequiredEnvironmentVariables when there is missing environment
        variables.

        It mocks the os.environ dictionary with empty values and then calls
        get_address_add with missing environment variables.
        """
        with patch("os.environ", {}):
            with pytest.raises(MissingRequiredEnvironmentVariables):
                get_address_add(
                    db_user=os.environ.get("test_user"),
                    db_database=os.environ.get("test_database"),
                    db_host=os.environ.get("test_host"),
                    db_port=os.environ.get("test_port"),
                    db_password=os.environ.get("test_password"),
                )

    def test_correct_data_returned_by_query(self):
        """
        Test whether the get_address_add function returns the correct data
        based on the query.

        It mocks the get_last_time function to simulate a specific time and
        then calls get_address_add with environment variables to connect to
        the database.
        """
        with patch(address_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2023-07-29 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_address_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )
            assert result == [
                {
                    "location_id": 3,
                    "address_line_1": "Bank of England",
                    "address_line_2": "Threadneedle St",
                    "district": "",
                    "city": "London",
                    "postal_code": "EC2R 8AH",
                    "country": "UK",
                    "phone": "02034614444",
                    "created_at": datetime.datetime(
                        2023, 7, 30, 14, 7, 32, 362337
                    ),
                    "last_updated": datetime.datetime(
                        2023, 7, 30, 14, 7, 32, 362337
                    ),
                }
            ]
