from src.ingestion_lambda.get_counterparty_add import (
    get_counterparty_add,
    MissingRequiredEnvironmentVariables,
)
from unittest.mock import patch
import datetime
import pytest
import os
import pg8000.exceptions
from dotenv import load_dotenv

load_dotenv()


counterparty_get_last_time_path = (
    "src.ingestion_lambda.get_counterparty_add.get_last_time"
)


class Test_Ingestion_Counterparty:
    def test_get_counterparty_add_returns_list_with_correct_keys(self):
        """
        Test to check that the get_counterparty_add function returns a list of
        dictionaries with the correct keys.

        The function connects to the database, retrieves counterparty data
        created or updated within the last search interval, and verifies that
        each dictionary in the result contains all the expected keys.
        """
        with patch(counterparty_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )

            result = get_counterparty_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )

            assert isinstance(result, list)
            expected_keys = {
                "counterparty_id",
                "counterparty_legal_name",
                "counterparty_legal_name",
                "legal_address_id",
                "commercial_contact",
                "delivery_contact",
                "created_at",
                "last_updated",
            }
            assert all(set(item.keys()) == expected_keys for item in result)

    def test_get_counterparty_add_has_correct_value_types(self):
        """
        Test to check that the get_counterparty_add function returns a list of
        dictionaries with correct value types for each key.

        The function connects to the database, retrieves counterparty data
        created or updated within the last search interval, and checks that
        each value in the dictionaries corresponds to the correct data type.
        """
        with patch(counterparty_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_counterparty_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )
            for item in result:
                assert isinstance(item["counterparty_id"], int)
                assert isinstance(item["counterparty_legal_name"], str)
                assert isinstance(item["legal_address_id"], int)
                assert isinstance(item["commercial_contact"], str)
                assert isinstance(item["delivery_contact"], str)
                assert isinstance(item["created_at"], datetime.date)
                assert isinstance(item["last_updated"], datetime.date)

    def test_get_counterparty_add_calls_get_last_time(self):
        """
        Test case to ensure that the get_counterparty_add function calls the
        get_last_time function.

        The function connects to the database, retrieves counterparty data
        created or updated within the last search interval, and verifies that
        the get_last_time function is called exactly once.
        """
        with patch(counterparty_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            get_counterparty_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )
            assert mock_get_last_time.call_count == 1

    def test_database_error(self):
        """
        Test case to ensure that the get_counterparty_add function raises an
        Exception when encountering a DatabaseError.

        The function connects to the database and simulates a pg8000
        DatabaseError to verify that it raises a generic Exception with the
        message "Database error".
        """
        with patch("pg8000.native.Connection") as mock_connection:
            mock_connection.side_effect = pg8000.exceptions.DatabaseError(
                "Database error"
            )
            with pytest.raises(Exception, match="Database error"):
                get_counterparty_add(
                    db_user=os.environ.get("TEST_SOURCE_USER"),
                    db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                    db_host=os.environ.get("TEST_SOURCE_HOST"),
                    db_port=os.environ.get("TEST_SOURCE_PORT"),
                    db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
                )

    def test_missing_environment_variables(self):
        """
        Test case to ensure that the get_counterparty_add function raises a
        MissingRequiredEnvironmentVariables exception when required
        environment variables are missing.

        The function simulates an empty environment via os.environ and verifies
        that it raises the MissingRequiredEnvironmentVariables exception.
        """
        with patch("os.environ", {}):
            with pytest.raises(MissingRequiredEnvironmentVariables):
                get_counterparty_add(
                    db_user=os.environ.get("test_user"),
                    db_database=os.environ.get("test_database"),
                    db_host=os.environ.get("test_host"),
                    db_port=os.environ.get("test_port"),
                    db_password=os.environ.get("test_password"),
                )

    def test_correct_data_returned_by_query(self):
        """
        Test case to ensure that the get_counterparty_add function returns the
        correct data.

        The function connects to the database, retrieves counterparty data
        created or updated within the last search interval, and checks that
        the returned data matches the expected data.
        """
        with patch(counterparty_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2023-07-29 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_counterparty_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )

            assert result == [
                {
                    "counterparty_id": 2,
                    "counterparty_legal_name": "Harris and Sons Ltd",
                    "legal_address_id": 2,
                    "commercial_contact": "Contract_2",
                    "delivery_contact": "Matt",
                    "created_at": datetime.datetime(
                        2023, 7, 31, 11, 35, 14, 976768
                    ),
                    "last_updated": datetime.datetime(
                        2023, 7, 31, 11, 35, 14, 976768
                    ),
                }
            ]
