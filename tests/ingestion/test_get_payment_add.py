from decimal import Decimal
from src.ingestion_lambda.get_payment_add import (
    get_payment_add,
    MissingRequiredEnvironmentVariables,
)
from unittest.mock import patch
import datetime
import pytest
import os
import pg8000.exceptions
from dotenv import load_dotenv

load_dotenv()


payment_get_last_time_path = (
    "src.ingestion_lambda.get_payment_add.get_last_time"
)


class Test_Ingestion_Payment:
    def test_get_payment_add_returns_list_with_correct_keys(self):
        """
        Test that the get_payment_add function returns a list of dictionaries
        with correct keys.

        This test mocks the get_last_time function to return a datetime. It
        then calls the get_payment_add function with test source environment
        variables and checks if the returned result is a list of dictionaries,
        where each dictionary contains the expected keys.
        """
        with patch(payment_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )

            result = get_payment_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )

            assert isinstance(result, list)
            expected_keys = {
                "payment_id",
                "created_at",
                "last_updated",
                "transaction_id",
                "counterparty_id",
                "payment_amount",
                "currency_id",
                "payment_type_id",
                "paid",
                "payment_date",
                "company_ac_number",
                "counterparty_ac_number",
            }
            assert all(set(item.keys()) == expected_keys for item in result)

    def test_get_payment_add_has_correct_value_types(self):
        """
        Test that the get_payment_add function returns dictionaries with
        correct value types.

        This test mocks the get_last_time function to return a datetime. It
        then calls the get_payment_add function with test source environment
        variables and checks if the returned dictionaries have the expected
        value types for each key.
        """
        with patch(payment_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_payment_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )
            for item in result:
                assert isinstance(item["payment_id"], int)
                assert isinstance(item["created_at"], datetime.date)
                assert isinstance(item["last_updated"], datetime.date)
                assert isinstance(item["transaction_id"], int)
                assert isinstance(item["counterparty_id"], int)
                assert isinstance(item["payment_amount"], Decimal)
                assert isinstance(item["currency_id"], int)
                assert isinstance(item["payment_type_id"], int)
                assert isinstance(item["paid"], bool)
                assert isinstance(item["payment_date"], str)
                assert isinstance(item["company_ac_number"], int)
                assert isinstance(item["counterparty_ac_number"], int)

    def test_get_payment_add_calls_get_last_time(self):
        """
        Test that the get_payment_add function calls the get_last_time
        function.

        This test mocks the get_last_time function to return a datetime. It
        then calls the get_payment_add function with test source environment
        variables and checks if the get_last_time function was called once.
        """
        with patch(payment_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            get_payment_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )
            assert mock_get_last_time.call_count == 1

    def test_database_error(self):
        """
        Test that the get_payment_add function raises an exception when a
        database error occurs.

        This test mocks the pg8000.native.Connection class to raise a
        pg8000.exceptions.DatabaseError. It then calls the get_payment_add
        function with test source environment variables and checks if the
        function raises an exception with the message "Database error".
        """
        with patch("pg8000.native.Connection") as mock_connection:
            mock_connection.side_effect = pg8000.exceptions.DatabaseError(
                "Database error"
            )
            with pytest.raises(Exception, match="Database error"):
                get_payment_add(
                    db_user=os.environ.get("TEST_SOURCE_USER"),
                    db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                    db_host=os.environ.get("TEST_SOURCE_HOST"),
                    db_port=os.environ.get("TEST_SOURCE_PORT"),
                    db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
                )

    def test_missing_environment_variables(self):
        """
        Test that the get_payment_add function raises 'Missing Required
        Environment Variables' exception.

        This test mocks the os.environ dictionary to be empty. It then calls
        the get_payment_add function with test environment variables and
        checks if the function raises the MissingRequiredEnvironmentVariables
        exception.
        """
        with patch("os.environ", {}):
            with pytest.raises(MissingRequiredEnvironmentVariables):
                get_payment_add(
                    db_user=os.environ.get("test_user"),
                    db_database=os.environ.get("test_database"),
                    db_host=os.environ.get("test_host"),
                    db_port=os.environ.get("test_port"),
                    db_password=os.environ.get("test_password"),
                )

    def test_correct_data_returned_by_query(self):
        """
        Test that the get_payment_add function returns the correct data based
        on the query.

        This test mocks the get_last_time function to return a datetime. It
        then calls the get_payment_add function with test source environment
        variables and checks if the returned result matches the expected data.
        """
        with patch(payment_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2023-07-29 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_payment_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"),
            )

            assert result == [
                {
                    "payment_id": 1,
                    "created_at": datetime.datetime(
                        2023, 8, 1, 12, 39, 34, 942457
                    ),
                    "last_updated": datetime.datetime(
                        2023, 8, 1, 12, 39, 34, 942457
                    ),
                    "transaction_id": 1,
                    "counterparty_id": 1,
                    "payment_amount": Decimal("12.59"),
                    "currency_id": 1,
                    "payment_type_id": 1,
                    "paid": True,
                    "payment_date": "Today",
                    "company_ac_number": 123,
                    "counterparty_ac_number": 123,
                }
            ]
