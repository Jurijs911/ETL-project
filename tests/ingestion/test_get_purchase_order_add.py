from decimal import Decimal
import os
from src.ingestion_lambda.get_purchase_order_add \
    import get_purchase_order_add, MissingRequiredEnvironmentVariables
import pytest
from unittest.mock import patch
import datetime
import pg8000.exceptions
from dotenv import load_dotenv

load_dotenv()


purchase_order_get_last_time_path = \
    'get_purchase_order_add.get_last_time'


class Test_Ingestion_Purchase:
    def test_get_purchase_order_add_returns_list_with_correct_keys():
        """
        Test whether the 'get_purchase_order_add' function returns a list of
        dictionaries with the correct keys for the 'purchase_order' table.

        The test patches the 'get_last_time' function to provide a date as the
        search interval. The function is then called with test environment
        variables to connect to the test database. The retrieved data is
        verified to contain dictionaries with the expected keys.
        """
        with patch(purchase_order_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )

            result = get_purchase_order_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

            assert isinstance(result, list)
            expected_keys = {
                             "purchase_order_id",
                             "created_at",
                             "last_updated",
                             "staff_id",
                             "counterparty_id",
                             "item_code",
                             "item_quantity",
                             "item_unit_price",
                             "currency_id",
                             "agreed_delivery_date",
                             "agreed_payment_date",
                             "agreed_delivery_location_id"
                             }
            assert all(set(item.keys()) == expected_keys for item in result)

    def test_get_address_add_has_correct_value_types():
        """
        Test whether the 'get_purchase_order_add' function returns data with
        correct value types for the 'purchase_order' table.

        The test patches the 'get_last_time' function to provide a date as the
        search interval. The function is called with test environment variables
        to connect to the test database. Test then checks the retrieved data
        contains dictionaries with the expected value types for each key.
        """
        with patch(purchase_order_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
            result = get_purchase_order_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))
            for item in result:
                assert isinstance(item['purchase_order_id'], int)
                assert isinstance(item['created_at'], datetime.date)
                assert isinstance(item['last_updated'], datetime.date)
                assert isinstance(item['staff_id'], int)
                assert isinstance(item['counterparty_id'], int)
                assert isinstance(item['item_code'], str)
                assert isinstance(item['item_quantity'], int)
                assert isinstance(item['item_unit_price'], Decimal)
                assert isinstance(item['currency_id'], int)
                assert isinstance(item['agreed_delivery_date'], str)
                assert isinstance(item['agreed_payment_date'], str)
                assert isinstance(item['agreed_delivery_location_id'], int)

    def test_get_purchase_order_add_calls_get_last_time():
        """
        Test whether the 'get_purchase_order_add' function calls the
        'get_last_time' function.

        The test patches the 'get_last_time' function to provide a fixed date
        as the search interval. The function is called with test environment
        variables to connect to the test database.
        """
        with patch(purchase_order_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
            get_purchase_order_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))
            assert mock_get_last_time.call_count == 1

    def test_database_error():
        """
        Test whether the 'get_purchase_order_add' function raises an exception
        for a database error.

        The test patches the database connection to raise a 'DatabaseError'
        exception. The function is then called with test environment variables
        to connect to the test database.
        """
        with patch('pg8000.native.Connection') as mock_connection:
            mock_connection.side_effect = pg8000.exceptions.DatabaseError(
                "Database error")
            with pytest.raises(Exception, match="Database error"):
                get_purchase_order_add(
                    db_user=os.environ.get("TEST_SOURCE_USER"),
                    db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                    db_host=os.environ.get("TEST_SOURCE_HOST"),
                    db_port=os.environ.get("TEST_SOURCE_PORT"),
                    db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

    def test_missing_environment_variables():
        """
        Test whether the 'get_purchase_order_add' function raises an exception
        for missing environment variables.

        The test patches the 'os.environ' dictionary to simulate missing
        environment variables. The function is called with test environment
        variables to connect to the test database.
        """
        with patch('os.environ', {}):
            with pytest.raises(MissingRequiredEnvironmentVariables):
                get_purchase_order_add(db_user=os.environ.get("test_user"),
                                       db_database=os.environ.get(
                                       "test_database"),
                                       db_host=os.environ.get('test_host'),
                                       db_port=os.environ.get("test_port"),
                                       db_password=os.environ.get(
                                       "test_password"))

    def test_correct_data_returned_by_query():
        """
        Test whether the 'get_purchase_order_add' function returns the correct
        data for the 'purchase_order' table.

        The test patches the 'get_last_time' function to provide a date as the
        search interval. The function is then called with test environment
        variables to connect to the test database.
        """
        with patch(purchase_order_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2023-07-29 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_purchase_order_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))
            assert result == [
                {'purchase_order_id': 1, 'created_at': datetime.datetime(
                    2023, 8, 1, 12, 36, 40, 948439),
                 'last_updated': datetime.datetime(
                    2023, 8, 1, 12, 36, 40, 948439),
                 'staff_id': 1, 'counterparty_id': 1, 'item_code': '1',
                 'item_quantity': 1, 'item_unit_price': Decimal('1.50'),
                 'currency_id': 1, 'agreed_delivery_date': 'Next Week',
                 'agreed_payment_date': 'Week After',
                 'agreed_delivery_location_id': 1}]
