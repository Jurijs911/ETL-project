from get_sales_order_add \
    import get_sales_order_add, MissingRequiredEnvironmentVariables
from unittest.mock import patch
import os
import datetime
import pytest
from decimal import Decimal
import pg8000.exceptions
from dotenv import load_dotenv

load_dotenv()


sales_order_get_last_time_path = 'get_sales_order_add.get_last_time'


class Test_Ingestion_Sales_Order:
    def test_get_sales_order_add_returns_list_with_correct_keys():
        with patch(sales_order_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )

            result = get_sales_order_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

            assert isinstance(result, list)
            expected_keys = {
                "sales_order_id", "created_at", "last_updated", "design_id",
                "staff_id", "counter_party_id", "units_sold", "unit_price",
                "currency_id", "agreed_delivery_date", "agreed_payment_date",
                "agreed_delivery_location_id"}
            assert all(set(item.keys()) == expected_keys for item in result)

    def test_get_sales_order_add_has_correct_value_types():
        with patch(sales_order_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
            result = get_sales_order_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))
            for item in result:
                assert isinstance(item['sales_order_id'], int)
                assert isinstance(item['created_at'], datetime.date)
                assert isinstance(item['last_updated'], datetime.date)
                assert isinstance(item['design_id'], int)
                assert isinstance(item['staff_id'], int)
                assert isinstance(item['counter_party_id'], int)
                assert isinstance(item['units_sold'], int)
                assert isinstance(item['unit_price'], Decimal)
                assert isinstance(item['currency_id'], int)
                assert isinstance(item['agreed_delivery_date'], str)
                assert isinstance(item['agreed_payment_date'], str)
                assert isinstance(item['agreed_delivery_location_id'], int)

    def test_get_sales_order_calls_get_last_time():
        with patch(sales_order_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
            get_sales_order_add(
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
                get_sales_order_add(
                    db_user=os.environ.get("TEST_SOURCE_USER"),
                    db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                    db_host=os.environ.get("TEST_SOURCE_HOST"),
                    db_port=os.environ.get("TEST_SOURCE_PORT"),
                    db_password=os.environ.get("TEST_SOURCE_PASSWORD"))

    def test_missing_environment_variables():
        with patch('os.environ', {}):
            with pytest.raises(MissingRequiredEnvironmentVariables):
                get_sales_order_add(db_user=os.environ.get(
                                    "test_user"),
                                    db_database=os.environ.get(
                                    "test_database"),
                                    db_host=os.environ.get(
                                    'test_host'),
                                    db_port=os.environ.get(
                                    "test_port"),
                                    db_password=os.environ.get(
                                    "test_password"))

    def test_correct_data_returned_by_query():
        with patch(sales_order_get_last_time_path) as mock_get_last_time:
            mock_get_last_time.return_value = datetime.datetime.strptime(
                "2023-07-27 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
            )
            result = get_sales_order_add(
                db_user=os.environ.get("TEST_SOURCE_USER"),
                db_database=os.environ.get("TEST_SOURCE_DATABASE"),
                db_host=os.environ.get("TEST_SOURCE_HOST"),
                db_port=os.environ.get("TEST_SOURCE_PORT"),
                db_password=os.environ.get("TEST_SOURCE_PASSWORD"))
            assert result == [
                {'sales_order_id': 1,
                 'created_at': datetime.datetime(
                    2023, 7, 28, 15, 9, 58, 335449),
                 'last_updated': datetime.datetime(
                    2023, 7, 28, 15, 9, 58, 335449),
                 'design_id': 1,
                 'staff_id': 1,
                 'counter_party_id': 1,
                 'units_sold': 100,
                 'unit_price': Decimal('2.5'),
                 'currency_id': 1,
                 'agreed_delivery_date': 'Tuesday next week',
                 'agreed_payment_date': 'Following Wednesday',
                 'agreed_delivery_location_id': 1}]
