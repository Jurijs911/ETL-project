from pg8000 import VARCHAR
from requests import patch
from src.ingestion_lambda.utils.get_sales_order_add import get_sales_order_add
from unittest.mock import patch, Mock
from unittest import mock
import pg8000.native
import datetime
import pytest
from decimal import Decimal


def test_get_sales_order_add_returns_list():
    with patch('src.ingestion_lambda.utils.get_sales_order_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        # test that we are returning a list,
        result = get_sales_order_add()
        # print(result)
        assert isinstance(result, list)


def test_get_sales_order_add_returns_correct_keys():
    with patch('src.ingestion_lambda.utils.get_sales_order_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        # test that we have the correct keys
        expected_keys = {
            "sales_order_id", "created_at", "last_updated", "design_id",
            "staff_id", "counter_party_id", "units_sold", "unit_price",
            "currency_id", "agreed_delivery_date", "agreed_payment_date",
            "agreed_delivery_location_id"}
        result = get_sales_order_add()
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_sales_order_add_has_correct_value_types():
    with patch('src.ingestion_lambda.utils.get_sales_order_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
    # test that each key has the correct type value
        result = get_sales_order_add()
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
    #test that the SQL query calls get_last_time()
    with patch('src.ingestion_lambda.utils.get_sales_order_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
        '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        get_sales_order_add()
        assert mock_get_last_time.call_count == 1