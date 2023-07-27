from src.ingestion_lambda.get_sales_order_add \
    import get_sales_order_add
from unittest.mock import patch
import datetime
from decimal import Decimal

ingestion_utils_path = 'src.ingestion_lambda.utils.'
sales_order_get_last_time_path = 'get_sales_order_add.get_last_time'
get_last_time_patch_path = ingestion_utils_path + \
    sales_order_get_last_time_path


def test_get_sales_order_add_returns_list():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        result = get_sales_order_add()
        assert isinstance(result, list)


def test_get_sales_order_add_returns_correct_keys():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        expected_keys = {
            "sales_order_id", "created_at", "last_updated", "design_id",
            "staff_id", "counter_party_id", "units_sold", "unit_price",
            "currency_id", "agreed_delivery_date", "agreed_payment_date",
            "agreed_delivery_location_id"}
        result = get_sales_order_add()
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_sales_order_add_has_correct_value_types():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
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
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        get_sales_order_add()
        assert mock_get_last_time.call_count == 1
