from src.ingestion_lambda.get_transaction_add import get_transaction_add
from unittest.mock import patch
import datetime

ingestion_utils_path = "src.ingestion_lambda."
transaction_get_last_time_path = "get_transaction_add.get_last_time"
get_last_time_patch_path = ingestion_utils_path + transaction_get_last_time_path


def test_get_transaction_add_returns_list():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        result = get_transaction_add()
        assert isinstance(result, list)


def test_get_transaction_add_returns_correct_keys():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        expected_keys = {
            "transaction_id",
            "transaction_type",
            "sales_order_id",
            "purchase_order_id",
            "created_at",
            "last_updated",
        }
        result = get_transaction_add()
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_transaction_add_has_correct_value_types():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        result = get_transaction_add()
        for item in result:
            assert isinstance(item["transaction_id"], int)
            assert isinstance(item["transaction_type"], str)
            assert (isinstance(item["sales_order_id"], int)
            assert (isinstance(item["purchase_order_id"], int) 
            assert isinstance(item["created_at"], datetime.date)
            assert isinstance(item["last_updated"], datetime.date)


def test_get_transaction_calls_get_last_time():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        get_transaction_add()
        assert mock_get_last_time.call_count == 1
