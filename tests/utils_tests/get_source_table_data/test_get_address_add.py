from src.ingestion_lambda.get_address_add import get_address_add
from unittest.mock import patch
import datetime

ingestion_utils_path = "src.ingestion_lambda.utils."
address_get_last_time_path = "get_address_add.get_last_time"
get_last_time_patch_path = ingestion_utils_path + address_get_last_time_path


def test_get_address_add_returns_list():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        result = get_address_add()
        assert isinstance(result, list)


def test_get_address_add_returns_correct_keys():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
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
        result = get_address_add()
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_address_add_has_correct_value_types():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        result = get_address_add()
        for item in result:
            assert isinstance(item["location_id"], int)
            assert isinstance(item["address_line_1"], str)
            assert (
                isinstance(item["address_line_2"], str)
                or item["address_line_2"] is None
            )
            assert (
                isinstance(item["district"], str) or item["district"] is None
            )
            assert isinstance(item["city"], str)
            assert isinstance(item["postal_code"], str)
            assert isinstance(item["country"], str)
            assert isinstance(item["phone"], str)
            assert isinstance(item["created_at"], datetime.date)
            assert isinstance(item["last_updated"], datetime.date)


def test_get_address_calls_get_last_time():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        get_address_add()
        assert mock_get_last_time.call_count == 1
