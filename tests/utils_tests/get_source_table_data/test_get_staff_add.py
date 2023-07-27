from src.ingestion_lambda.get_staff_add import get_staff_add
from unittest.mock import patch
import datetime

ingestion_utils_path = "src.ingestion_lambda.utils."
staff_get_last_time_path = "get_staff_add.get_last_time"
get_last_time_patch_path = ingestion_utils_path + staff_get_last_time_path


def test_get_staff_add_returns_list():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        result = get_staff_add()
        assert isinstance(result, list)


def test_gget_staff_add_returns_correct_keys():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        expected_keys = {
            "staff_id",
            "first_name",
            "last_name",
            "department_id",
            "email_address",
            "created_at",
            "last_updated",
        }
        result = get_staff_add()
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_staff_add_has_correct_value_types():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        result = get_staff_add()
        for item in result:
            assert isinstance(item["staff_id"], int)
            assert isinstance(item["first_name"], str)
            assert isinstance(item["last_name"], str)
            assert isinstance(item["department_id"], int)
            assert isinstance(item["email_address"], str)
            assert isinstance(item["created_at"], datetime.date)
            assert isinstance(item["last_updated"], datetime.date)


def test_get_staff_add_calls_get_last_time():
    with patch(get_last_time_patch_path) as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            "2020-07-25 15:20:49.962000", "%Y-%m-%d %H:%M:%S.%f"
        )
        get_staff_add()
        assert mock_get_last_time.call_count == 1
