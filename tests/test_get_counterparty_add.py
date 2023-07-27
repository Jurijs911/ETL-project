from requests import patch
from src.ingestion_lambda.utils.get_counterparty_add \
    import get_counterparty_add
from unittest.mock import patch
import datetime


def test_get_counterparty_add_returns_list():
    with patch('src.ingestion_lambda.utils.get_counterparty_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000',
            '%Y-%m-%d %H:%M:%S.%f')
        
        result = get_counterparty_add()
        
        assert isinstance(result, list)


def test_get_counterparty_add_returns_correct_keys():
    with patch('src.ingestion_lambda.utils.get_counterparty_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        # test that we have the correct keys
        expected_keys = {
            "counterparty_id", "counterparty_legal_name",
            "counterparty_legal_name", "legal_address_id",
            "commercial_contact", "delivery_contact", "created_at",
            "last_updated"}
        result = get_counterparty_add()
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_counterparty_add_has_correct_value_types():
    with patch('src.ingestion_lambda.utils.get_counterparty_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
    # test that each key has the correct type value
        result = get_counterparty_add()
        for item in result:
            assert isinstance(item['counterparty_id'], int)
            assert isinstance(item['counterparty_legal_name'], str)
            assert isinstance(item['legal_address_id'], int)
            assert isinstance(item['commercial_contact'], str)
            assert isinstance(item['delivery_contact'], str)
            assert isinstance(item['created_at'], datetime.date)
            assert isinstance(item['last_updated'], datetime.date)


def test_get_counterparty_add_calls_get_last_time():
    # test that the SQL query calls get_last_time()
    with patch('src.ingestion_lambda.utils.get_counterparty_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        get_counterparty_add()
        assert mock_get_last_time.call_count == 1
