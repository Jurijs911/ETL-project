
from decimal import Decimal
from src.ingestion_lambda.get_payment_add import get_payment_add
from unittest.mock import patch, Mock
from unittest import mock
import pg8000.native
import datetime
import pytest


def test_get_payment_add_returns_list():
    with patch('src.ingestion_lambda.utils.get_payment_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        # test that we are returning a list,
        result = get_payment_add()

        assert isinstance(result, list)


def test_get_payment_add_returns_correct_keys():
    with patch('src.ingestion_lambda.utils.get_payment_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        # test that we have the correct keys
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
            "counterparty_ac_number"
        }
        result = get_payment_add()
        assert all(set(item.keys()) == expected_keys for item in result)


def test_get_payment_add_has_correct_value_types():
    with patch('src.ingestion_lambda.utils.get_payment_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
    # test that each key has the correct type value
        result = get_payment_add()
        for item in result:
            assert isinstance(item['payment_id'], int)
            assert isinstance(item['created_at'], datetime.date)
            assert isinstance(item['last_updated'], datetime.date)
            assert isinstance(item['transaction_id'], int)
            assert isinstance(item['counterparty_id'], int)
            assert isinstance(item['payment_amount'], Decimal)
            assert isinstance(item['currency_id'], int)
            assert isinstance(item['payment_type_id'], int)
            assert isinstance(item['paid'], bool)
            assert isinstance(item['payment_date'], str)
            assert isinstance(item['company_ac_number'], int)
            assert isinstance(item['counterparty_ac_number'], int)


def test_get_payment_add_calls_get_last_time():
    # test that the SQL query calls get_last_time()
    with patch('src.ingestion_lambda.utils.get_payment_add.get_last_time') as mock_get_last_time:
        mock_get_last_time.return_value = datetime.datetime.strptime(
            '2020-07-25 15:20:49.962000', '%Y-%m-%d %H:%M:%S.%f')
        get_payment_add()
        assert mock_get_last_time.call_count == 1
