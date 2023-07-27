import pytest
from unittest.mock import patch, Mock
from src.loading.loading_utils import (
    create_connection, 
    insert_into_dim_design,
    insert_into_dim_currency,
    insert_into_dim_staff,
    insert_into_dim_location,
    insert_into_dim_date,
    insert_into_dim_counterparty,
    insert_into_dim_fact_sales_order
    )


def test_read_inserted_dim_design_data():

    mock_connection = Mock()
    with patch('src.loading.loading_utils.create_connection', return_value=mock_connection):

        design_data = [
            [5, "Design 5", "location5", "file5"],
            [6, "Design 6", "location6", "file6"],
        ]
        mock_connection.run.return_value = design_data

        result = insert_into_dim_design(mock_connection, design_data)
        assert result == design_data


def test_read_inserted_dim_currency_data():
    mock_connection = Mock()
    with patch('src.loading.loading_utils.create_connection', return_value=mock_connection):
        currency_data = [
            [1, "USD", "US Dollar"],
            [2, "GBP", "GB Pound"],
        ]
        mock_connection.run.return_value = currency_data

        result = insert_into_dim_currency(mock_connection, currency_data)
        assert result == currency_data

    
def test_read_inserted_dim_staff_data():
    mock_connection = Mock()
    with patch('src.loading.loading_utils.create_connection', return_value=mock_connection):
        staff_data = [
            [1, "Zenab", "Haider", "Sales", "Manchester", "zenab@email.com"],
            [2, "Lisa", "S", "Marketing", "London", "lisa@email.com"],
        ]
        mock_connection.run.return_value = staff_data

        result = insert_into_dim_staff(mock_connection, staff_data)
        assert result == staff_data


def test_insert_into_dim_location():
    mock_connection = Mock()
    with patch('src.loading.loading_utils.create_connection', return_value=mock_connection):
        location_data = [
            [1, "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"],
            [2, "123 apple street", "address_line_2", "apple", "applecity", "ABC-123", "England", "123-456-789"],
        ]
        mock_connection.run.return_value = location_data

        result = insert_into_dim_location(mock_connection, location_data)
        assert result == location_data


def test_insert_into_dim_date():
    mock_connection = Mock()
    with patch('src.loading.loading_utils.create_connection', return_value=mock_connection):
        date_data = [
            ["2023-07-27", 2023, 7, 27, 4, "Thursday", "July", 2]
        ]
        mock_connection.run.return_value = date_data

        result = insert_into_dim_date(mock_connection, date_data)
        assert result == date_data


def test_insert_into_dim_counterparty():
    mock_connection = Mock()
    with patch('src.loading.loading_utils.create_connection', return_value=mock_connection):
        counterparty_data = [
            [1, "counterparty_legal_name", "counterparty_legal_address_line_1", "counterparty_legal_address_line_2", "counterparty_legal_district", "counterparty_legal_city", "counterparty_legal_postal_code", "counterparty_legal_country", "counterparty_legal_phone_number"]
        ]
        mock_connection.run.return_value = counterparty_data

        result = insert_into_dim_counterparty(mock_connection, counterparty_data)
        assert result == counterparty_data


def test_insert_into_dim_fact_sales_order():
    mock_connection = Mock()
    with patch('src.loading.loading_utils.create_connection', return_value=mock_connection):
        sales_data = [
            [1, 1, "2023-07-27", "15:20:49:962000", "2023-07-27", "15:20:49:962000", 10, 12, 115, 20.20, 15, 16, "2023-07-30", "2023-08-05", 18]
        ]
        mock_connection.run.return_value = sales_data

        result = insert_into_dim_fact_sales_order(mock_connection, sales_data)
        assert result == sales_data