from src.remodelling.manipulation_utils import ( 
    format_fact_sales_order,
    format_dim_design,
    format_dim_staff,
    format_dim_location,
    format_dim_date,
    format_dim_currency,
    format_dim_counterparty,
)
from datetime import datetime

def test_format_fact_sales_order():
    sample_sales_data = [
        {
           
            "sales_order_id": 2,
            "created_at": datetime(2023, 7, 25, 15, 20, 49, 962000),
            "last_updated": datetime(2023, 7, 25, 15, 20, 49, 962000),
            "sales_staff_id": 100,
            "counterparty_id": 200,
            "units_sold": 2000,
            "unit_price": 20.65,
            "currency_id": 5,
            "design_id": 1,
            "agreed_payment_date": '2023, 7, 30',
            "agreed_delivery_date": '2023, 8, 12',
            "agreed_delivery_location_id": 2,
        },
    ]

    formatted_data = format_fact_sales_order(sample_sales_data)
    expected_sales_data = [
        {
           
            "sales_order_id": 2,
            "created_date": '2023-07-25',
            "created_time": '15:20:49:962000',
            "last_updated_date": '2023-07-25',
            "last_updated_time": '15:20:49:962000',
            "sales_staff_id": 100,
            "counterparty_id": 200,
            "units_sold": 2000,
            "unit_price": 20.65,
            "currency_id": 5,
            "design_id": 1,
            "agreed_payment_date": '2023, 7, 30',
            "agreed_delivery_date": '2023, 8, 12',
            "agreed_delivery_location_id": 2,
        },
    ]
    assert formatted_data == expected_sales_data

    

def test_format_dim_design():
    sample_design_data = [
        {
            "design_id": 1,
            "design_name": "design 1",
            "file_location": "./design.jpg",
            "file_name": "design.jpg",
        },
    ]

    formatted_design = format_dim_design(sample_design_data)

    expected_design_data = [
        {
            "design_id": 1,
            "design_name": "design 1",
            "file_location": "./design.jpg",
            "file_name": "design.jpg",
        },
    ]

    assert formatted_design == expected_design_data



def test_format_dim_staff():
    sample_staff_data = [
        {
            "staff_id": 1,
            "first_name": "zenab",
            "last_name": "haider",
            "department_name": "coding",
            "location": "manchester",
            "email_address": "zenab@gmail.com",
        },
    ]

    formatted_staff = format_dim_staff(sample_staff_data)

    expected_staff_data = [
        {
           "staff_id": 1,
            "first_name": "zenab",
            "last_name": "haider",
            "department_name": "coding",
            "location": "manchester",
            "email_address": "zenab@gmail.com", 
        },
    ]

    assert formatted_staff == expected_staff_data



def test_format_dim_location():
    sample_dim_location = [
        {
            "location_id": 1,
            "address_line_1": "123 apple street",
            "address_line_2": "apple street",
            "district": "bolton",
            "city": "greater manchester",
            "postal_code": "ABC 123",
            "country": "England",
            "phone": "123 456 789",
        },
    ]

    formatted_location = format_dim_location(sample_dim_location)

    expected_location_data = [
        {
            "location_id": 1,
            "address_line_1": "123 apple street",
            "address_line_2": "apple street",
            "district": "bolton",
            "city": "greater manchester",
            "postal_code": "ABC 123",
            "country": "England",
            "phone": "123 456 789",
        },
    ]

    assert formatted_location == expected_location_data


def test_format_dim_date():
    sample_date_data = [
        {
            "date_id": '2023-01-01',
        },
    ]

    formatted_date = format_dim_date(sample_date_data)

    expected_date_data = [
        {
            "date_id": '2023-01-01',
            "year": 2023,
            "month": 1,
            "day": 1,
            "day_of_week": 6,
            "day_name": "Sunday",
            "month_name": "January",
            "quarter": 1,
        },
    ]

    assert formatted_date == expected_date_data


def test_format_dim_currency():
    sample_currency_data = [
        {
            "currency_id": 1,
            "currency_code": "gbp",
            "currency_name": "great british pounds",
        },
    ]

    formatted_currency = format_dim_currency(sample_currency_data)

    expected_currency_data = [
        {
            "currency_id": 1,
            "currency_code": "gbp",
            "currency_name": "great british pounds",
        },
    ]

    assert formatted_currency == expected_currency_data


def test_format_dim_counterparty():
    sample_counterparty_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "hello",
            "counterparty_legal_address_line_1": "legal address",
            "counterparty_legal_address_line2": "counterparty_legal_address_line2",
            "counterparty_legal_district": "counterparty_legal_district",
            "counterparty_legal_city": "counterparty_legal_city",
            "counterparty_legal_postal_code": "counterparty_legal_postal_code",
            "counterparty_legal_country": "counterparty_legal_country",
            "counterparty_legal_phone_number": "counterparty_legal_phone_number",
        },
    ]

    formatted_counterparty_data = format_dim_counterparty(sample_counterparty_data)

    expected_counterparty_data = [
        {
            "counterparty_id": 1,
            "counterparty_legal_name": "hello",
            "counterparty_legal_address_line_1": "legal address",
            "counterparty_legal_address_line2": "counterparty_legal_address_line2",
            "counterparty_legal_district": "counterparty_legal_district",
            "counterparty_legal_city": "counterparty_legal_city",
            "counterparty_legal_postal_code": "counterparty_legal_postal_code",
            "counterparty_legal_country": "counterparty_legal_country",
            "counterparty_legal_phone_number": "counterparty_legal_phone_number",
        },
    ]

    assert formatted_counterparty_data == expected_counterparty_data